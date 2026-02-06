/**
 * DEX Trading Module
 *
 * Extracted DEX trading functions from MahoragaHarness.
 * Handles paper trading on Solana DEXs with momentum-based strategies.
 */

import type { HarnessContext } from "./context";
import type { DexPosition, DexPortfolioSnapshot, DexTradeRecord } from "./types";
import { sendTradeAlert } from "./notifications";
import {
  calculateDexSlippage,
  updateStreakAndDrawdownState,
  getSolPriceUsd,
} from "./utils";
import { createBirdeyeProvider } from "../../providers/birdeye";

/**
 * Run DEX momentum trading logic with PAPER TRADING.
 * Creates virtual positions to test strategy without real funds.
 * Tracks P&L and trade history for validation.
 */
export async function runDexTrading(ctx: HarnessContext): Promise<void> {
  if (!ctx.state.config.dex_enabled) return;
  if (ctx.state.dexSignals.length === 0) return;

  // Ensure paper trading state is initialized (migration fix for null/NaN values)
  if (ctx.state.dexPaperBalance == null || Number.isNaN(ctx.state.dexPaperBalance)) {
    ctx.state.dexPaperBalance = 1.0;
  }

  // Fetch real SOL price once for this trading cycle (cached for 5 minutes)
  const solPriceUsd = await getSolPriceUsd();
  const gasFee = ctx.state.config.dex_gas_fee_sol ?? 0.005;
  if (ctx.state.dexTradeHistory == null) ctx.state.dexTradeHistory = [];
  if (ctx.state.dexRealizedPnL == null || Number.isNaN(ctx.state.dexRealizedPnL)) {
    ctx.state.dexRealizedPnL = 0;
  }
  // Initialize streak and drawdown tracking fields (#15, #16, #17)
  if (ctx.state.dexMaxConsecutiveLosses == null) ctx.state.dexMaxConsecutiveLosses = 0;
  if (ctx.state.dexCurrentLossStreak == null) ctx.state.dexCurrentLossStreak = 0;
  if (ctx.state.dexMaxDrawdownPct == null) ctx.state.dexMaxDrawdownPct = 0;
  if (ctx.state.dexMaxDrawdownDuration == null) ctx.state.dexMaxDrawdownDuration = 0;
  if (ctx.state.dexDrawdownStartTime === undefined) ctx.state.dexDrawdownStartTime = null;
  if (ctx.state.dexPeakBalance == null || Number.isNaN(ctx.state.dexPeakBalance)) {
    ctx.state.dexPeakBalance = ctx.state.dexPaperBalance;
  }

  const heldTokens = new Set(Object.keys(ctx.state.dexPositions));

  // Check exits for existing positions
  for (const [tokenAddress, position] of Object.entries(ctx.state.dexPositions)) {
    const signal = ctx.state.dexSignals.find(s => s.tokenAddress === tokenAddress);

    // Calculate P&L based on current price vs entry
    // Use signal price if available, else last known price, else entry price (worst case)
    let currentPrice: number;
    if (signal?.priceUsd) {
      currentPrice = signal.priceUsd;
      position.lastKnownPrice = currentPrice; // Cache for when signal goes missing
    } else if (position.lastKnownPrice) {
      currentPrice = position.lastKnownPrice; // Use cached price when signal missing
    } else {
      currentPrice = position.entryPrice; // Fallback only if we never had a price
    }
    const plPct = ((currentPrice - position.entryPrice) / position.entryPrice) * 100;

    // Update peak price
    if (currentPrice > position.peakPrice) {
      position.peakPrice = currentPrice;
    }

    let shouldExit = false;
    let exitReason: "take_profit" | "stop_loss" | "lost_momentum" | "trailing_stop" | "breakeven_stop" | "scaling_trailing" = "take_profit";

    // Task #13: Check liquidity safety before any exit
    // If liquidity is too low relative to position size, we might get stuck
    const positionValueUsd = position.tokenAmount * currentPrice;
    const currentLiquidity = signal?.liquidity || position.entryLiquidity * 0.5; // Assume 50% decay if signal lost
    const minLiquidityRatio = 5; // Position should be at most 20% of liquidity for safe exit
    const canSafelyExit = currentLiquidity >= positionValueUsd * minLiquidityRatio;

    // Lost momentum - token fell off radar
    // KEY FIX: If position is GREEN, don't exit! Let trailing stop handle it.
    // Only exit on lost momentum if we're RED and it's been missing for a while.
    // NOTE: Signal should never be missing for open positions - gatherers.ts preserves them
    if (!signal) {
      // Increment missed scan counter
      position.missedScans = (position.missedScans || 0) + 1;

      // If position is profitable, DON'T exit just because it's not trending
      // The trailing stop will protect gains - no need to panic sell a winner
      if (plPct > 0) {
        ctx.log("DexMomentum", "signal_miss_but_green", {
          symbol: position.symbol,
          missedScans: position.missedScans,
          plPct: plPct.toFixed(1) + "%",
          reason: "Position is profitable - letting trailing stop manage exit, not panic selling",
        });
        // Don't exit - trailing stop will handle it if price drops
      }
      // If position is RED and missing for extended period (10+ scans = 5 min), consider exit
      else if (position.missedScans >= 10) { // Keep 10 scans (~5 min) to allow consolidation
        if (canSafelyExit) {
          shouldExit = true;
          exitReason = "lost_momentum";
          ctx.log("DexMomentum", "lost_momentum_exit", {
            symbol: position.symbol,
            missedScans: position.missedScans,
            plPct: plPct.toFixed(1) + "%",
            reason: "Position is RED and token missing from signals for 5+ minutes",
          });
        } else {
          ctx.log("DexMomentum", "exit_blocked_low_liquidity", {
            symbol: position.symbol,
            reason: "Token lost momentum but liquidity too low for safe exit",
            positionValueUsd: positionValueUsd.toFixed(2),
            estimatedLiquidity: currentLiquidity.toFixed(2),
          });
        }
      } else {
        ctx.log("DexMomentum", "signal_miss_grace", {
          symbol: position.symbol,
          missedScans: position.missedScans,
          plPct: plPct.toFixed(1) + "%",
          gracePeriod: plPct > 0 ? "GREEN position - trailing stop will manage" : "Waiting 10 scans (5 min) before exit",
        });
      }
    } else {
      // Reset missed scan counter when signal is found
      position.missedScans = 0;

      // Task #12: Momentum score decay - exit if score dropped significantly
      // KEY FIX: Only exit on momentum decay if position is RED
      // If we're green, the trailing stop will handle it
      if (signal.momentumScore < position.entryMomentumScore * 0.4 && plPct < 0) {
        // Momentum dropped to less than 40% of entry score AND we're losing money
        if (canSafelyExit) {
          shouldExit = true;
          exitReason = "lost_momentum";
          ctx.log("DexMomentum", "momentum_decay_exit", {
            symbol: position.symbol,
            entryMomentumScore: position.entryMomentumScore.toFixed(1),
            currentMomentumScore: signal.momentumScore.toFixed(1),
            decayPct: ((1 - signal.momentumScore / position.entryMomentumScore) * 100).toFixed(1),
            plPct: plPct.toFixed(1) + "%",
            reason: "Momentum decayed AND position is RED",
          });
        }
      } else if (signal.momentumScore < position.entryMomentumScore * 0.4) {
        // Momentum decayed but we're green - log but don't exit
        ctx.log("DexMomentum", "momentum_decay_but_green", {
          symbol: position.symbol,
          entryMomentumScore: position.entryMomentumScore.toFixed(1),
          currentMomentumScore: signal.momentumScore.toFixed(1),
          plPct: plPct.toFixed(1) + "%",
          reason: "Momentum decayed but position is GREEN - letting trailing stop manage",
        });
      }
      // Scaling Trailing Stop - earlier activation with proportional protection
      // Activates at +10%, allows drawdown from breakeven up to 45% max
      else if (ctx.state.config.dex_scaling_trailing_enabled) {
        const scalingActivation = ctx.state.config.dex_scaling_trailing_activation_pct ?? 10;
        const maxDrawdownPct = ctx.state.config.dex_scaling_max_drawdown_pct ?? 45;
        const peakGainPct = ((position.peakPrice - position.entryPrice) / position.entryPrice) * 100;
        const peakWasMeaningful = position.peakPrice > position.entryPrice * 1.05;

        if (peakGainPct >= scalingActivation && peakWasMeaningful) {
          // Drawdown allowed: scales from peakGain% (at activation) up to max
          // At +15%: can drop 15% (to breakeven)
          // At +100%: can drop 45% (to +55%)
          // At +200%: can drop 45% (to +155%)
          const drawdownAllowed = Math.min(peakGainPct, maxDrawdownPct);
          const profitFloorPct = peakGainPct - drawdownAllowed;
          const profitFloorPrice = position.entryPrice * (1 + profitFloorPct / 100);

          if (currentPrice <= profitFloorPrice) {
            // Position reached activation (+10%) and has now fallen to/below the floor
            // EXIT IMMEDIATELY - the whole point is to protect gains at breakeven
            // Don't wait for recovery - memecoins rarely recover once they dump
            shouldExit = true;
            exitReason = "scaling_trailing";

            ctx.log("DexMomentum", "scaling_stop_triggered", {
              symbol: position.symbol,
              peakGainPct: peakGainPct.toFixed(1),
              drawdownAllowed: drawdownAllowed.toFixed(1),
              profitFloorPct: profitFloorPct.toFixed(1),
              currentPnl: plPct.toFixed(1),
              note: plPct < profitFloorPct ? "Crashed past floor - exiting now to limit damage" : "Hit floor",
            });
          }
        }

        // FIX 1: Stop loss fallback when scaling trailing not activated
        // CRITICAL: Without this, positions can fall -90%+ because stop loss code is
        // in the next else-if block which is never reached when scaling_trailing is enabled
        if (!shouldExit) {
          const tierStopLossPct = (() => {
            switch (position.tier) {
              case 'lottery':
                return ctx.state.config.dex_lottery_stop_loss_pct ?? 35;
              case 'microspray':
                return ctx.state.config.dex_microspray_stop_loss_pct ?? 35;
              case 'breakout':
                return ctx.state.config.dex_breakout_stop_loss_pct ?? 35;
              case 'early':
                return ctx.state.config.dex_early_stop_loss_pct ?? 35;
              default: // established
                return ctx.state.config.dex_stop_loss_pct ?? 35;
            }
          })();

          if (plPct <= -tierStopLossPct) {
            // Stop loss always triggers (even with low liquidity - better to take high slippage than bigger loss)
            if (!canSafelyExit) {
              ctx.log("DexMomentum", "stop_loss_low_liquidity_warning", {
                symbol: position.symbol,
                plPct: plPct.toFixed(2),
                positionValueUsd: positionValueUsd.toFixed(2),
                liquidity: currentLiquidity.toFixed(2),
                warning: "Exiting at stop loss with potentially high slippage",
              });
            }
            shouldExit = true;
            exitReason = "stop_loss";
            ctx.log("DexMomentum", "stop_loss_triggered", {
              symbol: position.symbol,
              plPct: plPct.toFixed(2) + "%",
              stopLossThreshold: "-" + tierStopLossPct + "%",
              tier: position.tier,
              context: "scaling_trailing_fallback",
            });
          }
        }
      }
      // Trailing stop loss (#9) - activates after position is up by activation_pct
      // FIX: Removed immediate take_profit - let winners run via trailing stop instead
      // When position hits take_profit_pct, trailing stop activates and tracks the peak
      else if (ctx.state.config.dex_trailing_stop_enabled) {
        const peakGainPct = ((position.peakPrice - position.entryPrice) / position.entryPrice) * 100;

        // Micro-spray, breakout, and lottery have aggressive trailing stops
        const isHighRiskTier = position.tier === 'microspray' || position.tier === 'breakout' || position.tier === 'lottery';
        const activationPct = isHighRiskTier
          ? (ctx.state.config.dex_lottery_trailing_activation ?? 100) // All high-risk tiers use same activation
          : (ctx.state.config.dex_trailing_stop_activation_pct ?? 50);
        const distancePct = isHighRiskTier
          ? 20 // High-risk tiers: tighter trailing stop (20% from peak)
          : (ctx.state.config.dex_trailing_stop_distance_pct ?? 25);

        // Check if trailing stop is activated (position reached activation threshold at some point)
        // FIX: Also verify peak was meaningfully above entry (not just tracking artifact)
        const peakWasMeaningful = position.peakPrice > position.entryPrice * 1.05; // Peak was at least 5% above entry

        if (peakGainPct >= activationPct && peakWasMeaningful) {
          // Trailing stop price is distance_pct below peak
          const trailingStopPrice = position.peakPrice * (1 - distancePct / 100);

          // Log runner mode when position is above activation threshold
          if (plPct >= activationPct) {
            ctx.log("DexMomentum", "runner_mode_active", {
              symbol: position.symbol,
              currentPnl: plPct.toFixed(1) + "%",
              peakPnl: peakGainPct.toFixed(1) + "%",
              trailingStopPrice: "$" + trailingStopPrice.toFixed(8),
              distancePct: distancePct + "%",
            });
          }

          if (currentPrice <= trailingStopPrice) {
            // For trailing stop, prefer safe exit but log warning if liquidity is low
            if (!canSafelyExit) {
              ctx.log("DexMomentum", "trailing_stop_low_liquidity_warning", {
                symbol: position.symbol,
                positionValueUsd: positionValueUsd.toFixed(2),
                liquidity: currentLiquidity.toFixed(2),
                warning: "Exiting with potentially high slippage due to low liquidity",
              });
            }
            shouldExit = true;
            exitReason = "trailing_stop";
          }

          // Breakeven stop: Once trailing stop is active, never go negative
          // If position reached activation threshold but crashed back near entry, exit at breakeven
          const breakevenBuffer = ctx.state.config.dex_breakeven_buffer_pct ?? 2; // 2% buffer for fees
          const breakevenPrice = position.entryPrice * (1 + breakevenBuffer / 100);
          if (!shouldExit && currentPrice <= breakevenPrice) {
            ctx.log("DexMomentum", "breakeven_stop_triggered", {
              symbol: position.symbol,
              entryPrice: position.entryPrice.toFixed(8),
              breakevenPrice: breakevenPrice.toFixed(8),
              currentPrice: currentPrice.toFixed(8),
              peakPnl: peakGainPct.toFixed(1) + "%",
              reason: "Position reached activation but crashed back - exiting at breakeven",
            });
            shouldExit = true;
            exitReason = "breakeven_stop";
          }
        }
        // FIX: If peak wasn't meaningful but peakGainPct looks high, it's a bug - use fixed stop loss
        else if (peakGainPct >= activationPct && !peakWasMeaningful) {
          ctx.log("DexMomentum", "trailing_stop_skipped_invalid_peak", {
            symbol: position.symbol,
            peakPrice: position.peakPrice.toFixed(8),
            entryPrice: position.entryPrice.toFixed(8),
            reason: "Peak price not meaningfully above entry - using fixed stop loss instead",
          });
          // Fall through to fixed stop loss check below
        }

        // Tier-specific stop loss (configurable per tier from UI)
        const tierStopLossPct = (() => {
          switch (position.tier) {
            case 'lottery':
              return ctx.state.config.dex_lottery_stop_loss_pct ?? 35;
            case 'microspray':
              return ctx.state.config.dex_microspray_stop_loss_pct ?? 35;
            case 'breakout':
              return ctx.state.config.dex_breakout_stop_loss_pct ?? 35;
            case 'early':
              return ctx.state.config.dex_early_stop_loss_pct ?? 35;
            default: // established
              return ctx.state.config.dex_stop_loss_pct;
          }
        })();

        // Fixed stop loss if trailing stop not yet activated
        if (!shouldExit && plPct <= -tierStopLossPct) {
          // Stop loss always triggers (even with low liquidity - better to take high slippage than bigger loss)
          if (!canSafelyExit) {
            ctx.log("DexMomentum", "stop_loss_low_liquidity_warning", {
              symbol: position.symbol,
              plPct: plPct.toFixed(2),
              positionValueUsd: positionValueUsd.toFixed(2),
              liquidity: currentLiquidity.toFixed(2),
              warning: "Exiting at stop loss with potentially high slippage",
            });
          }
          shouldExit = true;
          exitReason = "stop_loss";
        }
      }
      // Fixed stop loss (when trailing stop is disabled)
      else {
        // Tier-specific stop loss (configurable per tier from UI)
        const tierStopLossPct = (() => {
          switch (position.tier) {
            case 'lottery':
              return ctx.state.config.dex_lottery_stop_loss_pct ?? 35;
            case 'microspray':
              return ctx.state.config.dex_microspray_stop_loss_pct ?? 35;
            case 'breakout':
              return ctx.state.config.dex_breakout_stop_loss_pct ?? 35;
            case 'early':
              return ctx.state.config.dex_early_stop_loss_pct ?? 35;
            default: // established
              return ctx.state.config.dex_stop_loss_pct;
          }
        })();
        if (plPct <= -tierStopLossPct) {
          // Stop loss always triggers (even with low liquidity)
          if (!canSafelyExit) {
            ctx.log("DexMomentum", "stop_loss_low_liquidity_warning", {
              symbol: position.symbol,
              plPct: plPct.toFixed(2),
              positionValueUsd: positionValueUsd.toFixed(2),
              liquidity: currentLiquidity.toFixed(2),
              warning: "Exiting at stop loss with potentially high slippage",
            });
          }
          shouldExit = true;
          exitReason = "stop_loss";
        }
      }
    } // End of else block (signal found)

    if (shouldExit) {
      // Record stop loss cooldown (#8) for stop_loss, trailing_stop, and scaling_trailing exits
      // Store exit price for price-based re-entry logic (use currentPrice before slippage)
      if (exitReason === "stop_loss" || exitReason === "trailing_stop" || exitReason === "scaling_trailing") {
        if (!ctx.state.dexStopLossCooldowns) ctx.state.dexStopLossCooldowns = {};
        const cooldownHours = ctx.state.config.dex_stop_loss_cooldown_hours ?? 2;
        ctx.state.dexStopLossCooldowns[tokenAddress] = {
          exitPrice: currentPrice,
          exitTime: Date.now(),
          fallbackExpiry: Date.now() + (cooldownHours * 60 * 60 * 1000),
        };
      }

      // Apply slippage to exit price (selling pushes price down = worse exit)
      const slippageModel = ctx.state.config.dex_slippage_model || "realistic";
      const posValueUsd = position.tokenAmount * currentPrice;
      const liquidity = signal?.liquidity || 10000; // Fallback liquidity for lost momentum tokens
      const sellSlippage = calculateDexSlippage(
        slippageModel,
        posValueUsd,
        liquidity
      );
      const exitPriceWithSlippage = currentPrice * (1 - sellSlippage);

      // Calculate P&L with slippage applied
      const actualPlPct =
        ((exitPriceWithSlippage - position.entryPrice) / position.entryPrice) * 100;
      const pnlSol = position.entrySol * (actualPlPct / 100);

      // Record the trade
      const tradeRecord: DexTradeRecord = {
        symbol: position.symbol,
        tokenAddress,
        entryPrice: position.entryPrice,
        exitPrice: exitPriceWithSlippage,
        entrySol: position.entrySol,
        entryTime: position.entryTime,
        exitTime: Date.now(),
        pnlPct: actualPlPct,
        pnlSol,
        exitReason,
      };

      ctx.state.dexTradeHistory.push(tradeRecord);
      ctx.state.dexRealizedPnL += pnlSol;
      ctx.state.dexPaperBalance += position.entrySol + pnlSol;

      // Deduct gas fee for the sell transaction
      ctx.state.dexPaperBalance -= gasFee;
      ctx.log("DexMomentum", "gas_fee_deducted", {
        action: "sell",
        symbol: position.symbol,
        gasFee: gasFee.toFixed(4) + " SOL",
        gasFeeUsd: "$" + (gasFee * solPriceUsd).toFixed(2),
      });

      // Update streak and drawdown tracking (#17)
      const isWin = pnlSol > 0;
      updateStreakAndDrawdownState(isWin, ctx.state.dexPaperBalance, ctx.state);

      // Record stop loss for circuit breaker (#10)
      if (exitReason === "stop_loss") {
        if (!ctx.state.dexRecentStopLosses) ctx.state.dexRecentStopLosses = [];
        ctx.state.dexRecentStopLosses.push({
          timestamp: Date.now(),
          symbol: position.symbol,
        });

        // Check if circuit breaker should trigger
        const windowMs = (ctx.state.config.dex_circuit_breaker_window_hours || 1) * 60 * 60 * 1000;
        const recentLosses = ctx.state.dexRecentStopLosses.filter(
          sl => Date.now() - sl.timestamp < windowMs
        );
        const maxLosses = ctx.state.config.dex_circuit_breaker_losses || 3;

        if (recentLosses.length >= maxLosses) {
          const pauseMs = (ctx.state.config.dex_circuit_breaker_pause_hours || 4) * 60 * 60 * 1000;
          ctx.state.dexCircuitBreakerUntil = Date.now() + pauseMs;
          ctx.log("DexMomentum", "circuit_breaker_triggered", {
            stopLossCount: recentLosses.length,
            windowHours: ctx.state.config.dex_circuit_breaker_window_hours || 1,
            pauseUntil: new Date(ctx.state.dexCircuitBreakerUntil).toISOString(),
            pauseHours: ctx.state.config.dex_circuit_breaker_pause_hours || 4,
            recentSymbols: recentLosses.map(sl => sl.symbol).join(", "),
          });
        }
      }

      // Remove position
      delete ctx.state.dexPositions[tokenAddress];

      ctx.log("DexMomentum", "paper_sell", {
        symbol: position.symbol,
        exitReason,
        entryPrice: "$" + position.entryPrice.toFixed(6),
        displayPrice: "$" + currentPrice.toFixed(6),
        exitPrice: "$" + exitPriceWithSlippage.toFixed(6),
        slippage: (sellSlippage * 100).toFixed(2) + "%",
        slippageModel,
        gasFee: gasFee.toFixed(4) + " SOL",
        pnlPct: actualPlPct.toFixed(1) + "%",
        pnlSol: pnlSol.toFixed(4) + " SOL",
        holdTime: ((Date.now() - position.entryTime) / 3600000).toFixed(1) + "h",
        totalRealizedPnL: ctx.state.dexRealizedPnL.toFixed(4) + " SOL",
        paperBalance: ctx.state.dexPaperBalance.toFixed(4) + " SOL",
      });

      // Send Telegram notification for DEX exit
      sendTradeAlert(ctx, "exit", {
        symbol: position.symbol,
        side: "SELL",
        price: exitPriceWithSlippage,
        pnlPercent: actualPlPct,
        reason: exitReason,
        market: "dex",
        details: {
          "Entry": "$" + position.entryPrice.toFixed(6),
          "Hold Time": ((Date.now() - position.entryTime) / 3600000).toFixed(1) + "h",
          "P&L SOL": pnlSol.toFixed(4),
        },
      });
    }
  }

  // Look for new entries
  const positionCount = Object.keys(ctx.state.dexPositions).length;
  if (positionCount >= ctx.state.config.dex_max_positions) return;

  // Check circuit breaker (#10) - pause new entries if too many stop losses
  // Now with stabilization-based early clearing
  if (ctx.state.dexCircuitBreakerUntil && Date.now() < ctx.state.dexCircuitBreakerUntil) {
    const minCooldownMs = (ctx.state.config.dex_breaker_min_cooldown_minutes ?? 30) * 60 * 1000;
    const breakerStartTime = ctx.state.dexCircuitBreakerUntil -
      ((ctx.state.config.dex_circuit_breaker_pause_hours ?? 1) * 60 * 60 * 1000);
    const minCooldownPassed = Date.now() >= breakerStartTime + minCooldownMs;

    // Check for early clear conditions after minimum cooldown
    if (minCooldownPassed) {
      // Condition 1: An open position has recovered to positive
      const hasRecoveredPosition = Object.values(ctx.state.dexPositions).some(pos => {
        const sig = ctx.state.dexSignals.find(s => s.tokenAddress === pos.tokenAddress);
        const curPrice = sig?.priceUsd || pos.entryPrice;
        const pl = ((curPrice - pos.entryPrice) / pos.entryPrice) * 100;
        return pl > 0;
      });

      // Condition 2: High conviction signal available
      const highConvictionSignal = ctx.state.dexSignals.some(s =>
        s.momentumScore >= (ctx.state.config.dex_reentry_min_momentum ?? 70) &&
        !Object.keys(ctx.state.dexPositions).includes(s.tokenAddress)
      );

      if (hasRecoveredPosition || highConvictionSignal) {
        ctx.state.dexCircuitBreakerUntil = null;
        ctx.log("DexMomentum", "circuit_breaker_early_clear", {
          reason: hasRecoveredPosition ? "position_recovered" : "high_conviction_signal",
          minutesPaused: Math.round((Date.now() - breakerStartTime) / 60000),
        });
      } else {
        ctx.log("DexMomentum", "circuit_breaker_active", {
          pausedUntil: new Date(ctx.state.dexCircuitBreakerUntil).toISOString(),
          remainingMinutes: Math.round((ctx.state.dexCircuitBreakerUntil - Date.now()) / 60000),
          minCooldownPassed: true,
          waitingFor: "position recovery or high conviction signal (momentum > 70)",
        });
        return;
      }
    } else {
      ctx.log("DexMomentum", "circuit_breaker_active", {
        pausedUntil: new Date(ctx.state.dexCircuitBreakerUntil).toISOString(),
        remainingMinutes: Math.round((ctx.state.dexCircuitBreakerUntil - Date.now()) / 60000),
        minCooldownPassed: false,
      });
      return;
    }
  } else if (ctx.state.dexCircuitBreakerUntil && Date.now() >= ctx.state.dexCircuitBreakerUntil) {
    // Circuit breaker time expired, clear it
    ctx.state.dexCircuitBreakerUntil = null;
    ctx.log("DexMomentum", "circuit_breaker_cleared", { reason: "time_expired" });
  }

  // Check drawdown pause (#11) - pause new entries if max drawdown exceeded
  if (ctx.state.dexDrawdownPaused) {
    ctx.log("DexMomentum", "drawdown_pause_active", {
      reason: "Max drawdown limit exceeded",
    });
    return;
  }

  // Calculate position size:
  // 1. Use percentage of current balance
  // 2. Cap at max_position_sol
  // 3. Ensure minimum viable position (0.01 SOL)
  const pctSize = (ctx.state.config.dex_position_size_pct || 33) / 100;
  const maxCap = ctx.state.config.dex_max_position_sol || 1.0;
  const minPosition = 0.01; // Minimum viable position

  if (ctx.state.dexPaperBalance < minPosition) {
    return; // Not enough paper balance
  }

  // Clean up old cooldowns (#8) - remove entries older than 24 hours to prevent memory bloat
  if (ctx.state.dexStopLossCooldowns) {
    const now = Date.now();
    const maxCooldownAge = 24 * 60 * 60 * 1000; // 24 hours
    for (const [tokenAddr, cooldown] of Object.entries(ctx.state.dexStopLossCooldowns)) {
      // Handle both old format (number) and new format (object)
      const exitTime = typeof cooldown === 'number' ? cooldown : cooldown.exitTime;
      if (now - exitTime > maxCooldownAge) {
        delete ctx.state.dexStopLossCooldowns[tokenAddr];
      }
    }
  }

  // FIX 2: Create Birdeye provider for re-entry chart analysis
  // This checks for dead cat bounces before allowing price-based re-entry
  const reentryBirdeye = ctx.state.config.dex_chart_analysis_enabled && ctx.env.BIRDEYE_API_KEY
    ? createBirdeyeProvider(ctx.env.BIRDEYE_API_KEY)
    : null;

  // Helper function to check if re-entry should be blocked due to dead cat bounce
  const isDeadCatBounce = async (s: typeof ctx.state.dexSignals[0]): Promise<boolean> => {
    if (!reentryBirdeye) return false; // No Birdeye = allow re-entry
    try {
      const analysis = await reentryBirdeye.analyzeChart(s.tokenAddress, s.ageHours);
      if (analysis && analysis.recommendation === 'avoid') {
        ctx.log("DexMomentum", "reentry_blocked_dead_cat", {
          symbol: s.symbol,
          chartScore: analysis.entryScore,
          recommendation: analysis.recommendation,
          trend: analysis.indicators.trend,
          volumeProfile: analysis.indicators.volumeProfile,
          patterns: analysis.patterns.map(p => p.pattern).join(", ") || "none",
          reason: "Chart analysis indicates poor entry point - likely dead cat bounce",
        });
        return true;
      }
      return false;
    } catch (e) {
      // Chart analysis failed - allow re-entry (don't block on API errors)
      ctx.log("DexMomentum", "reentry_chart_check_failed", {
        symbol: s.symbol,
        error: String(e),
        action: "Allowing re-entry despite chart check failure",
      });
      return false;
    }
  };

  // Build candidates list with sync filters first, then apply async chart check
  const syncFilteredCandidates = ctx.state.dexSignals
    .filter(s => !heldTokens.has(s.tokenAddress))
    .filter(s => s.momentumScore >= 60) // Minimum momentum score threshold (raised from 50 for quality)
    .slice(0, 10); // Pre-limit before async checks

  // Apply cooldown checks with async dead cat bounce detection
  const candidatePromises = syncFilteredCandidates.map(async (s) => {
    if (!ctx.state.dexStopLossCooldowns) return s;
    const cooldown = ctx.state.dexStopLossCooldowns[s.tokenAddress];
    if (!cooldown) return s;

    // Handle legacy format (just a number timestamp)
    if (typeof cooldown === 'number') {
      return Date.now() >= cooldown ? s : null;
    }

    const recoveryPct = ctx.state.config.dex_reentry_recovery_pct ?? 15;
    const minMomentum = ctx.state.config.dex_reentry_min_momentum ?? 70;

    // Check if price has recovered X% above exit price
    const priceRecoveryThreshold = cooldown.exitPrice * (1 + recoveryPct / 100);
    if (s.priceUsd >= priceRecoveryThreshold) {
      // FIX 2: Before allowing re-entry on price recovery, verify with chart analysis
      // This prevents buying dead cat bounces
      const isDeadCat = await isDeadCatBounce(s);
      if (isDeadCat) {
        return null; // Block re-entry - dead cat bounce detected
      }

      ctx.log("DexMomentum", "cooldown_cleared_price_recovery", {
        symbol: s.symbol,
        exitPrice: cooldown.exitPrice.toFixed(6),
        currentPrice: s.priceUsd.toFixed(6),
        recoveryPct: (((s.priceUsd - cooldown.exitPrice) / cooldown.exitPrice) * 100).toFixed(1) + "%",
        chartVerified: reentryBirdeye ? "yes" : "skipped",
      });
      delete ctx.state.dexStopLossCooldowns[s.tokenAddress];
      return s;
    }

    // Allow re-entry if momentum score is very strong AND minimum time has passed
    // This prevents immediate re-entry on dead cat bounces
    const minCooldownMs = 5 * 60 * 1000; // 5 minutes minimum after any stop loss
    const timeSinceExit = Date.now() - cooldown.exitTime;

    if (s.momentumScore >= minMomentum && timeSinceExit >= minCooldownMs) {
      // FIX 2: Also verify high-momentum re-entries with chart analysis
      const isDeadCat = await isDeadCatBounce(s);
      if (isDeadCat) {
        return null; // Block re-entry - dead cat bounce detected
      }

      ctx.log("DexMomentum", "cooldown_cleared_high_momentum", {
        symbol: s.symbol,
        momentumScore: s.momentumScore.toFixed(1),
        threshold: minMomentum,
        minutesSinceExit: Math.round(timeSinceExit / 60000),
        chartVerified: reentryBirdeye ? "yes" : "skipped",
      });
      delete ctx.state.dexStopLossCooldowns[s.tokenAddress];
      return s;
    } else if (s.momentumScore >= minMomentum && timeSinceExit < minCooldownMs) {
      ctx.log("DexMomentum", "cooldown_waiting_min_time", {
        symbol: s.symbol,
        momentumScore: s.momentumScore.toFixed(1),
        minutesSinceExit: Math.round(timeSinceExit / 60000),
        minMinutesRequired: 5,
      });
      return null;
    }

    // Fallback: allow re-entry after time expires
    if (Date.now() >= cooldown.fallbackExpiry) {
      ctx.log("DexMomentum", "cooldown_cleared_time_expired", {
        symbol: s.symbol,
      });
      delete ctx.state.dexStopLossCooldowns[s.tokenAddress];
      return s;
    }

    return null;
  });

  const candidateResults = await Promise.all(candidatePromises);
  const candidates = candidateResults.filter((s): s is NonNullable<typeof s> => s !== null).slice(0, 3);

  // Count current positions by tier for limit checks
  const tierCounts = {
    microspray: 0,
    breakout: 0,
    lottery: 0,
  };
  for (const p of Object.values(ctx.state.dexPositions)) {
    if (p.tier === 'microspray') tierCounts.microspray++;
    else if (p.tier === 'breakout') tierCounts.breakout++;
    else if (p.tier === 'lottery') tierCounts.lottery++;
  }
  const maxMicroSpray = ctx.state.config.dex_microspray_max_positions ?? 10;
  const maxBreakout = ctx.state.config.dex_breakout_max_positions ?? 5;
  const maxLotteryPositions = ctx.state.config.dex_lottery_max_positions ?? 5;

  ctx.log("DexMomentum", "buy_candidates", {
    count: candidates.length,
    candidates: candidates.map(c => `${c.symbol}(${c.tier})`).join(", "),
  });

  // Create Birdeye provider once outside loop so throttle works across all candidates
  const birdeye = ctx.state.config.dex_chart_analysis_enabled && ctx.env.BIRDEYE_API_KEY
    ? createBirdeyeProvider(ctx.env.BIRDEYE_API_KEY)
    : null;

  for (const candidate of candidates) {
    if (Object.keys(ctx.state.dexPositions).length >= ctx.state.config.dex_max_positions) break;

    // Check tier-specific position limits
    if (candidate.tier === 'microspray' && tierCounts.microspray >= maxMicroSpray) {
      ctx.log("DexMomentum", "microspray_limit_reached", {
        symbol: candidate.symbol,
        current: tierCounts.microspray,
        max: maxMicroSpray,
      });
      continue;
    }
    if (candidate.tier === 'breakout' && tierCounts.breakout >= maxBreakout) {
      ctx.log("DexMomentum", "breakout_limit_reached", {
        symbol: candidate.symbol,
        current: tierCounts.breakout,
        max: maxBreakout,
      });
      continue;
    }
    if (candidate.tier === 'lottery' && tierCounts.lottery >= maxLotteryPositions) {
      ctx.log("DexMomentum", "lottery_limit_reached", {
        symbol: candidate.symbol,
        current: tierCounts.lottery,
        max: maxLotteryPositions,
      });
      continue;
    }

    // Chart pattern analysis - check if this is a good entry point
    if (birdeye) {
      try {
        const chartAnalysis = await birdeye.analyzeChart(candidate.tokenAddress, candidate.ageHours);
        const minScore = ctx.state.config.dex_chart_min_entry_score ?? 40;

        if (chartAnalysis) {
          ctx.log("DexMomentum", "chart_analysis", {
            symbol: candidate.symbol,
            timeframe: chartAnalysis.timeframe,
            candles: chartAnalysis.candles,
            entryScore: chartAnalysis.entryScore,
            recommendation: chartAnalysis.recommendation,
            trend: chartAnalysis.indicators.trend,
            volumeProfile: chartAnalysis.indicators.volumeProfile,
            patterns: chartAnalysis.patterns.map(p => p.pattern).join(", ") || "none",
          });

          if (chartAnalysis.entryScore < minScore) {
            ctx.log("DexMomentum", "skip_bad_chart", {
              symbol: candidate.symbol,
              entryScore: chartAnalysis.entryScore,
              minRequired: minScore,
              recommendation: chartAnalysis.recommendation,
              reason: chartAnalysis.patterns.find(p => p.signal === "bearish")?.description || "Low entry score",
            });
            continue; // Skip this candidate
          }
        } else {
          ctx.log("DexMomentum", "chart_analysis_no_data", {
            symbol: candidate.symbol,
            reason: "Insufficient candle data (token too new)",
          });
        }
      } catch (e) {
        // Chart analysis failed - continue without it (don't block trade)
        ctx.log("DexMomentum", "chart_analysis_error", {
          symbol: candidate.symbol,
          error: String(e),
        });
      }
    }

    // Calculate position size for this trade (tier-specific)
    let solAmount: number;

    if (candidate.tier === 'microspray') {
      // Micro-spray tier: ultra-tiny position
      solAmount = ctx.state.config.dex_microspray_position_sol ?? 0.005;
      ctx.log("DexMomentum", "microspray_tier_sizing", {
        symbol: candidate.symbol,
        tier: "microspray",
        fixedSize: solAmount.toFixed(4) + " SOL",
        ageMinutes: (candidate.ageHours * 60).toFixed(0),
      });
      tierCounts.microspray++;
    } else if (candidate.tier === 'breakout') {
      // Breakout tier: small position for rapid pump plays
      solAmount = ctx.state.config.dex_breakout_position_sol ?? 0.015;
      ctx.log("DexMomentum", "breakout_tier_sizing", {
        symbol: candidate.symbol,
        tier: "breakout",
        fixedSize: solAmount.toFixed(4) + " SOL",
        priceChange5m: candidate.priceChange5m?.toFixed(1) + "%",
        ageHours: candidate.ageHours.toFixed(1),
      });
      tierCounts.breakout++;
    } else if (candidate.tier === 'lottery') {
      // Lottery tier: fixed tiny position (lottery ticket)
      solAmount = ctx.state.config.dex_lottery_position_sol ?? 0.02;
      ctx.log("DexMomentum", "lottery_tier_sizing", {
        symbol: candidate.symbol,
        tier: "lottery",
        fixedSize: solAmount.toFixed(4) + " SOL",
        ageHours: candidate.ageHours.toFixed(1),
      });
      tierCounts.lottery++;
    } else if (candidate.tier === 'early') {
      // Early tier: reduced position size
      const earlyMultiplier = (ctx.state.config.dex_early_position_size_pct ?? 50) / 100;
      const tierPctSize = pctSize * earlyMultiplier;
      const calculatedSize = ctx.state.dexPaperBalance * tierPctSize;
      solAmount = Math.min(calculatedSize, maxCap);
      ctx.log("DexMomentum", "early_tier_sizing", {
        symbol: candidate.symbol,
        tier: "early",
        normalPct: (pctSize * 100).toFixed(0) + "%",
        adjustedPct: (tierPctSize * 100).toFixed(0) + "%",
        legitimacy: candidate.legitimacyScore,
      });
    } else {
      // Established tier: normal position size
      const calculatedSize = ctx.state.dexPaperBalance * pctSize;
      solAmount = Math.min(calculatedSize, maxCap);
    }

    if (ctx.state.dexPaperBalance < minPosition || solAmount < minPosition) break;

    // Calculate total portfolio value for concentration limit check
    let totalPositionValueSol = 0;
    for (const [tokenAddr, pos] of Object.entries(ctx.state.dexPositions)) {
      const sig = ctx.state.dexSignals.find(s => s.tokenAddress === tokenAddr);
      const price = sig?.priceUsd || pos.entryPrice;
      const valueUsd = pos.tokenAmount * price;
      totalPositionValueSol += valueUsd / solPriceUsd;
    }
    const totalPortfolioSol = ctx.state.dexPaperBalance + totalPositionValueSol;

    // Apply position concentration limit (default 40%)
    // Note: This only limits new positions - if a position grows beyond the limit due to gains, that's fine
    const maxConcentrationPct = ctx.state.config.dex_max_single_position_pct || 40;
    const maxPositionSol = totalPortfolioSol * (maxConcentrationPct / 100);
    let reducedDueToConcentration = false;
    const originalSolAmount = solAmount;

    if (solAmount > maxPositionSol) {
      solAmount = maxPositionSol;
      reducedDueToConcentration = true;
    }

    // Ensure we still have a viable position after concentration limit
    if (solAmount < minPosition) {
      ctx.log("DexMomentum", "skip_concentration_limit", {
        symbol: candidate.symbol,
        reason: "Position too small after concentration limit",
        originalSize: originalSolAmount.toFixed(4) + " SOL",
        maxAllowed: maxPositionSol.toFixed(4) + " SOL",
        concentrationLimit: maxConcentrationPct + "%",
        totalPortfolio: totalPortfolioSol.toFixed(4) + " SOL",
      });
      continue;
    }

    // Calculate token amount (simulated)
    // Assume 1 SOL = ~$200 for rough calculation, actual would use Jupiter quote
    const usdAmount = solAmount * solPriceUsd;

    // Apply slippage to entry price (buying pushes price up = worse entry)
    const slippageModel = ctx.state.config.dex_slippage_model || "realistic";
    const buySlippage = calculateDexSlippage(
      slippageModel,
      usdAmount,
      candidate.liquidity
    );
    const entryPriceWithSlippage = candidate.priceUsd * (1 + buySlippage);

    // Token amount is based on slipped price (fewer tokens due to slippage)
    const tokenAmount = usdAmount / entryPriceWithSlippage;

    // Create paper position
    const position: DexPosition = {
      tokenAddress: candidate.tokenAddress,
      symbol: candidate.symbol,
      entryPrice: entryPriceWithSlippage,
      entrySol: solAmount,
      entryTime: Date.now(),
      tokenAmount,
      peakPrice: entryPriceWithSlippage,
      entryMomentumScore: candidate.momentumScore, // Track for decay detection (#12)
      entryLiquidity: candidate.liquidity, // Track for exit safety (#13)
      tier: candidate.tier, // Track for tier-specific rules
    };

    ctx.state.dexPositions[candidate.tokenAddress] = position;
    ctx.state.dexPaperBalance -= solAmount;

    // Deduct gas fee for the buy transaction
    ctx.state.dexPaperBalance -= gasFee;
    ctx.log("DexMomentum", "gas_fee_deducted", {
      action: "buy",
      symbol: candidate.symbol,
      gasFee: gasFee.toFixed(4) + " SOL",
      gasFeeUsd: "$" + (gasFee * solPriceUsd).toFixed(2),
    });

    // Build log data with concentration limit info if applied
    const logData: Record<string, unknown> = {
      symbol: candidate.symbol,
      name: candidate.name,
      tokenAddress: candidate.tokenAddress,
      displayPrice: "$" + candidate.priceUsd.toFixed(6),
      entryPrice: "$" + entryPriceWithSlippage.toFixed(6),
      slippage: (buySlippage * 100).toFixed(2) + "%",
      slippageModel,
      solAmount: solAmount.toFixed(4) + " SOL",
      gasFee: gasFee.toFixed(4) + " SOL",
      tokenAmount: tokenAmount.toFixed(2),
      priceChange24h: candidate.priceChange24h.toFixed(1) + "%",
      momentumScore: candidate.momentumScore.toFixed(1),
      liquidity: "$" + Math.round(candidate.liquidity).toLocaleString(),
      paperBalance: ctx.state.dexPaperBalance.toFixed(4) + " SOL remaining",
      url: candidate.url,
      mode: "PAPER TRADING",
    };

    if (reducedDueToConcentration) {
      logData.concentrationLimitApplied = true;
      logData.originalSize = originalSolAmount.toFixed(4) + " SOL";
      logData.reducedTo = solAmount.toFixed(4) + " SOL";
      logData.concentrationLimit = maxConcentrationPct + "%";
      logData.portfolioValue = totalPortfolioSol.toFixed(4) + " SOL";
      ctx.log("DexMomentum", "paper_buy_reduced", logData);
    } else {
      ctx.log("DexMomentum", "paper_buy", logData);
    }

    // Send Telegram notification for DEX entry
    sendTradeAlert(ctx, "entry", {
      symbol: candidate.symbol,
      side: "BUY",
      price: entryPriceWithSlippage,
      reason: `Momentum ${candidate.momentumScore.toFixed(0)} | ${candidate.tier} tier`,
      market: "dex",
      details: {
        "Size": solAmount.toFixed(4) + " SOL",
        "Liquidity": "$" + Math.round(candidate.liquidity).toLocaleString(),
        "24h Change": candidate.priceChange24h.toFixed(1) + "%",
      },
    });
  }
}

/**
 * Record a portfolio snapshot for tracking value over time.
 * Also handles maximum drawdown protection logic.
 */
export async function recordDexSnapshot(ctx: HarnessContext): Promise<void> {
  // Fetch real SOL price (cached for 5 minutes)
  const solPriceUsd = await getSolPriceUsd();
  let positionValueSol = 0;

  for (const [tokenAddress, pos] of Object.entries(ctx.state.dexPositions)) {
    const signal = ctx.state.dexSignals.find(s => s.tokenAddress === tokenAddress);
    const currentPrice = signal?.priceUsd || pos.lastKnownPrice || pos.entryPrice;
    const currentValueUsd = pos.tokenAmount * currentPrice;
    positionValueSol += currentValueUsd / solPriceUsd;
  }

  const snapshot: DexPortfolioSnapshot = {
    timestamp: Date.now(),
    totalValueSol: ctx.state.dexPaperBalance + positionValueSol,
    paperBalanceSol: ctx.state.dexPaperBalance,
    positionValueSol,
    realizedPnLSol: ctx.state.dexRealizedPnL,
  };

  // Initialize if needed
  if (!ctx.state.dexPortfolioHistory) {
    ctx.state.dexPortfolioHistory = [];
  }

  ctx.state.dexPortfolioHistory.push(snapshot);

  // Keep last 100 snapshots (roughly 50 minutes at 30s intervals, or longer if running less frequently)
  if (ctx.state.dexPortfolioHistory.length > 100) {
    ctx.state.dexPortfolioHistory = ctx.state.dexPortfolioHistory.slice(-100);
  }

  // ========== Maximum Drawdown Protection (#11) ==========
  const totalValueSol = ctx.state.dexPaperBalance + positionValueSol;

  // Initialize peak value if not set (use starting balance or current value)
  if (!ctx.state.dexPeakValue || ctx.state.dexPeakValue === 0) {
    ctx.state.dexPeakValue = ctx.state.config.dex_starting_balance_sol || 1.0;
  }

  // Update peak value (high water mark)
  if (totalValueSol > ctx.state.dexPeakValue) {
    ctx.state.dexPeakValue = totalValueSol;
    // Reset drawdown pause if we make new highs
    if (ctx.state.dexDrawdownPaused) {
      ctx.state.dexDrawdownPaused = false;
      ctx.log("DexMomentum", "drawdown_pause_lifted", {
        newPeakValue: totalValueSol.toFixed(4) + " SOL",
        reason: "New high water mark reached",
      });
    }
  }

  // Calculate current drawdown
  const drawdownPct = ((ctx.state.dexPeakValue - totalValueSol) / ctx.state.dexPeakValue) * 100;
  const maxDrawdownPct = ctx.state.config.dex_max_drawdown_pct || 25;

  // Check if drawdown exceeds limit
  if (drawdownPct >= maxDrawdownPct && !ctx.state.dexDrawdownPaused) {
    ctx.state.dexDrawdownPaused = true;
    ctx.log("DexMomentum", "max_drawdown_triggered", {
      currentValue: totalValueSol.toFixed(4) + " SOL",
      peakValue: ctx.state.dexPeakValue.toFixed(4) + " SOL",
      drawdownPct: drawdownPct.toFixed(1) + "%",
      maxDrawdownPct: maxDrawdownPct + "%",
      action: "New entries paused until recovery",
    });
  }
}

/**
 * Emergency close all DEX positions (used during crisis mode).
 */
export async function closeAllDexPositions(ctx: HarnessContext, reason: string): Promise<void> {
  const positions = Object.values(ctx.state.dexPositions);
  if (positions.length === 0) return;

  ctx.log("Crisis", "closing_all_dex_positions", {
    count: positions.length,
    reason,
  });

  for (const pos of positions) {
    // Find current signal for price
    const signal = ctx.state.dexSignals.find(s => s.tokenAddress === pos.tokenAddress);
    const currentPrice = signal?.priceUsd || pos.lastKnownPrice || pos.entryPrice;

    const pnlPct = ((currentPrice - pos.entryPrice) / pos.entryPrice) * 100;
    const exitValue = (currentPrice / pos.entryPrice) * pos.entrySol;
    const pnlSol = exitValue - pos.entrySol;

    // Record trade
    ctx.state.dexTradeHistory.push({
      symbol: pos.symbol,
      tokenAddress: pos.tokenAddress,
      entryPrice: pos.entryPrice,
      exitPrice: currentPrice,
      entrySol: pos.entrySol,
      entryTime: pos.entryTime,
      exitTime: Date.now(),
      pnlPct,
      pnlSol,
      exitReason: "manual",
    });

    // Update balance
    ctx.state.dexPaperBalance += exitValue;
    ctx.state.dexRealizedPnL += pnlSol;

    // Remove position
    delete ctx.state.dexPositions[pos.tokenAddress];

    ctx.log("Crisis", "dex_position_closed", {
      symbol: pos.symbol,
      pnlPct: pnlPct.toFixed(2),
      pnlSol: pnlSol.toFixed(4),
      reason,
    });
  }
}
