/**
 * HTTP Route Handlers Module
 *
 * Handles all HTTP API endpoints for the MahoragaHarness durable object.
 * These routes provide the management interface for the trading agent.
 */

import type { HarnessContext } from "./context";
import type { AgentConfig, CrisisLevel } from "./types";
import { createAlpacaProviders } from "../../providers/alpaca";
import { getSolPriceUsd, calculateDexTradingMetrics } from "./utils";

/**
 * Extended context for route handlers that need additional capabilities
 * beyond the base HarnessContext (e.g., alarm scheduling, LLM reinitialization)
 */
export interface RoutesContext extends HarnessContext {
  /** Reinitialize the LLM provider after config changes */
  initializeLLM: () => void;

  /** Schedule the next alarm for background processing */
  scheduleNextAlarm: () => Promise<void>;

  /** Delete all scheduled alarms */
  deleteAlarm: () => Promise<void>;
}

/**
 * GET /status - Get full agent status
 *
 * Returns comprehensive status including:
 * - Trading enabled state
 * - Alpaca account info and positions
 * - Market clock
 * - Configuration
 * - Signal cache and research
 * - DEX positions with live P&L
 * - Crisis state
 */
export async function handleStatus(ctx: RoutesContext): Promise<Response> {
  const alpaca = createAlpacaProviders(ctx.env);

  let account = null;
  let positions: Array<{
    symbol: string;
    avg_entry_price?: number;
    current_price: number;
    asset_class?: string;
  }> = [];
  let clock = null;

  try {
    [account, positions, clock] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
      alpaca.trading.getClock(),
    ]);

    // Update entry prices from Alpaca if we don't have them
    for (const pos of positions || []) {
      const entry = ctx.state.positionEntries[pos.symbol];
      if (entry && entry.entry_price === 0 && pos.avg_entry_price) {
        entry.entry_price = pos.avg_entry_price;
        entry.peak_price = Math.max(entry.peak_price, pos.current_price);
      }
    }
  } catch {
    // Ignore - will return null values
  }

  // Fetch real SOL price (cached for 5 minutes)
  const solPriceUsd = await getSolPriceUsd();

  // Calculate DEX positions with current P&L
  const dexPositionsWithPnL = Object.entries(ctx.state.dexPositions).map(([tokenAddress, pos]) => {
    const currentSignal = ctx.state.dexSignals.find(s => s.tokenAddress === tokenAddress);
    // Use signal price > lastKnownPrice > entryPrice (consistent with trading logic)
    let currentPrice: number;
    if (currentSignal?.priceUsd) {
      currentPrice = currentSignal.priceUsd;
      // Update lastKnownPrice when we have a signal
      pos.lastKnownPrice = currentPrice;
    } else if (pos.lastKnownPrice) {
      currentPrice = pos.lastKnownPrice;
    } else {
      currentPrice = pos.entryPrice;
    }

    // Handle legacy positions where tokenAmount/entrySol wasn't stored or is NaN
    const entrySol = (pos.entrySol == null || Number.isNaN(pos.entrySol))
      ? (ctx.state.config.dex_max_position_sol ?? 0.1)
      : pos.entrySol;
    const tokenAmount = (pos.tokenAmount == null || Number.isNaN(pos.tokenAmount))
      ? ((entrySol * solPriceUsd) / pos.entryPrice)
      : pos.tokenAmount;

    const currentValue = tokenAmount * currentPrice;
    const entryValue = tokenAmount * pos.entryPrice;
    const unrealizedPl = currentValue - entryValue;
    const unrealizedPlPct = ((currentPrice - pos.entryPrice) / pos.entryPrice) * 100;

    return {
      ...pos,
      tokenAmount,
      currentPrice,
      currentValue,
      unrealizedPl,
      unrealizedPlPct,
      holdingHours: (Date.now() - pos.entryTime) / (1000 * 60 * 60),
    };
  });

  return ctx.jsonResponse({
    ok: true,
    data: {
      enabled: ctx.state.enabled,
      account,
      positions,
      clock,
      config: ctx.state.config,
      signals: ctx.state.signalCache,
      logs: ctx.state.logs.slice(-100),
      costs: ctx.state.costTracker,
      lastAnalystRun: ctx.state.lastAnalystRun,
      lastResearchRun: ctx.state.lastResearchRun,
      signalResearch: ctx.state.signalResearch,
      positionResearch: ctx.state.positionResearch,
      positionEntries: ctx.state.positionEntries,
      twitterConfirmations: ctx.state.twitterConfirmations,
      premarketPlan: ctx.state.premarketPlan,
      stalenessAnalysis: ctx.state.stalenessAnalysis,
      // DEX positions with live P&L
      dexPositions: dexPositionsWithPnL,
      dexSignals: ctx.state.dexSignals.slice(0, 10), // Top 10 momentum signals
      dexPaperTrading: {
        enabled: true,
        paperBalance: (ctx.state.dexPaperBalance == null || Number.isNaN(ctx.state.dexPaperBalance)) ? 1.0 : ctx.state.dexPaperBalance,
        realizedPnL: (ctx.state.dexRealizedPnL == null || Number.isNaN(ctx.state.dexRealizedPnL)) ? 0 : ctx.state.dexRealizedPnL,
        totalTrades: ctx.state.dexTradeHistory?.length ?? 0,
        winningTrades: ctx.state.dexTradeHistory?.filter(t => t.pnlPct > 0).length ?? 0,
        losingTrades: ctx.state.dexTradeHistory?.filter(t => t.pnlPct <= 0).length ?? 0,
        recentTrades: ctx.state.dexTradeHistory?.slice(-50) ?? [], // Keep last 50 trades for history
        // Trading metrics (#15, #16, #17)
        ...calculateDexTradingMetrics(ctx.state.dexTradeHistory ?? [], ctx.state),
        // Circuit breaker status (#10)
        circuitBreakerActive: ctx.state.dexCircuitBreakerUntil ? Date.now() < ctx.state.dexCircuitBreakerUntil : false,
        circuitBreakerUntil: ctx.state.dexCircuitBreakerUntil,
        recentStopLosses: ctx.state.dexRecentStopLosses?.length ?? 0,
        // Drawdown protection status (#11)
        drawdownPaused: ctx.state.dexDrawdownPaused ?? false,
        peakValue: ctx.state.dexPeakValue ?? 0,
        currentDrawdownPct: ctx.state.dexPeakValue && ctx.state.dexPeakValue > 0
          ? ((ctx.state.dexPeakValue - (ctx.state.dexPaperBalance ?? 0)) / ctx.state.dexPeakValue * 100)
          : 0,
      },
      dexPortfolioHistory: ctx.state.dexPortfolioHistory?.slice(-50) ?? [], // Last 50 snapshots for charting
      // Crisis Mode status
      crisisState: ctx.state.crisisState,
      lastCrisisCheck: ctx.state.lastCrisisCheck,
    },
  });
}

/**
 * POST /config - Update agent configuration
 *
 * Accepts partial config updates and merges with existing config.
 * Reinitializes LLM provider if model settings change.
 */
export async function handleUpdateConfig(ctx: RoutesContext, request: Request): Promise<Response> {
  const body = await request.json() as Partial<AgentConfig>;
  ctx.state.config = { ...ctx.state.config, ...body };
  ctx.initializeLLM();
  await ctx.persist();
  return ctx.jsonResponse({ ok: true, config: ctx.state.config });
}

/**
 * POST /enable - Enable the trading agent
 *
 * Starts background processing via alarms.
 */
export async function handleEnable(ctx: RoutesContext): Promise<Response> {
  ctx.state.enabled = true;
  await ctx.persist();
  await ctx.scheduleNextAlarm();
  ctx.log("System", "agent_enabled", {});
  return ctx.jsonResponse({ ok: true, enabled: true });
}

/**
 * POST /disable - Disable the trading agent
 *
 * Stops background processing by cancelling alarms.
 * Does NOT close existing positions.
 */
export async function handleDisable(ctx: RoutesContext): Promise<Response> {
  ctx.state.enabled = false;
  await ctx.deleteAlarm();
  await ctx.persist();
  ctx.log("System", "agent_disabled", {});
  return ctx.jsonResponse({ ok: true, enabled: false });
}

/**
 * POST /dex/reset - Reset DEX paper trading
 *
 * Clears all DEX positions and resets to starting balance.
 * Also resets all tracking metrics and circuit breakers.
 */
export async function handleDexReset(ctx: RoutesContext): Promise<Response> {
  // Use configured starting balance, fallback to 1 SOL
  const startingBalance = ctx.state.config.dex_starting_balance_sol || 1.0;
  ctx.state.dexPositions = {};
  ctx.state.dexSignals = [];
  ctx.state.dexTradeHistory = [];
  ctx.state.dexRealizedPnL = 0;
  ctx.state.dexPaperBalance = startingBalance;
  ctx.state.dexPortfolioHistory = []; // Clear history on reset
  // Reset streak and drawdown tracking (#15, #16, #17)
  ctx.state.dexMaxConsecutiveLosses = 0;
  ctx.state.dexCurrentLossStreak = 0;
  ctx.state.dexMaxDrawdownPct = 0;
  ctx.state.dexMaxDrawdownDuration = 0;
  ctx.state.dexDrawdownStartTime = null;
  ctx.state.dexPeakBalance = startingBalance;
  // Reset circuit breaker state (#10)
  ctx.state.dexRecentStopLosses = [];
  ctx.state.dexCircuitBreakerUntil = null;
  // Reset drawdown protection state (#11)
  ctx.state.dexPeakValue = startingBalance;
  ctx.state.dexDrawdownPaused = false;
  // Reset stop loss cooldowns (#8) - allow fresh entries on all tokens
  ctx.state.dexStopLossCooldowns = {};
  await ctx.persist();
  ctx.log("DexMomentum", "paper_reset", { startingBalance: startingBalance + " SOL" });
  return ctx.jsonResponse({
    ok: true,
    message: "DEX paper trading reset",
    paperBalance: startingBalance,
  });
}

/**
 * POST /dex/clear-cooldowns - Clear all stop-loss cooldowns
 *
 * Allows re-entry on tokens that were previously stopped out.
 */
export async function handleDexClearCooldowns(ctx: RoutesContext): Promise<Response> {
  const clearedCount = Object.keys(ctx.state.dexStopLossCooldowns || {}).length;
  ctx.state.dexStopLossCooldowns = {};
  await ctx.persist();
  ctx.log("DexMomentum", "cooldowns_cleared", { count: clearedCount });
  return ctx.jsonResponse({
    ok: true,
    message: `Cleared ${clearedCount} token cooldowns`,
    clearedCount,
  });
}

/**
 * POST /dex/clear-breaker - Clear circuit breaker
 *
 * Manually clears the circuit breaker to allow trading to resume.
 */
export async function handleDexClearBreaker(ctx: RoutesContext): Promise<Response> {
  const wasActive = !!ctx.state.dexCircuitBreakerUntil;
  ctx.state.dexCircuitBreakerUntil = null;
  ctx.state.dexRecentStopLosses = [];
  await ctx.persist();
  ctx.log("DexMomentum", "breaker_manually_cleared", { wasActive });
  return ctx.jsonResponse({
    ok: true,
    message: wasActive ? "Circuit breaker cleared" : "Circuit breaker was not active",
    wasActive,
  });
}

/**
 * GET /logs - Get agent logs
 *
 * Returns recent log entries. Supports limit parameter.
 */
export function handleGetLogs(ctx: RoutesContext, url: URL): Response {
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const logs = ctx.state.logs.slice(-limit);
  return ctx.jsonResponse({ logs });
}

/**
 * POST /kill-switch - Emergency stop
 *
 * Immediately disables agent and clears all pending signals.
 * Does NOT automatically close existing positions.
 */
export async function handleKillSwitch(ctx: RoutesContext): Promise<Response> {
  ctx.state.enabled = false;
  await ctx.deleteAlarm();
  ctx.state.signalCache = [];
  ctx.state.signalResearch = {};
  ctx.state.premarketPlan = null;
  await ctx.persist();
  ctx.log("System", "kill_switch_activated", { timestamp: new Date().toISOString() });
  return ctx.jsonResponse({
    ok: true,
    message: "KILL SWITCH ACTIVATED. Agent disabled, alarms cancelled, signal cache cleared.",
    note: "Existing positions are NOT automatically closed. Review and close manually if needed."
  });
}

/**
 * POST /crisis/toggle - Toggle crisis mode settings
 *
 * Allows manual override of crisis level for testing or emergency situations.
 */
export async function handleCrisisToggle(ctx: RoutesContext, request: Request): Promise<Response> {
  const body = await request.json() as { manualOverride?: boolean; level?: CrisisLevel };

  // Toggle manual override
  if (body.manualOverride !== undefined) {
    ctx.state.crisisState.manualOverride = body.manualOverride;
    ctx.log("Crisis", "manual_override_changed", {
      manualOverride: body.manualOverride,
    });
  }

  // Manually set crisis level (only when override is active)
  if (body.level !== undefined && ctx.state.crisisState.manualOverride) {
    const previousLevel = ctx.state.crisisState.level;
    ctx.state.crisisState.level = body.level;
    ctx.state.crisisState.lastLevelChange = Date.now();
    ctx.log("Crisis", "manual_level_set", {
      previous: previousLevel,
      current: body.level,
    });
  }

  await ctx.persist();

  return ctx.jsonResponse({
    ok: true,
    crisisState: ctx.state.crisisState,
  });
}
