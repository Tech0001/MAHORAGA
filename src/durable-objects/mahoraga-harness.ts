/**
 * MahoragaHarness - Autonomous Trading Agent Durable Object
 * 
 * A fully autonomous trading agent that runs 24/7 on Cloudflare Workers.
 * This is the "harness" - customize it to match your trading strategy.
 * 
 * ═══════════════════════════════════════════════════════════════════════════
 * HOW TO CUSTOMIZE THIS AGENT
 * ═══════════════════════════════════════════════════════════════════════════
 * 
 * 1. CONFIGURATION (AgentConfig & DEFAULT_CONFIG)
 *    - Tune risk parameters, position sizes, thresholds
 *    - Enable/disable features (options, crypto, staleness)
 *    - Set LLM models and token limits
 * 
 * 2. DATA SOURCES (runDataGatherers, gatherStockTwits, gatherReddit, etc.)
 *    - Add new data sources (news APIs, alternative data)
 *    - Modify scraping logic and sentiment analysis
 *    - Adjust source weights in SOURCE_CONFIG
 * 
 * 3. TRADING LOGIC (runAnalyst, executeBuy, executeSell)
 *    - Change entry/exit rules
 *    - Modify position sizing formulas
 *    - Add custom indicators
 * 
 * 4. LLM PROMPTS (researchSignal, runPreMarketAnalysis)
 *    - Customize how the AI analyzes signals
 *    - Change research criteria and output format
 * 
 * 5. NOTIFICATIONS (sendDiscordNotification)
 *    - Set DISCORD_WEBHOOK_URL secret to enable
 *    - Modify what triggers notifications
 * 
 * Deploy with: wrangler deploy -c wrangler.v2.toml
 * ═══════════════════════════════════════════════════════════════════════════
 */

import { DurableObject } from "cloudflare:workers";
import type { Env } from "../env.d";
import { createAlpacaProviders } from "../providers/alpaca";
import type { Account, Position, MarketClock, LLMProvider } from "../providers/types";
import { createLLMProvider } from "../providers/llm/factory";
import { createDexScreenerProvider, type DexMomentumSignal } from "../providers/dexscreener";
import { createBirdeyeProvider } from "../providers/birdeye";
// TODO: Re-enable when Solana wallet is configured for execution
// import { createJupiterProvider } from "../providers/jupiter";

// ============================================================================
// SECTION 1: TYPES & CONFIGURATION
// ============================================================================
// [CUSTOMIZABLE] Modify these interfaces to add new fields for custom data sources.
// [CUSTOMIZABLE] AgentConfig contains ALL tunable parameters - start here!
// ============================================================================

// Crisis Mode Types - Black Swan Protection System
type CrisisLevel = 0 | 1 | 2 | 3; // 0=Normal, 1=Elevated, 2=High Alert, 3=Full Crisis

interface CrisisIndicators {
  // Volatility & Fear
  vix: number | null;                    // VIX index value (fear gauge)

  // Credit Markets
  highYieldSpread: number | null;        // High yield spread in basis points
  yieldCurve2Y10Y: number | null;        // 2Y/10Y Treasury spread (negative = inverted = recession)
  tedSpread: number | null;              // TED spread (LIBOR - T-bill, banking stress)

  // Crypto (risk indicator, not safe haven)
  btcPrice: number | null;               // BTC price
  btcWeeklyChange: number | null;        // BTC % change over 7 days
  stablecoinPeg: number | null;          // USDT price (should be ~$1.00)

  // Currency & Dollar
  dxy: number | null;                    // Dollar Index (spike = risk-off)
  usdJpy: number | null;                 // USD/JPY (yen carry trade unwind signal)

  // Banking Stress
  kre: number | null;                    // Regional Bank ETF price
  kreWeeklyChange: number | null;        // Regional Bank ETF weekly % change

  // Precious Metals
  goldSilverRatio: number | null;        // Gold/Silver ratio
  silverWeeklyChange: number | null;     // Silver weekly % change (momentum)

  // Market Breadth
  stocksAbove200MA: number | null;       // % of S&P 500 above 200-day MA

  // Fed & Liquidity (from FRED)
  fedBalanceSheet: number | null;        // Fed balance sheet in trillions
  fedBalanceSheetChange: number | null;  // Weekly change in Fed balance sheet

  lastUpdated: number;                   // Timestamp of last fetch
}

interface CrisisState {
  level: CrisisLevel;
  indicators: CrisisIndicators;
  triggeredIndicators: string[];         // Which indicators are in crisis
  pausedUntil: number | null;            // Trading paused until this timestamp
  lastLevelChange: number;               // When crisis level last changed
  positionsClosedInCrisis: string[];     // Symbols closed due to crisis
  manualOverride: boolean;               // User can manually enable/disable
}

interface AgentConfig {
  // Polling intervals - how often the agent checks for new data
  data_poll_interval_ms: number;   // [TUNE] Default: 30s. Lower = more API calls
  analyst_interval_ms: number;     // [TUNE] Default: 120s. How often to run trading logic

  // Position limits - risk management basics
  max_position_value: number;      // [TUNE] Max $ per position
  max_positions: number;           // [TUNE] Max concurrent positions
  min_sentiment_score: number;     // [TUNE] Min sentiment to consider buying (0-1)
  min_analyst_confidence: number;  // [TUNE] Min LLM confidence to execute (0-1)
  sell_sentiment_threshold: number; // [TUNE] Sentiment below this triggers sell review

  // Risk management - take profit and stop loss
  take_profit_pct: number;         // [TUNE] Take profit at this % gain
  stop_loss_pct: number;           // [TUNE] Stop loss at this % loss
  position_size_pct_of_cash: number; // [TUNE] % of cash per trade
  starting_equity: number;         // [TUNE] Starting equity for P&L calculation

  // Stale position management - exit positions that have lost momentum
  stale_position_enabled: boolean;
  stale_min_hold_hours: number;    // [TUNE] Min hours before checking staleness
  stale_max_hold_days: number;     // [TUNE] Force exit after this many days
  stale_min_gain_pct: number;      // [TUNE] Required gain % to hold past max days
  stale_mid_hold_days: number;
  stale_mid_min_gain_pct: number;
  stale_social_volume_decay: number; // [TUNE] Exit if volume drops to this % of entry
  stale_no_mentions_hours: number;   // [TUNE] Exit if no mentions for N hours

  // LLM configuration
  llm_provider: 'openai-raw' | 'ai-sdk' | 'cloudflare-gateway'; // [TUNE] Provider: openai-raw, ai-sdk, cloudflare-gateway
  llm_model: string;               // [TUNE] Model for quick research (gpt-4o-mini)
  llm_analyst_model: string;       // [TUNE] Model for deep analysis (gpt-4o)
  llm_max_tokens: number;
  llm_min_hold_minutes: number;    // [TUNE] Min minutes before LLM can recommend sell (default: 30)

  // Options trading - trade options instead of shares for high-conviction plays
  options_enabled: boolean;        // [TOGGLE] Enable/disable options trading
  options_min_confidence: number;  // [TUNE] Higher threshold for options (riskier)
  options_max_pct_per_trade: number;
  options_max_total_exposure: number;
  options_min_dte: number;         // [TUNE] Minimum days to expiration
  options_max_dte: number;         // [TUNE] Maximum days to expiration
  options_target_delta: number;    // [TUNE] Target delta (0.3-0.5 typical)
  options_min_delta: number;
  options_max_delta: number;
  options_stop_loss_pct: number;   // [TUNE] Options stop loss (wider than stocks)
  options_take_profit_pct: number; // [TUNE] Options take profit (higher targets)
  options_max_positions: number;

  // Crypto trading - 24/7 momentum-based crypto trading
  crypto_enabled: boolean;         // [TOGGLE] Enable/disable crypto trading
  crypto_symbols: string[];        // [TUNE] Which cryptos to trade (BTC/USD, etc.)
  crypto_momentum_threshold: number; // [TUNE] Min % move to trigger signal
  crypto_max_position_value: number;
  crypto_take_profit_pct: number;
  crypto_stop_loss_pct: number;

  // Custom ticker blacklist - user-defined symbols to never trade (e.g., insider trading restrictions)
  ticker_blacklist: string[];

  // Stock trading toggle - disable to trade crypto only (avoids PDT rules)
  stocks_enabled: boolean;          // [TOGGLE] Enable/disable stock trading

  // Allowed exchanges - only trade stocks listed on these exchanges (avoids OTC data issues)
  allowed_exchanges: string[];

  // DEX momentum trading - hunt for gems on Solana DEXs
  dex_enabled: boolean;             // [TOGGLE] Enable/disable DEX gem hunting
  dex_starting_balance_sol: number; // [TUNE] Starting paper trading balance in SOL

  // Multi-tier system with toggles
  // Micro-spray (30min-2h) - ultra-tiny bets to catch early movers [TOGGLE OFF by default]
  dex_microspray_enabled: boolean;       // [TOGGLE] Enable micro-spray tier
  dex_microspray_position_sol: number;   // [TUNE] Ultra-tiny position (default 0.005 SOL)
  dex_microspray_max_positions: number;  // [TUNE] Max concurrent micro-spray positions (default 10)
  // Breakout (2-6h) - detect rapid 5-min pumps [TOGGLE OFF by default]
  dex_breakout_enabled: boolean;         // [TOGGLE] Enable breakout tier
  dex_breakout_min_5m_pump: number;      // [TUNE] Minimum 5-min pump % to trigger (default 50)
  dex_breakout_position_sol: number;     // [TUNE] Position size (default 0.015 SOL)
  dex_breakout_max_positions: number;    // [TUNE] Max concurrent breakout positions (default 5)
  // Lottery (1-6h) - current working tier
  dex_lottery_enabled: boolean;          // [TOGGLE] Enable lottery tier
  dex_lottery_min_age_hours: number;     // [TUNE] Min age in hours (default 1)
  dex_lottery_max_age_hours: number;     // [TUNE] Max age in hours (default 6)
  dex_lottery_min_liquidity: number;     // [TUNE] Min liquidity (default $15k)
  dex_lottery_position_sol: number;      // [TUNE] Fixed tiny position size in SOL (default 0.02)
  dex_lottery_max_positions: number;     // [TUNE] Max concurrent lottery positions (default 5)
  dex_lottery_trailing_activation: number; // [TUNE] Auto-enable trailing stop at this gain % (default 100)
  // Tier 1: Early Gems (6h-3 days)
  dex_early_min_age_days: number;       // [TUNE] Tier 1: Min age (default 0.25 = 6 hours)
  dex_early_max_age_days: number;       // [TUNE] Tier 1: Max age (default 3 days)
  dex_early_min_liquidity: number;      // [TUNE] Tier 1: Min liquidity (default $30k)
  dex_early_min_legitimacy: number;     // [TUNE] Tier 1: Min legitimacy score 0-100 (default 40)
  dex_early_position_size_pct: number;  // [TUNE] Tier 1: Position size multiplier (default 50 = half normal size)
  // Tier 2: Established (3-14 days)
  dex_established_min_age_days: number; // [TUNE] Tier 2: Min age (default 3 days)
  dex_established_max_age_days: number; // [TUNE] Tier 2: Max age (default 14 days)
  dex_established_min_liquidity: number; // [TUNE] Tier 2: Min liquidity (default $50k)

  // Legacy age config (fallback if tier-specific not set)
  dex_min_age_days: number;         // [TUNE] Minimum token age in days (filter out brand new rugs)
  dex_max_age_days: number;         // [TUNE] Maximum token age in days (before CEX listing)
  dex_min_liquidity: number;        // [TUNE] Minimum liquidity in USD
  dex_min_volume_24h: number;       // [TUNE] Minimum 24h volume
  dex_min_price_change: number;     // [TUNE] Minimum 24h price change %
  dex_max_position_sol: number;     // [TUNE] Max SOL per position (capped)
  dex_position_size_pct: number;    // [TUNE] Position size as % of balance (0-100)
  dex_take_profit_pct: number;      // [TUNE] Take profit %
  dex_stop_loss_pct: number;        // [TUNE] Stop loss %
  dex_max_positions: number;        // [TUNE] Max concurrent DEX positions
  dex_slippage_model: 'none' | 'conservative' | 'realistic'; // [TUNE] Slippage simulation model
  dex_gas_fee_sol: number;          // [TUNE] Simulated gas fee per trade in SOL (default: 0.005)

  // Circuit breaker - pause trading after multiple stop losses
  dex_circuit_breaker_losses: number;       // [TUNE] Number of stop losses to trigger circuit breaker
  dex_circuit_breaker_window_hours: number; // [TUNE] Time window to count stop losses
  dex_circuit_breaker_pause_hours: number;  // [TUNE] How long to pause after circuit breaker triggers

  // Maximum drawdown protection
  dex_max_drawdown_pct: number;             // [TUNE] Max drawdown % before pausing trading

  // Position concentration limit
  dex_max_single_position_pct: number;      // [TUNE] Max % of total DEX portfolio in one token (default: 40)

  // Stop loss cooldown - prevent re-entry after stop loss (price-based primary, time-based fallback)
  dex_stop_loss_cooldown_hours: number;     // [TUNE] Fallback time cooldown if price data unavailable
  dex_reentry_recovery_pct: number;         // [TUNE] Allow re-entry when price is X% above exit price
  dex_reentry_min_momentum: number;         // [TUNE] OR allow re-entry when momentum score exceeds this
  dex_breaker_min_cooldown_minutes: number; // [TUNE] Minimum pause before circuit breaker can clear

  // Trailing stop loss - lock in gains by trailing the peak price
  dex_trailing_stop_enabled: boolean;       // [TOGGLE] Enable trailing stop loss for DEX positions
  dex_trailing_stop_activation_pct: number; // [TUNE] % gain required before trailing stop activates
  dex_trailing_stop_distance_pct: number;   // [TUNE] Distance from peak price for trailing stop

  // Chart pattern analysis - use Birdeye OHLCV data to avoid buying tops
  dex_chart_analysis_enabled: boolean;      // [TOGGLE] Enable chart pattern analysis before entry
  dex_chart_min_entry_score: number;        // [TUNE] Minimum entry score (0-100) to enter position

  // Crisis Mode - Black Swan Protection System
  crisis_mode_enabled: boolean;             // [TOGGLE] Enable crisis detection and auto-protection
  crisis_vix_elevated: number;              // [TUNE] VIX level for elevated risk (default: 25)
  crisis_vix_high: number;                  // [TUNE] VIX level for high alert (default: 35)
  crisis_vix_critical: number;              // [TUNE] VIX level for full crisis (default: 45)
  crisis_hy_spread_warning: number;         // [TUNE] High yield spread bps for warning (default: 400)
  crisis_hy_spread_critical: number;        // [TUNE] High yield spread bps for crisis (default: 600)
  crisis_btc_breakdown_price: number;       // [TUNE] BTC price that signals risk-off (default: 50000)
  crisis_btc_weekly_drop_pct: number;       // [TUNE] BTC weekly drop % for risk signal (default: -20)
  crisis_stocks_above_200ma_warning: number; // [TUNE] % stocks above 200MA for warning (default: 30)
  crisis_stocks_above_200ma_critical: number; // [TUNE] % stocks above 200MA for crisis (default: 20)
  crisis_stablecoin_depeg_threshold: number; // [TUNE] USDT price below this = crisis (default: 0.985)
  crisis_gold_silver_ratio_low: number;     // [TUNE] G/S ratio below this = monetary crisis (default: 60)
  crisis_check_interval_ms: number;         // [TUNE] How often to check crisis indicators (default: 300000 = 5min)
  crisis_level1_position_reduction: number; // [TUNE] Reduce position sizes by this % at level 1 (default: 50)
  crisis_level1_stop_loss_pct: number;      // [TUNE] Tighter stop loss at level 1 (default: 5)
  crisis_level2_min_profit_to_hold: number; // [TUNE] Min % profit to keep position at level 2 (default: 2)

  // New expanded crisis indicators
  crisis_yield_curve_inversion_warning: number; // [TUNE] Yield curve spread below this = warning (default: 0.25)
  crisis_yield_curve_inversion_critical: number; // [TUNE] Yield curve deeply inverted = critical (default: -0.5)
  crisis_ted_spread_warning: number;         // [TUNE] TED spread above this = banking stress warning (default: 0.5)
  crisis_ted_spread_critical: number;        // [TUNE] TED spread above this = banking crisis (default: 1.0)
  crisis_dxy_elevated: number;               // [TUNE] DXY above this = elevated dollar strength (default: 105)
  crisis_dxy_critical: number;               // [TUNE] DXY above this = flight to safety (default: 110)
  crisis_usdjpy_warning: number;             // [TUNE] USD/JPY below this = yen carry unwind warning (default: 140)
  crisis_usdjpy_critical: number;            // [TUNE] USD/JPY below this = yen carry unwind crisis (default: 130)
  crisis_kre_weekly_warning: number;         // [TUNE] KRE weekly drop % for warning (default: -10)
  crisis_kre_weekly_critical: number;        // [TUNE] KRE weekly drop % for crisis (default: -20)
  crisis_silver_weekly_warning: number;      // [TUNE] Silver weekly rise % for warning (default: 10)
  crisis_silver_weekly_critical: number;     // [TUNE] Silver weekly rise % for monetary crisis (default: 20)
  crisis_fed_balance_change_warning: number; // [TUNE] Fed balance sheet weekly % change for warning (default: 2)
  crisis_fed_balance_change_critical: number; // [TUNE] Fed balance sheet weekly % change for crisis (default: 5)
}

// [CUSTOMIZABLE] Add fields here when you add new data sources
interface Signal {
  symbol: string;
  source: string;           // e.g., "stocktwits", "reddit", "crypto", "your_source"
  source_detail: string;    // e.g., "reddit_wallstreetbets"
  sentiment: number;        // Weighted sentiment (-1 to 1)
  raw_sentiment: number;    // Raw sentiment before weighting
  volume: number;           // Number of mentions/messages
  freshness: number;        // Time decay factor (0-1)
  source_weight: number;    // How much to trust this source
  reason: string;           // Human-readable reason
  timestamp: number;        // Unix timestamp (ms) when signal was gathered
  upvotes?: number;
  comments?: number;
  quality_score?: number;
  subreddits?: string[];
  best_flair?: string | null;
  bullish?: number;
  bearish?: number;
  isCrypto?: boolean;
  momentum?: number;
  price?: number;
}

interface PositionEntry {
  symbol: string;
  entry_time: number;
  entry_price: number;
  entry_sentiment: number;
  entry_social_volume: number;
  entry_sources: string[];
  entry_reason: string;
  peak_price: number;
  peak_sentiment: number;
}

interface SocialHistoryEntry {
  timestamp: number;
  volume: number;
  sentiment: number;
}

interface LogEntry {
  timestamp: string;
  agent: string;
  action: string;
  [key: string]: unknown;
}

interface CostTracker {
  total_usd: number;
  calls: number;
  tokens_in: number;
  tokens_out: number;
}

interface ResearchResult {
  symbol: string;
  verdict: "BUY" | "SKIP" | "WAIT";
  confidence: number;
  entry_quality: "excellent" | "good" | "fair" | "poor";
  reasoning: string;
  red_flags: string[];
  catalysts: string[];
  timestamp: number;
}

interface TwitterConfirmation {
  symbol: string;
  tweet_count: number;
  sentiment: number;
  confirms_existing: boolean;
  highlights: Array<{ author: string; text: string; likes: number }>;
  timestamp: number;
}

interface PremarketPlan {
  timestamp: number;
  recommendations: Array<{
    action: "BUY" | "SELL" | "HOLD";
    symbol: string;
    confidence: number;
    reasoning: string;
    suggested_size_pct?: number;
  }>;
  market_summary: string;
  high_conviction: string[];
  researched_buys: ResearchResult[];
}

interface DexPosition {
  tokenAddress: string;
  symbol: string;
  entryPrice: number;
  entrySol: number;
  entryTime: number;
  tokenAmount: number;
  peakPrice: number;
  entryMomentumScore: number;  // Track entry momentum for decay detection (#12)
  entryLiquidity: number;      // Track entry liquidity for exit safety (#13)
  tier?: 'microspray' | 'breakout' | 'lottery' | 'early' | 'established';  // Track for tier-specific rules
  missedScans?: number;        // Track consecutive scans where token wasn't in signals (grace period for lost_momentum)
}

interface AgentState {
  config: AgentConfig;
  signalCache: Signal[];
  positionEntries: Record<string, PositionEntry>;
  socialHistory: Record<string, SocialHistoryEntry[]>;
  logs: LogEntry[];
  costTracker: CostTracker;
  lastDataGatherRun: number;
  lastAnalystRun: number;
  lastResearchRun: number;
  signalResearch: Record<string, ResearchResult>;
  positionResearch: Record<string, unknown>;
  stalenessAnalysis: Record<string, unknown>;
  twitterConfirmations: Record<string, TwitterConfirmation>;
  twitterDailyReads: number;
  twitterDailyReadReset: number;
  premarketPlan: PremarketPlan | null;
  enabled: boolean;
  // DEX momentum trading state
  dexSignals: DexMomentumSignal[];
  dexPositions: Record<string, DexPosition>;
  dexTradeHistory: DexTradeRecord[];
  dexRealizedPnL: number;
  dexPaperBalance: number; // Virtual SOL balance for paper trading
  dexPortfolioHistory: DexPortfolioSnapshot[]; // Track value over time for charts
  lastDexScanRun: number;
  // DEX streak and drawdown tracking (#15, #16, #17)
  dexMaxConsecutiveLosses: number;
  dexCurrentLossStreak: number;
  dexMaxDrawdownPct: number;
  dexMaxDrawdownDuration: number; // Duration in ms
  dexDrawdownStartTime: number | null; // When current drawdown started
  dexPeakBalance: number; // Peak balance for drawdown calculation
  // Circuit breaker state (#10)
  dexRecentStopLosses: Array<{timestamp: number, symbol: string}>;
  dexCircuitBreakerUntil: number | null;
  // Drawdown protection state (#11)
  dexPeakValue: number; // High water mark for drawdown calculation (total portfolio value)
  dexDrawdownPaused: boolean;
  // Stop loss cooldown tracking (#8) - price-based re-entry
  dexStopLossCooldowns: Record<string, { exitPrice: number; exitTime: number; fallbackExpiry: number }>;
  // Crisis Mode state
  crisisState: CrisisState;
  lastCrisisCheck: number;
}

interface DexPortfolioSnapshot {
  timestamp: number;
  totalValueSol: number; // Total value in SOL (balance + positions)
  paperBalanceSol: number;
  positionValueSol: number;
  realizedPnLSol: number;
}

interface DexTradeRecord {
  symbol: string;
  tokenAddress: string;
  entryPrice: number;
  exitPrice: number;
  entrySol: number;
  entryTime: number;
  exitTime: number;
  pnlPct: number;
  pnlSol: number;
  exitReason: "take_profit" | "stop_loss" | "lost_momentum" | "manual" | "trailing_stop";
}

// ============================================================================
// [CUSTOMIZABLE] SOURCE_CONFIG - How much to trust each data source
// ============================================================================
const SOURCE_CONFIG = {
  // [TUNE] Weight each source by reliability (0-1). Higher = more trusted.
  weights: {
    stocktwits: 0.85,           // Decent signal, some noise
    reddit_wallstreetbets: 0.6, // High volume, lots of memes - lower trust
    reddit_stocks: 0.9,         // Higher quality discussions
    reddit_investing: 0.8,      // Long-term focused
    reddit_options: 0.85,       // Options-specific alpha
    twitter_fintwit: 0.95,      // FinTwit has real traders
    twitter_news: 0.9,          // Breaking news accounts
  },
  // [TUNE] Reddit flair multipliers - boost/penalize based on post type
  flairMultipliers: {
    "DD": 1.5,                  // Due Diligence - high value
    "Technical Analysis": 1.3,
    "Fundamentals": 1.3,
    "News": 1.2,
    "Discussion": 1.0,
    "Chart": 1.1,
    "Daily Discussion": 0.7,   // Low signal
    "Weekend Discussion": 0.6,
    "YOLO": 0.6,               // Entertainment, not alpha
    "Gain": 0.5,               // Loss porn - inverse signal?
    "Loss": 0.5,
    "Meme": 0.4,
    "Shitpost": 0.3,
  } as Record<string, number>,
  // [TUNE] Engagement multipliers - more engagement = more trusted
  engagement: {
    upvotes: { 1000: 1.5, 500: 1.3, 200: 1.2, 100: 1.1, 50: 1.0, 0: 0.8 } as Record<number, number>,
    comments: { 200: 1.4, 100: 1.25, 50: 1.15, 20: 1.05, 0: 0.9 } as Record<number, number>,
  },
  // [TUNE] How fast old posts lose weight (minutes). Lower = faster decay.
  decayHalfLifeMinutes: 120,
};

const DEFAULT_CONFIG: AgentConfig = {
  data_poll_interval_ms: 30_000,
  analyst_interval_ms: 120_000,
  max_position_value: 5000,
  max_positions: 5,
  min_sentiment_score: 0.3,
  min_analyst_confidence: 0.6,
  sell_sentiment_threshold: -0.2,
  take_profit_pct: 10,
  stop_loss_pct: 5,
  position_size_pct_of_cash: 25,
  starting_equity: 100000, // Starting equity for P&L calculation
  stale_position_enabled: true,
  stale_min_hold_hours: 24,
  stale_max_hold_days: 3,
  stale_min_gain_pct: 5,
  stale_mid_hold_days: 2,
  stale_mid_min_gain_pct: 3,
  stale_social_volume_decay: 0.3,
  stale_no_mentions_hours: 24,
  llm_provider: "openai-raw",
  llm_model: "gpt-4o-mini",
  llm_analyst_model: "gpt-4o",
  llm_max_tokens: 500,
  llm_min_hold_minutes: 30,
  options_enabled: false,
  options_min_confidence: 0.8,
  options_max_pct_per_trade: 0.02,
  options_max_total_exposure: 0.10,
  options_min_dte: 30,
  options_max_dte: 60,
  options_target_delta: 0.45,
  options_min_delta: 0.30,
  options_max_delta: 0.70,
  options_stop_loss_pct: 50,
  options_take_profit_pct: 100,
  options_max_positions: 3,
  crypto_enabled: false,
  crypto_symbols: ["BTC/USD", "ETH/USD", "SOL/USD"],
  crypto_momentum_threshold: 2.0,
  crypto_max_position_value: 1000,
  crypto_take_profit_pct: 10,
  crypto_stop_loss_pct: 5,
  ticker_blacklist: [],
  stocks_enabled: true,
  allowed_exchanges: ["NYSE", "NASDAQ", "ARCA", "AMEX", "BATS"],
  // DEX momentum trading defaults
  dex_enabled: false,
  dex_starting_balance_sol: 1.0, // Start with 1 SOL for paper trading

  // Multi-tier system defaults
  // Micro-spray - ultra-tiny bets on very fresh coins [OFF by default]
  dex_microspray_enabled: false,      // Toggle OFF - enable when ready to test
  dex_microspray_position_sol: 0.005, // 0.005 SOL per position (~$0.50)
  dex_microspray_max_positions: 10,   // Spray up to 10 micro positions
  // Breakout - detect rapid 5-min pumps [OFF by default]
  dex_breakout_enabled: false,        // Toggle OFF - enable when ready to test
  dex_breakout_min_5m_pump: 50,       // Must be up 50%+ in 5 minutes
  dex_breakout_position_sol: 0.015,   // 0.015 SOL per position (~$1.50)
  dex_breakout_max_positions: 5,      // Max 5 breakout positions
  // Lottery - current working tier
  dex_lottery_enabled: true,
  dex_lottery_min_age_hours: 1,
  dex_lottery_max_age_hours: 6,
  dex_lottery_min_liquidity: 15000,
  dex_lottery_position_sol: 0.02,
  dex_lottery_max_positions: 5,
  dex_lottery_trailing_activation: 100,
  // Tier 1: Early Gems
  dex_early_min_age_days: 0.25,       // 6 hours minimum (after lottery window)
  dex_early_max_age_days: 3,          // Tier 1 ends at 3 days
  dex_early_min_liquidity: 30000,     // $30k minimum for early tier
  dex_early_min_legitimacy: 40,       // Must have website OR socials (40 points)
  dex_early_position_size_pct: 50,    // Use 50% of normal position size (higher risk)
  // Tier 2: Established
  dex_established_min_age_days: 3,    // Tier 2 starts at 3 days
  dex_established_max_age_days: 14,   // Tier 2 ends at 14 days
  dex_established_min_liquidity: 50000, // $50k minimum for established tier

  // Legacy config (still used as fallbacks)
  dex_min_age_days: 1,       // Skip very new tokens (<24h)
  dex_max_age_days: 90,      // Expanded range to catch more momentum
  dex_min_liquidity: 20000,  // Lower threshold for small caps
  dex_min_volume_24h: 5000,  // Lower volume threshold
  dex_min_price_change: 3,   // Lower price change threshold
  dex_max_position_sol: 1.0, // Max cap per position
  dex_position_size_pct: 33, // Use ~1/3 of balance per position (divides across max_positions)
  dex_take_profit_pct: 100,  // Take profit at 100% - let winners run
  dex_stop_loss_pct: 30,     // Stop loss at 30% - survive meme coin volatility
  dex_max_positions: 3,
  dex_slippage_model: 'realistic', // Simulate realistic DEX slippage
  dex_gas_fee_sol: 0.005, // ~$1 gas fee per trade at $200/SOL
  dex_circuit_breaker_losses: 3,      // Pause after 3 stop losses
  dex_circuit_breaker_window_hours: 24, // Within 24 hours
  dex_circuit_breaker_pause_hours: 1,  // Pause for 1 hour (was 6) - shorter cooldown
  dex_max_drawdown_pct: 35,           // Pause trading at 35% drawdown (was 25%)
  dex_max_single_position_pct: 40,    // Max 40% of portfolio in one token
  dex_stop_loss_cooldown_hours: 2,    // Fallback time cooldown (used if price check unavailable)
  dex_reentry_recovery_pct: 15,       // Re-enter when price is 15% above exit price
  dex_reentry_min_momentum: 70,       // OR re-enter when momentum score > 70
  dex_breaker_min_cooldown_minutes: 30, // Minimum 30 min pause before breaker can clear
  dex_trailing_stop_enabled: true,    // Enable trailing stop loss
  dex_trailing_stop_activation_pct: 50, // Trailing stop activates after 50% gain (let it run first)
  dex_trailing_stop_distance_pct: 25,  // Trailing stop is 25% below peak (room for pullbacks)
  // Chart pattern analysis defaults
  dex_chart_analysis_enabled: true,   // Enable Birdeye chart analysis before entry
  dex_chart_min_entry_score: 40,      // Minimum entry score (0-100) - avoid worst setups
  // Crisis Mode defaults
  crisis_mode_enabled: true,          // Crisis detection enabled by default
  crisis_vix_elevated: 25,            // VIX above 25 = elevated risk
  crisis_vix_high: 35,                // VIX above 35 = high alert
  crisis_vix_critical: 45,            // VIX above 45 = full crisis
  crisis_hy_spread_warning: 400,      // HY spread above 400bps = warning
  crisis_hy_spread_critical: 600,     // HY spread above 600bps = crisis
  crisis_btc_breakdown_price: 50000,  // BTC below $50k = risk-off signal
  crisis_btc_weekly_drop_pct: -20,    // BTC down 20%+ in a week = red flag
  crisis_stocks_above_200ma_warning: 30,  // Less than 30% above 200MA = warning
  crisis_stocks_above_200ma_critical: 20, // Less than 20% above 200MA = crisis
  crisis_stablecoin_depeg_threshold: 0.985, // USDT below $0.985 = crisis
  crisis_gold_silver_ratio_low: 60,   // G/S ratio below 60 = monetary crisis signal
  crisis_check_interval_ms: 300_000,  // Check every 5 minutes
  crisis_level1_position_reduction: 50, // Reduce position sizes by 50%
  crisis_level1_stop_loss_pct: 5,     // Tighter stop loss
  crisis_level2_min_profit_to_hold: 2, // Need 2% profit to hold at level 2

  // New expanded crisis indicators defaults
  crisis_yield_curve_inversion_warning: 0.25,  // Yield curve below 25bps = flattening warning
  crisis_yield_curve_inversion_critical: -0.5, // Yield curve below -50bps = recession warning
  crisis_ted_spread_warning: 0.5,         // TED spread above 50bps = banking stress
  crisis_ted_spread_critical: 1.0,        // TED spread above 100bps = banking crisis
  crisis_dxy_elevated: 105,               // Dollar index above 105 = risk-off mode
  crisis_dxy_critical: 110,               // Dollar index above 110 = flight to safety
  crisis_usdjpy_warning: 140,             // Yen strengthening below 140 = carry unwind starts
  crisis_usdjpy_critical: 130,            // Yen below 130 = carry trade blowing up
  crisis_kre_weekly_warning: -10,         // Regional banks down 10%/week = stress
  crisis_kre_weekly_critical: -20,        // Regional banks down 20%/week = crisis
  crisis_silver_weekly_warning: 10,       // Silver up 10%/week = monetary concerns
  crisis_silver_weekly_critical: 20,      // Silver up 20%/week = monetary crisis
  crisis_fed_balance_change_warning: 2,   // Fed balance sheet change 2%/week = intervention
  crisis_fed_balance_change_critical: 5,  // Fed balance sheet change 5%/week = emergency
};

const DEFAULT_STATE: AgentState = {
  config: DEFAULT_CONFIG,
  signalCache: [],
  positionEntries: {},
  socialHistory: {},
  logs: [],
  costTracker: { total_usd: 0, calls: 0, tokens_in: 0, tokens_out: 0 },
  lastDataGatherRun: 0,
  lastAnalystRun: 0,
  lastResearchRun: 0,
  signalResearch: {},
  positionResearch: {},
  stalenessAnalysis: {},
  twitterConfirmations: {},
  twitterDailyReads: 0,
  twitterDailyReadReset: 0,
  premarketPlan: null,
  enabled: false,
  // DEX state
  dexSignals: [],
  dexPositions: {},
  dexTradeHistory: [],
  dexRealizedPnL: 0,
  dexPaperBalance: 1.0, // Start with 1 SOL for paper trading
  dexPortfolioHistory: [],
  lastDexScanRun: 0,
  // DEX streak and drawdown tracking (#15, #16, #17)
  dexMaxConsecutiveLosses: 0,
  dexCurrentLossStreak: 0,
  dexMaxDrawdownPct: 0,
  dexMaxDrawdownDuration: 0,
  dexDrawdownStartTime: null,
  dexPeakBalance: 1.0,
  // Circuit breaker state (#10)
  dexRecentStopLosses: [],
  dexCircuitBreakerUntil: null,
  // Drawdown protection state (#11)
  dexPeakValue: 1.0, // Start with initial balance as peak
  dexDrawdownPaused: false,
  // Stop loss cooldown tracking (#8)
  dexStopLossCooldowns: {},
  // Crisis Mode state
  crisisState: {
    level: 0,
    indicators: {
      vix: null,
      highYieldSpread: null,
      yieldCurve2Y10Y: null,
      tedSpread: null,
      btcPrice: null,
      btcWeeklyChange: null,
      stablecoinPeg: null,
      dxy: null,
      usdJpy: null,
      kre: null,
      kreWeeklyChange: null,
      goldSilverRatio: null,
      silverWeeklyChange: null,
      stocksAbove200MA: null,
      fedBalanceSheet: null,
      fedBalanceSheetChange: null,
      lastUpdated: 0,
    },
    triggeredIndicators: [],
    pausedUntil: null,
    lastLevelChange: 0,
    positionsClosedInCrisis: [],
    manualOverride: false,
  },
  lastCrisisCheck: 0,
};

// Blacklist for ticker extraction - common English words and trading slang
const TICKER_BLACKLIST = new Set([
  // Finance/trading terms
  "CEO", "CFO", "COO", "CTO", "IPO", "EPS", "GDP", "SEC", "FDA", "USA", "USD", "ETF", "NYSE", "API",
  "ATH", "ATL", "IMO", "FOMO", "YOLO", "DD", "TA", "FA", "ROI", "PE", "PB", "PS", "EV", "DCF",
  "WSB", "RIP", "LOL", "OMG", "WTF", "FUD", "HODL", "APE", "MOASS", "DRS", "NFT", "DAO",
  // Common English words (2-4 letters that look like tickers)
  "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HER", "WAS", "ONE", "OUR",
  "OUT", "DAY", "HAD", "HAS", "HIS", "HOW", "ITS", "LET", "MAY", "NEW", "NOW", "OLD", "SEE",
  "WAY", "WHO", "BOY", "DID", "GET", "HIM", "HIT", "LOW", "MAN", "RUN", "SAY", "SHE", "TOO",
  "USE", "DAD", "MOM", "GOT", "HAS", "HAD", "LET", "PUT", "SAW", "SAT", "SET", "SIT", "TRY",
  "THAT", "THIS", "WITH", "HAVE", "FROM", "THEY", "BEEN", "CALL", "WILL", "EACH", "MAKE",
  "LIKE", "TIME", "JUST", "KNOW", "TAKE", "COME", "MADE", "FIND", "MORE", "LONG", "HERE",
  "MANY", "SOME", "THAN", "THEM", "THEN", "ONLY", "OVER", "SUCH", "YEAR", "INTO", "MOST",
  "ALSO", "BACK", "GOOD", "WELL", "EVEN", "WANT", "GIVE", "MUCH", "WORK", "FIRST", "AFTER",
  "AS", "AT", "BE", "BY", "DO", "GO", "IF", "IN", "IS", "IT", "MY", "NO", "OF", "ON", "OR",
  "SO", "TO", "UP", "US", "WE", "AN", "AM", "AH", "OH", "OK", "HI", "YA", "YO",
  // More trading slang
  "BULL", "BEAR", "CALL", "PUTS", "HOLD", "SELL", "MOON", "PUMP", "DUMP", "BAGS", "TEND",
  // Additional common words that appear as false positives
  "START", "ABOUT", "NAME", "NEXT", "PLAY", "LIVE", "GAME", "BEST", "LINK", "READ",
  "POST", "NEWS", "FREE", "LOOK", "HELP", "OPEN", "FULL", "VIEW", "REAL", "SEND",
  "HIGH", "DROP", "FAST", "SAFE", "RISK", "TURN", "PLAN", "DEAL", "MOVE", "HUGE",
  "EASY", "HARD", "LATE", "WAIT", "SOON", "STOP", "EXIT", "GAIN", "LOSS", "GROW",
  "FALL", "JUMP", "KEEP", "COPY", "EDIT", "SAVE", "NOTE", "TIPS", "IDEA", "PLUS",
  "ZERO", "SELF", "BOTH", "BETA", "TEST", "INFO", "DATA", "CASH", "WHAT", "WHEN",
  "WHERE", "WHY", "WATCH", "LOVE", "HATE", "TECH", "HOPE", "FEAR", "WEEK", "LAST",
  "PART", "SIDE", "STEP", "SURE", "TELL", "THINK", "TOLD", "TRUE", "TURN", "TYPE",
  "UNIT", "USED", "VERY", "WANT", "WENT", "WERE", "YEAH", "YOUR", "ELSE", "AWAY",
  "OTHER", "PRICE", "THEIR", "STILL", "CHEAP", "THESE", "LEAP", "EVERY", "SINCE",
  "BEING", "THOSE", "DOING", "COULD", "WOULD", "SHOULD", "MIGHT", "MUST", "SHALL",
]);

// ============================================================================
// SECTION: DEX TRADING METRICS CALCULATION (#15, #16, #17)
// ============================================================================

interface DexTradingMetrics {
  // Win rate and expectancy (#15)
  winRate: number;
  avgWinPct: number;
  avgLossPct: number;
  expectancy: number;
  profitFactor: number;
  // Sharpe ratio (#16)
  sharpeRatio: number;
  // Streak tracking (#17)
  maxConsecutiveLosses: number;
  currentLossStreak: number;
  maxDrawdownPct: number;
  maxDrawdownDuration: number;
  currentDrawdownPct: number;
}

function calculateDexTradingMetrics(
  tradeHistory: DexTradeRecord[],
  state: {
    dexMaxConsecutiveLosses: number;
    dexCurrentLossStreak: number;
    dexMaxDrawdownPct: number;
    dexMaxDrawdownDuration: number;
    dexDrawdownStartTime: number | null;
    dexPeakBalance: number;
    dexPaperBalance: number;
  }
): DexTradingMetrics {
  const defaultMetrics: DexTradingMetrics = {
    winRate: 0,
    avgWinPct: 0,
    avgLossPct: 0,
    expectancy: 0,
    profitFactor: 0,
    sharpeRatio: 0,
    maxConsecutiveLosses: state.dexMaxConsecutiveLosses || 0,
    currentLossStreak: state.dexCurrentLossStreak || 0,
    maxDrawdownPct: state.dexMaxDrawdownPct || 0,
    maxDrawdownDuration: state.dexMaxDrawdownDuration || 0,
    currentDrawdownPct: 0,
  };

  if (!tradeHistory || tradeHistory.length === 0) {
    return defaultMetrics;
  }

  // Separate winning and losing trades
  const winningTrades = tradeHistory.filter(t => t.pnlPct > 0);
  const losingTrades = tradeHistory.filter(t => t.pnlPct <= 0);

  const totalTrades = tradeHistory.length;
  const winningCount = winningTrades.length;

  // #15: Win rate calculation
  const winRate = totalTrades > 0 ? winningCount / totalTrades : 0;

  // #15: Average win/loss percentage
  const avgWinPct = winningTrades.length > 0
    ? winningTrades.reduce((sum, t) => sum + t.pnlPct, 0) / winningTrades.length
    : 0;

  const avgLossPct = losingTrades.length > 0
    ? losingTrades.reduce((sum, t) => sum + t.pnlPct, 0) / losingTrades.length
    : 0;

  // #15: Expectancy = (win_rate * avg_win) - ((1-win_rate) * abs(avg_loss))
  const expectancy = (winRate * avgWinPct) - ((1 - winRate) * Math.abs(avgLossPct));

  // #15: Profit factor = sum(winning pnl) / abs(sum(losing pnl))
  const totalWinSol = winningTrades.reduce((sum, t) => sum + t.pnlSol, 0);
  const totalLossSol = Math.abs(losingTrades.reduce((sum, t) => sum + t.pnlSol, 0));
  const profitFactor = totalLossSol > 0 ? totalWinSol / totalLossSol : (totalWinSol > 0 ? Infinity : 0);

  // #16: Sharpe ratio = mean(trade_returns) / std(trade_returns)
  const returns = tradeHistory.map(t => t.pnlPct);
  const meanReturn = returns.reduce((sum, r) => sum + r, 0) / returns.length;

  const squaredDiffs = returns.map(r => Math.pow(r - meanReturn, 2));
  const variance = squaredDiffs.reduce((sum, d) => sum + d, 0) / returns.length;
  const stdDev = Math.sqrt(variance);

  const sharpeRatio = stdDev > 0 ? meanReturn / stdDev : 0;

  // #17: Calculate current drawdown
  const peakBalance = state.dexPeakBalance || 1.0;
  const currentBalance = state.dexPaperBalance || 1.0;
  const currentDrawdownPct = peakBalance > 0
    ? ((peakBalance - currentBalance) / peakBalance) * 100
    : 0;

  return {
    winRate,
    avgWinPct,
    avgLossPct,
    expectancy,
    profitFactor: profitFactor === Infinity ? 999 : profitFactor, // Cap infinite profit factor for display
    sharpeRatio,
    maxConsecutiveLosses: state.dexMaxConsecutiveLosses || 0,
    currentLossStreak: state.dexCurrentLossStreak || 0,
    maxDrawdownPct: state.dexMaxDrawdownPct || 0,
    maxDrawdownDuration: state.dexMaxDrawdownDuration || 0,
    currentDrawdownPct: Math.max(0, currentDrawdownPct),
  };
}

// Helper to update streak and drawdown state after a trade
function updateStreakAndDrawdownState(
  isWin: boolean,
  currentBalance: number,
  state: {
    dexMaxConsecutiveLosses: number;
    dexCurrentLossStreak: number;
    dexMaxDrawdownPct: number;
    dexMaxDrawdownDuration: number;
    dexDrawdownStartTime: number | null;
    dexPeakBalance: number;
  }
): void {
  const now = Date.now();

  // Update loss streak tracking
  if (!isWin) {
    // Losing trade - increment streak
    state.dexCurrentLossStreak = (state.dexCurrentLossStreak || 0) + 1;
    if (state.dexCurrentLossStreak > (state.dexMaxConsecutiveLosses || 0)) {
      state.dexMaxConsecutiveLosses = state.dexCurrentLossStreak;
    }
  } else {
    // Winning trade - reset loss streak
    state.dexCurrentLossStreak = 0;
  }

  // Update peak balance and drawdown tracking
  if (currentBalance > (state.dexPeakBalance || 0)) {
    // New peak - update peak and clear drawdown start time
    state.dexPeakBalance = currentBalance;

    // If we were in a drawdown, record its duration
    if (state.dexDrawdownStartTime !== null) {
      const drawdownDuration = now - state.dexDrawdownStartTime;
      if (drawdownDuration > (state.dexMaxDrawdownDuration || 0)) {
        state.dexMaxDrawdownDuration = drawdownDuration;
      }
      state.dexDrawdownStartTime = null;
    }
  } else {
    // We're in a drawdown
    const drawdownPct = ((state.dexPeakBalance - currentBalance) / state.dexPeakBalance) * 100;

    if (drawdownPct > (state.dexMaxDrawdownPct || 0)) {
      state.dexMaxDrawdownPct = drawdownPct;
    }

    // Start tracking drawdown duration if not already
    if (state.dexDrawdownStartTime === null) {
      state.dexDrawdownStartTime = now;
    }
  }
}

class ValidTickerCache {
  private secTickers: Set<string> | null = null;
  private lastSecRefresh = 0;
  private alpacaCache: Map<string, boolean> = new Map();
  private readonly SEC_REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours

  async refreshSecTickersIfNeeded(): Promise<void> {
    if (this.secTickers && Date.now() - this.lastSecRefresh < this.SEC_REFRESH_INTERVAL_MS) {
      return;
    }
    try {
      const res = await fetch("https://www.sec.gov/files/company_tickers.json", {
        headers: { "User-Agent": "Mahoraga Trading Bot" },
      });
      if (!res.ok) return;
      const data = await res.json() as Record<string, { cik_str: number; ticker: string; title: string }>;
      this.secTickers = new Set(
        Object.values(data).map((e) => e.ticker.toUpperCase())
      );
      this.lastSecRefresh = Date.now();
    } catch {
      // Keep existing cache on failure
    }
  }

  isKnownSecTicker(symbol: string): boolean {
    return this.secTickers?.has(symbol.toUpperCase()) ?? false;
  }

  getCachedValidation(symbol: string): boolean | undefined {
    return this.alpacaCache.get(symbol.toUpperCase());
  }

  setCachedValidation(symbol: string, isValid: boolean): void {
    this.alpacaCache.set(symbol.toUpperCase(), isValid);
  }

  async validateWithAlpaca(
    symbol: string,
    alpaca: { trading: { getAsset(s: string): Promise<{ tradable: boolean } | null> } }
  ): Promise<boolean> {
    const upper = symbol.toUpperCase();
    const cached = this.alpacaCache.get(upper);
    if (cached !== undefined) return cached;

    try {
      const asset = await alpaca.trading.getAsset(upper);
      const isValid = asset !== null && asset.tradable;
      this.alpacaCache.set(upper, isValid);
      return isValid;
    } catch {
      this.alpacaCache.set(upper, false);
      return false;
    }
  }
}

const tickerCache = new ValidTickerCache();

// ============================================================================
// SOL Price Cache - Fetch real SOL/USD price with 5-minute cache
// ============================================================================
interface SolPriceCache {
  price: number;
  timestamp: number;
}

let solPriceCache: SolPriceCache | null = null;
const SOL_PRICE_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const SOL_PRICE_FALLBACK = 200; // Fallback if API fails

async function getSolPriceUsd(): Promise<number> {
  const now = Date.now();

  // Return cached price if still valid
  if (solPriceCache && now - solPriceCache.timestamp < SOL_PRICE_CACHE_TTL_MS) {
    return solPriceCache.price;
  }

  try {
    // Use DexScreener API to get SOL price (SOL/USDC pair on Raydium)
    const res = await fetch(
      "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112",
      { headers: { "User-Agent": "MahoragaBot/1.0" } }
    );

    if (!res.ok) {
      throw new Error(`DexScreener API returned ${res.status}`);
    }

    const data = (await res.json()) as {
      pairs?: Array<{ priceUsd?: string; liquidity?: { usd?: number } }>;
    };

    // Find the highest liquidity SOL pair for best price accuracy
    const pairs = data.pairs || [];
    const solPair = pairs
      .filter((p) => p.priceUsd && p.liquidity?.usd && p.liquidity.usd > 100000)
      .sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];

    if (solPair?.priceUsd) {
      const price = parseFloat(solPair.priceUsd);
      if (!isNaN(price) && price > 0) {
        solPriceCache = { price, timestamp: now };
        console.log(`[SolPrice] Fetched real SOL price: $${price.toFixed(2)}`);
        return price;
      }
    }

    throw new Error("No valid SOL price found in DexScreener response");
  } catch (error) {
    console.error(
      `[SolPrice] Failed to fetch SOL price: ${error}. Using fallback: $${SOL_PRICE_FALLBACK}`
    );
    // Return cached price if available (even if stale), otherwise fallback
    return solPriceCache?.price || SOL_PRICE_FALLBACK;
  }
}

// ============================================================================
// SECTION 2: HELPER FUNCTIONS
// ============================================================================
// [CUSTOMIZABLE] These utilities calculate sentiment weights and extract tickers.
// Modify these to change how posts are scored and filtered.
// ============================================================================

function normalizeCryptoSymbol(symbol: string): string {
  if (symbol.includes("/")) {
    return symbol.toUpperCase();
  }
  const match = symbol.toUpperCase().match(/^([A-Z]{2,5})(USD|USDT|USDC)$/);
  if (match) {
    return `${match[1]}/${match[2]}`;
  }
  return symbol;
}

function isCryptoSymbol(symbol: string, cryptoSymbols: string[]): boolean {
  const normalizedInput = normalizeCryptoSymbol(symbol);
  for (const configSymbol of cryptoSymbols) {
    if (normalizeCryptoSymbol(configSymbol) === normalizedInput) {
      return true;
    }
  }
  return /^[A-Z]{2,5}\/(USD|USDT|USDC)$/.test(normalizedInput);
}

/**
 * [TUNE] Time decay - how quickly old posts lose weight
 * Uses exponential decay with half-life from SOURCE_CONFIG.decayHalfLifeMinutes
 * Modify the min/max clamp values (0.2-1.0) to change bounds
 */
function calculateTimeDecay(postTimestamp: number): number {
  const ageMinutes = (Date.now() - postTimestamp * 1000) / 60000;
  const halfLife = SOURCE_CONFIG.decayHalfLifeMinutes;
  const decay = Math.pow(0.5, ageMinutes / halfLife);
  return Math.max(0.2, Math.min(1.0, decay));
}

function getEngagementMultiplier(upvotes: number, comments: number): number {
  let upvoteMultiplier = 0.8;
  const upvoteThresholds = Object.entries(SOURCE_CONFIG.engagement.upvotes)
    .sort(([a], [b]) => Number(b) - Number(a));
  for (const [threshold, mult] of upvoteThresholds) {
    if (upvotes >= parseInt(threshold)) {
      upvoteMultiplier = mult;
      break;
    }
  }

  let commentMultiplier = 0.9;
  const commentThresholds = Object.entries(SOURCE_CONFIG.engagement.comments)
    .sort(([a], [b]) => Number(b) - Number(a));
  for (const [threshold, mult] of commentThresholds) {
    if (comments >= parseInt(threshold)) {
      commentMultiplier = mult;
      break;
    }
  }

  return (upvoteMultiplier + commentMultiplier) / 2;
}

/** [TUNE] Flair multiplier - boost/penalize based on Reddit post flair */
function getFlairMultiplier(flair: string | null | undefined): number {
  if (!flair) return 1.0;
  return SOURCE_CONFIG.flairMultipliers[flair.trim()] || 1.0;
}

/**
 * [CUSTOMIZABLE] Ticker extraction - modify regex to change what counts as a ticker
 * Current: $SYMBOL or SYMBOL followed by trading keywords
 * Add patterns for your data sources (e.g., cashtags, mentions)
 */
function extractTickers(text: string, customBlacklist: string[] = []): string[] {
  const matches = new Set<string>();
  const customSet = new Set(customBlacklist.map(t => t.toUpperCase()));
  const regex = /\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b(?=\s+(?:calls?|puts?|stock|shares?|moon|rocket|yolo|buy|sell|long|short))/gi;
  let match;
  while ((match = regex.exec(text)) !== null) {
    const ticker = (match[1] || match[2] || "").toUpperCase();
    if (ticker.length >= 2 && ticker.length <= 5 && !TICKER_BLACKLIST.has(ticker) && !customSet.has(ticker)) {
      matches.add(ticker);
    }
  }
  return Array.from(matches);
}

/**
 * [CUSTOMIZABLE] Sentiment detection - keyword-based bullish/bearish scoring
 * Add/remove words to match your trading style
 * Returns -1 (bearish) to +1 (bullish)
 */
function detectSentiment(text: string): number {
  const lower = text.toLowerCase();
  const bullish = ["moon", "rocket", "buy", "calls", "long", "bullish", "yolo", "tendies", "gains", "diamond", "squeeze", "pump", "green", "up", "breakout", "undervalued", "accumulate"];
  const bearish = ["puts", "short", "sell", "bearish", "crash", "dump", "drill", "tank", "rip", "red", "down", "bag", "overvalued", "bubble", "avoid"];

  let bull = 0, bear = 0;
  for (const w of bullish) if (lower.includes(w)) bull++;
  for (const w of bearish) if (lower.includes(w)) bear++;

  const total = bull + bear;
  if (total === 0) return 0;
  return (bull - bear) / total;
}

/**
 * Calculate DEX slippage based on liquidity and position size
 *
 * Real DEX trades have 1-5%+ slippage due to:
 * - AMM price impact (larger trades move price more)
 * - Low liquidity pools have higher slippage
 * - MEV/frontrunning can add extra slippage
 *
 * Formula: slippage_pct = base_slippage + (position_usd / liquidity_usd) * multiplier
 *
 * @param model - Slippage model: 'none', 'conservative', 'realistic'
 * @param positionUsd - Position size in USD
 * @param liquidityUsd - Pool liquidity in USD
 * @returns Slippage as a decimal (e.g., 0.02 = 2%)
 */
function calculateDexSlippage(
  model: "none" | "conservative" | "realistic",
  positionUsd: number,
  liquidityUsd: number
): number {
  if (model === "none") return 0;

  // Prevent division by zero
  if (liquidityUsd <= 0) liquidityUsd = 1;

  // Model parameters
  const params = {
    conservative: { baseSlippage: 0.005, multiplier: 2 }, // 0.5% base + 2x impact
    realistic: { baseSlippage: 0.01, multiplier: 5 }, // 1% base + 5x impact
  };

  const { baseSlippage, multiplier } = params[model];

  // Calculate price impact: larger position relative to liquidity = more slippage
  const priceImpact = (positionUsd / liquidityUsd) * multiplier;

  // Total slippage = base + impact, capped at reasonable max (15%)
  const totalSlippage = Math.min(baseSlippage + priceImpact, 0.15);

  return totalSlippage;
}

// ============================================================================
// SECTION 2.5: CRISIS MODE - BLACK SWAN PROTECTION SYSTEM
// ============================================================================
// Monitor market stress indicators and auto-protect portfolio during crises.
// Runs alongside normal trading so you can profit during calm periods while
// being ready to protect capital when the black swans arrive.
// ============================================================================

/**
 * Fetch VIX (CBOE Volatility Index) from Yahoo Finance
 * VIX > 25 = elevated fear, > 35 = high fear, > 45 = panic
 */
async function fetchVIX(): Promise<number | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=1d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };
    return data.chart?.result?.[0]?.meta?.regularMarketPrice ?? null;
  } catch {
    return null;
  }
}

/**
 * Fetch BTC price and calculate weekly change
 * BTC is a risk indicator (NOT a safe haven) - breakdown signals risk-off cascade
 */
async function fetchBTCData(): Promise<{ price: number; weeklyChange: number } | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1d&range=7d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as {
      chart?: { result?: Array<{
        meta?: { regularMarketPrice?: number };
        indicators?: { quote?: Array<{ close?: number[] }> }
      }> }
    };
    const result = data.chart?.result?.[0];
    const currentPrice = result?.meta?.regularMarketPrice;
    const closes = result?.indicators?.quote?.[0]?.close;
    if (!currentPrice || !closes || closes.length < 2) return null;

    // Get first valid close from 7 days ago
    const weekAgoPrice = closes.find(c => c != null && c > 0) ?? currentPrice;
    const weeklyChange = ((currentPrice - weekAgoPrice) / weekAgoPrice) * 100;

    return { price: currentPrice, weeklyChange };
  } catch {
    return null;
  }
}

/**
 * Fetch USDT stablecoin price - depeg below $0.985 signals banking/crypto crisis
 */
async function fetchStablecoinPeg(): Promise<number | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/USDT-USD?interval=1d&range=1d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };
    return data.chart?.result?.[0]?.meta?.regularMarketPrice ?? null;
  } catch {
    return null;
  }
}

/**
 * Fetch Gold/Silver ratio - collapse below 60 signals monetary system stress
 * Normal: 70-80, Crisis: <60 (silver outperforming gold = safe haven rotation)
 */
async function fetchGoldSilverRatio(): Promise<number | null> {
  try {
    // Fetch gold and silver prices concurrently
    const [goldResp, silverResp] = await Promise.all([
      fetch("https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=1d", {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
      }),
      fetch("https://query1.finance.yahoo.com/v8/finance/chart/SI=F?interval=1d&range=1d", {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
      })
    ]);

    if (!goldResp.ok || !silverResp.ok) return null;

    const goldData = await goldResp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };
    const silverData = await silverResp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };

    const goldPrice = goldData.chart?.result?.[0]?.meta?.regularMarketPrice;
    const silverPrice = silverData.chart?.result?.[0]?.meta?.regularMarketPrice;

    if (!goldPrice || !silverPrice || silverPrice === 0) return null;

    return goldPrice / silverPrice;
  } catch {
    return null;
  }
}

/**
 * Fetch High Yield Bond Spread (HYG vs Treasury proxy)
 * Spread > 400bps = credit stress, > 600bps = credit crisis
 */
async function fetchHighYieldSpread(): Promise<number | null> {
  try {
    // HYG (high yield corporate bonds) vs TLT (treasury) as a spread proxy
    // This is a simplified proxy - real HY spread requires more complex calculation
    const [hygResp, tltResp] = await Promise.all([
      fetch("https://query1.finance.yahoo.com/v8/finance/chart/HYG?interval=1d&range=5d", {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
      }),
      fetch("https://query1.finance.yahoo.com/v8/finance/chart/TLT?interval=1d&range=5d", {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
      })
    ]);

    if (!hygResp.ok || !tltResp.ok) return null;

    const hygData = await hygResp.json() as {
      chart?: { result?: Array<{ indicators?: { quote?: Array<{ close?: number[] }> } }> }
    };
    const tltData = await tltResp.json() as {
      chart?: { result?: Array<{ indicators?: { quote?: Array<{ close?: number[] }> } }> }
    };

    const hygCloses = hygData.chart?.result?.[0]?.indicators?.quote?.[0]?.close;
    const tltCloses = tltData.chart?.result?.[0]?.indicators?.quote?.[0]?.close;

    if (!hygCloses?.length || !tltCloses?.length) return null;

    // Calculate 5-day performance difference as spread proxy
    // When HYG underperforms TLT significantly, credit spreads are widening
    const hygFirst = hygCloses.find(c => c != null) ?? 0;
    const hygLast = hygCloses[hygCloses.length - 1] ?? hygFirst;
    const tltFirst = tltCloses.find(c => c != null) ?? 0;
    const tltLast = tltCloses[tltCloses.length - 1] ?? tltFirst;

    if (hygFirst === 0 || tltFirst === 0) return null;

    const hygChange = ((hygLast - hygFirst) / hygFirst) * 100;
    const tltChange = ((tltLast - tltFirst) / tltFirst) * 100;

    // Convert relative underperformance to approximate basis points
    // If HYG drops 2% more than TLT, that's roughly 200bps spread widening
    const spreadProxy = (tltChange - hygChange) * 100;

    // Return estimated spread (baseline 300bps + proxy adjustment)
    return Math.max(200, 300 + spreadProxy);
  } catch {
    return null;
  }
}

/**
 * Fetch 2Y/10Y Treasury Yield Spread from FRED
 * Negative = inverted yield curve = recession signal
 */
async function fetchYieldCurve(fredApiKey: string): Promise<number | null> {
  try {
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=T10Y2Y&api_key=${fredApiKey}&file_type=json&limit=1&sort_order=desc`;
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json() as { observations?: Array<{ value: string }> };
    const value = data.observations?.[0]?.value;
    if (!value || value === ".") return null;
    return parseFloat(value);
  } catch {
    return null;
  }
}

/**
 * Fetch TED Spread from FRED (LIBOR - T-bill)
 * Higher = more banking stress. > 1.0 = elevated, > 2.0 = crisis
 */
async function fetchTedSpread(fredApiKey: string): Promise<number | null> {
  try {
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=TEDRATE&api_key=${fredApiKey}&file_type=json&limit=1&sort_order=desc`;
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json() as { observations?: Array<{ value: string }> };
    const value = data.observations?.[0]?.value;
    if (!value || value === ".") return null;
    return parseFloat(value);
  } catch {
    return null;
  }
}

/**
 * Fetch Fed Balance Sheet from FRED (WALCL - total assets)
 * Returns value in trillions. Decreasing = QT (tightening)
 */
async function fetchFedBalanceSheet(fredApiKey: string): Promise<{ value: number; weeklyChange: number } | null> {
  try {
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=WALCL&api_key=${fredApiKey}&file_type=json&limit=5&sort_order=desc`;
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json() as { observations?: Array<{ value: string }> };
    const obs = data.observations;
    if (!obs || obs.length < 2) return null;

    const latestObs = obs[0];
    const weekAgoObs = obs[obs.length - 1];
    if (!latestObs || !weekAgoObs) return null;
    const latestValue = parseFloat(latestObs.value);
    const weekAgoValue = parseFloat(weekAgoObs.value);
    if (isNaN(latestValue) || isNaN(weekAgoValue)) return null;

    // WALCL is in millions, convert to trillions
    const valueTrillions = latestValue / 1_000_000;
    const weeklyChange = ((latestValue - weekAgoValue) / weekAgoValue) * 100;

    return { value: valueTrillions, weeklyChange };
  } catch {
    return null;
  }
}

/**
 * Fetch DXY (Dollar Index) from Yahoo Finance
 * Spike in DXY often signals risk-off / flight to safety
 */
async function fetchDXY(): Promise<number | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=1d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };
    return data.chart?.result?.[0]?.meta?.regularMarketPrice ?? null;
  } catch {
    return null;
  }
}

/**
 * Fetch USD/JPY from Yahoo Finance
 * Sharp drop = yen strengthening = carry trade unwind = risk-off
 */
async function fetchUsdJpy(): Promise<number | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY=X?interval=1d&range=1d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as { chart?: { result?: Array<{ meta?: { regularMarketPrice?: number } }> } };
    return data.chart?.result?.[0]?.meta?.regularMarketPrice ?? null;
  } catch {
    return null;
  }
}

/**
 * Fetch KRE (Regional Bank ETF) price and weekly change
 * Regional banks often lead broader financial stress
 */
async function fetchKRE(): Promise<{ price: number; weeklyChange: number } | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/KRE?interval=1d&range=7d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as {
      chart?: { result?: Array<{
        meta?: { regularMarketPrice?: number };
        indicators?: { quote?: Array<{ close?: number[] }> }
      }> }
    };
    const result = data.chart?.result?.[0];
    const currentPrice = result?.meta?.regularMarketPrice;
    const closes = result?.indicators?.quote?.[0]?.close;
    if (!currentPrice || !closes || closes.length < 2) return null;

    const weekAgoPrice = closes.find(c => c != null && c > 0) ?? currentPrice;
    const weeklyChange = ((currentPrice - weekAgoPrice) / weekAgoPrice) * 100;

    return { price: currentPrice, weeklyChange };
  } catch {
    return null;
  }
}

/**
 * Fetch Silver weekly momentum
 * Strong silver momentum can signal monetary crisis expectations
 */
async function fetchSilverMomentum(): Promise<number | null> {
  try {
    const url = "https://query1.finance.yahoo.com/v8/finance/chart/SI=F?interval=1d&range=7d";
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
    });
    if (!resp.ok) return null;
    const data = await resp.json() as {
      chart?: { result?: Array<{
        meta?: { regularMarketPrice?: number };
        indicators?: { quote?: Array<{ close?: number[] }> }
      }> }
    };
    const result = data.chart?.result?.[0];
    const currentPrice = result?.meta?.regularMarketPrice;
    const closes = result?.indicators?.quote?.[0]?.close;
    if (!currentPrice || !closes || closes.length < 2) return null;

    const weekAgoPrice = closes.find(c => c != null && c > 0) ?? currentPrice;
    const weeklyChange = ((currentPrice - weekAgoPrice) / weekAgoPrice) * 100;

    return weeklyChange;
  } catch {
    return null;
  }
}

/**
 * Fetch all crisis indicators concurrently
 */
async function fetchCrisisIndicators(fredApiKey?: string): Promise<CrisisIndicators> {
  // Fetch all indicators in parallel for speed
  const [
    vix,
    btcData,
    stablecoinPeg,
    goldSilverRatio,
    highYieldSpread,
    yieldCurve,
    tedSpread,
    fedBalance,
    dxy,
    usdJpy,
    kreData,
    silverMomentum,
  ] = await Promise.all([
    fetchVIX(),
    fetchBTCData(),
    fetchStablecoinPeg(),
    fetchGoldSilverRatio(),
    fetchHighYieldSpread(),
    fredApiKey ? fetchYieldCurve(fredApiKey) : Promise.resolve(null),
    fredApiKey ? fetchTedSpread(fredApiKey) : Promise.resolve(null),
    fredApiKey ? fetchFedBalanceSheet(fredApiKey) : Promise.resolve(null),
    fetchDXY(),
    fetchUsdJpy(),
    fetchKRE(),
    fetchSilverMomentum(),
  ]);

  return {
    // Volatility
    vix,

    // Credit Markets
    highYieldSpread,
    yieldCurve2Y10Y: yieldCurve,
    tedSpread,

    // Crypto
    btcPrice: btcData?.price ?? null,
    btcWeeklyChange: btcData?.weeklyChange ?? null,
    stablecoinPeg,

    // Currency
    dxy,
    usdJpy,

    // Banking
    kre: kreData?.price ?? null,
    kreWeeklyChange: kreData?.weeklyChange ?? null,

    // Precious Metals
    goldSilverRatio,
    silverWeeklyChange: silverMomentum,

    // Market Breadth
    stocksAbove200MA: null, // TODO: Requires market breadth data source

    // Fed & Liquidity
    fedBalanceSheet: fedBalance?.value ?? null,
    fedBalanceSheetChange: fedBalance?.weeklyChange ?? null,

    lastUpdated: Date.now(),
  };
}

/**
 * Evaluate crisis indicators and determine crisis level
 * Returns: { level: 0-3, triggeredIndicators: string[] }
 */
function evaluateCrisisLevel(
  indicators: CrisisIndicators,
  config: AgentConfig
): { level: CrisisLevel; triggeredIndicators: string[] } {
  const triggered: string[] = [];
  let score = 0;

  // VIX evaluation (max 3 points)
  if (indicators.vix !== null) {
    if (indicators.vix >= config.crisis_vix_critical) {
      triggered.push(`VIX CRITICAL: ${indicators.vix.toFixed(1)} (>=${config.crisis_vix_critical})`);
      score += 3;
    } else if (indicators.vix >= config.crisis_vix_high) {
      triggered.push(`VIX HIGH: ${indicators.vix.toFixed(1)} (>=${config.crisis_vix_high})`);
      score += 2;
    } else if (indicators.vix >= config.crisis_vix_elevated) {
      triggered.push(`VIX elevated: ${indicators.vix.toFixed(1)} (>=${config.crisis_vix_elevated})`);
      score += 1;
    }
  }

  // High Yield Spread (max 2 points)
  if (indicators.highYieldSpread !== null) {
    if (indicators.highYieldSpread >= config.crisis_hy_spread_critical) {
      triggered.push(`HY Spread CRITICAL: ${indicators.highYieldSpread.toFixed(0)}bps (>=${config.crisis_hy_spread_critical})`);
      score += 2;
    } else if (indicators.highYieldSpread >= config.crisis_hy_spread_warning) {
      triggered.push(`HY Spread warning: ${indicators.highYieldSpread.toFixed(0)}bps (>=${config.crisis_hy_spread_warning})`);
      score += 1;
    }
  }

  // BTC weekly drop (max 2 points) - risk indicator, not safe haven
  // Using % change rather than absolute price - more meaningful signal
  if (indicators.btcWeeklyChange !== null) {
    if (indicators.btcWeeklyChange <= config.crisis_btc_weekly_drop_pct) {
      triggered.push(`BTC weekly crash: ${indicators.btcWeeklyChange.toFixed(1)}% (<=${config.crisis_btc_weekly_drop_pct}%)`);
      score += 2; // Full 2 points for significant weekly drop
    } else if (indicators.btcWeeklyChange <= -10) {
      // Moderate drop (-10% to -20%) - warning signal
      triggered.push(`BTC weekly decline: ${indicators.btcWeeklyChange.toFixed(1)}%`);
      score += 1;
    }
  }

  // Stablecoin depeg (2 points) - banking/crypto crisis
  if (indicators.stablecoinPeg !== null && indicators.stablecoinPeg < config.crisis_stablecoin_depeg_threshold) {
    triggered.push(`USDT DEPEG: $${indicators.stablecoinPeg.toFixed(4)} (<${config.crisis_stablecoin_depeg_threshold})`);
    score += 2;
  }

  // Gold/Silver ratio collapse (2 points) - monetary crisis
  if (indicators.goldSilverRatio !== null && indicators.goldSilverRatio < config.crisis_gold_silver_ratio_low) {
    triggered.push(`G/S ratio collapse: ${indicators.goldSilverRatio.toFixed(1)} (<${config.crisis_gold_silver_ratio_low})`);
    score += 2;
  }

  // Stocks below 200MA (if available)
  if (indicators.stocksAbove200MA !== null) {
    if (indicators.stocksAbove200MA < config.crisis_stocks_above_200ma_critical) {
      triggered.push(`Market breakdown: only ${indicators.stocksAbove200MA.toFixed(0)}% above 200MA`);
      score += 2;
    } else if (indicators.stocksAbove200MA < config.crisis_stocks_above_200ma_warning) {
      triggered.push(`Market weakness: ${indicators.stocksAbove200MA.toFixed(0)}% above 200MA`);
      score += 1;
    }
  }

  // Yield Curve Inversion (max 2 points) - recession signal
  // Negative spread means short-term rates > long-term rates = inverted
  if (indicators.yieldCurve2Y10Y !== null) {
    if (indicators.yieldCurve2Y10Y <= config.crisis_yield_curve_inversion_critical) {
      triggered.push(`YIELD CURVE DEEPLY INVERTED: ${(indicators.yieldCurve2Y10Y * 100).toFixed(0)}bps (<=${(config.crisis_yield_curve_inversion_critical * 100).toFixed(0)}bps)`);
      score += 2;
    } else if (indicators.yieldCurve2Y10Y <= config.crisis_yield_curve_inversion_warning) {
      triggered.push(`Yield curve flat/inverting: ${(indicators.yieldCurve2Y10Y * 100).toFixed(0)}bps`);
      score += 1;
    }
  }

  // TED Spread (max 2 points) - banking stress indicator
  // LIBOR - T-bill spread; high = banks don't trust each other
  if (indicators.tedSpread !== null) {
    if (indicators.tedSpread >= config.crisis_ted_spread_critical) {
      triggered.push(`TED SPREAD CRISIS: ${indicators.tedSpread.toFixed(2)}% (>=${config.crisis_ted_spread_critical}%)`);
      score += 2;
    } else if (indicators.tedSpread >= config.crisis_ted_spread_warning) {
      triggered.push(`TED spread elevated: ${indicators.tedSpread.toFixed(2)}%`);
      score += 1;
    }
  }

  // DXY Dollar Index (max 2 points) - flight to safety
  // High DXY = risk-off, everyone fleeing to USD
  if (indicators.dxy !== null) {
    if (indicators.dxy >= config.crisis_dxy_critical) {
      triggered.push(`DXY FLIGHT TO SAFETY: ${indicators.dxy.toFixed(1)} (>=${config.crisis_dxy_critical})`);
      score += 2;
    } else if (indicators.dxy >= config.crisis_dxy_elevated) {
      triggered.push(`DXY elevated: ${indicators.dxy.toFixed(1)} (>=${config.crisis_dxy_elevated})`);
      score += 1;
    }
  }

  // USD/JPY (max 2 points) - yen carry trade unwind
  // Low USD/JPY = yen strengthening = carry trade unwinding = global deleveraging
  if (indicators.usdJpy !== null) {
    if (indicators.usdJpy <= config.crisis_usdjpy_critical) {
      triggered.push(`YEN CARRY UNWIND CRISIS: USD/JPY ${indicators.usdJpy.toFixed(1)} (<=${config.crisis_usdjpy_critical})`);
      score += 2;
    } else if (indicators.usdJpy <= config.crisis_usdjpy_warning) {
      triggered.push(`Yen carry unwind warning: USD/JPY ${indicators.usdJpy.toFixed(1)}`);
      score += 1;
    }
  }

  // KRE Regional Banks (max 2 points) - banking sector stress
  if (indicators.kreWeeklyChange !== null) {
    if (indicators.kreWeeklyChange <= config.crisis_kre_weekly_critical) {
      triggered.push(`REGIONAL BANK CRISIS: KRE ${indicators.kreWeeklyChange.toFixed(1)}%/week (<=${config.crisis_kre_weekly_critical}%)`);
      score += 2;
    } else if (indicators.kreWeeklyChange <= config.crisis_kre_weekly_warning) {
      triggered.push(`Regional bank stress: KRE ${indicators.kreWeeklyChange.toFixed(1)}%/week`);
      score += 1;
    }
  }

  // Silver Momentum (max 2 points) - monetary crisis indicator
  // Rapid silver rise = people fleeing to hard assets, monetary system distrust
  if (indicators.silverWeeklyChange !== null) {
    if (indicators.silverWeeklyChange >= config.crisis_silver_weekly_critical) {
      triggered.push(`SILVER SURGE - MONETARY CRISIS: +${indicators.silverWeeklyChange.toFixed(1)}%/week (>=${config.crisis_silver_weekly_critical}%)`);
      score += 2;
    } else if (indicators.silverWeeklyChange >= config.crisis_silver_weekly_warning) {
      triggered.push(`Silver momentum elevated: +${indicators.silverWeeklyChange.toFixed(1)}%/week`);
      score += 1;
    }
  }

  // Fed Balance Sheet Changes (max 2 points) - emergency intervention
  // Rapid changes = Fed intervening in markets = something is breaking
  if (indicators.fedBalanceSheetChange !== null) {
    const absChange = Math.abs(indicators.fedBalanceSheetChange);
    if (absChange >= config.crisis_fed_balance_change_critical) {
      const direction = indicators.fedBalanceSheetChange > 0 ? "expansion" : "contraction";
      triggered.push(`FED EMERGENCY ${direction.toUpperCase()}: ${indicators.fedBalanceSheetChange.toFixed(1)}%/week`);
      score += 2;
    } else if (absChange >= config.crisis_fed_balance_change_warning) {
      const direction = indicators.fedBalanceSheetChange > 0 ? "expanding" : "contracting";
      triggered.push(`Fed balance sheet ${direction}: ${indicators.fedBalanceSheetChange.toFixed(1)}%/week`);
      score += 1;
    }
  }

  // Determine level based on score
  let level: CrisisLevel = 0;
  if (score >= 6) {
    level = 3; // Full crisis
  } else if (score >= 4) {
    level = 2; // High alert
  } else if (score >= 2) {
    level = 1; // Elevated
  }

  return { level, triggeredIndicators: triggered };
}

// ============================================================================
// SECTION 3: DURABLE OBJECT CLASS
// ============================================================================
// The main agent class. Modify alarm() to change the core loop.
// Add new HTTP endpoints in fetch() for custom dashboard controls.
// ============================================================================

export class MahoragaHarness extends DurableObject<Env> {
  private state: AgentState = { ...DEFAULT_STATE };
  private _llm: LLMProvider | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    this._llm = createLLMProvider(env);
    if (this._llm) {
      console.log(`[MahoragaHarness] LLM Provider initialized: ${env.LLM_PROVIDER || "openai-raw"}`);
    } else {
      console.log("[MahoragaHarness] WARNING: No valid LLM provider configured - research disabled");
    }

    this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get<AgentState>("state");
      if (stored) {
        this.state = { ...DEFAULT_STATE, ...stored };
        // Migrate config: replace null values with defaults from DEFAULT_CONFIG
        // This handles configs saved before new fields were added
        this.state.config = this.migrateConfig(stored.config);
        // Migrate other null fields to defaults (spread only handles undefined)
        if (this.state.dexPaperBalance == null || Number.isNaN(this.state.dexPaperBalance)) {
          this.state.dexPaperBalance = 1.0;
        }
        if (this.state.dexTradeHistory == null) this.state.dexTradeHistory = [];
        if (this.state.dexRealizedPnL == null || Number.isNaN(this.state.dexRealizedPnL)) {
          this.state.dexRealizedPnL = 0;
        }
        // Initialize streak and drawdown tracking fields (#15, #16, #17)
        if (this.state.dexMaxConsecutiveLosses == null) this.state.dexMaxConsecutiveLosses = 0;
        if (this.state.dexCurrentLossStreak == null) this.state.dexCurrentLossStreak = 0;
        if (this.state.dexMaxDrawdownPct == null) this.state.dexMaxDrawdownPct = 0;
        if (this.state.dexMaxDrawdownDuration == null) this.state.dexMaxDrawdownDuration = 0;
        if (this.state.dexDrawdownStartTime === undefined) this.state.dexDrawdownStartTime = null;
        if (this.state.dexPeakBalance == null || Number.isNaN(this.state.dexPeakBalance)) {
          this.state.dexPeakBalance = this.state.dexPaperBalance;
        }
        // Initialize crisis state if missing
        if (!this.state.crisisState) {
          this.state.crisisState = DEFAULT_STATE.crisisState;
        }
        if (this.state.lastCrisisCheck == null) {
          this.state.lastCrisisCheck = 0;
        }
      }
      this.initializeLLM();

      // Reschedule alarm if stale - in local dev, past alarms don't fire on restart;
      // in production this is a defensive check for edge cases (long inactivity, redeployments)
      if (this.state.enabled) {
        const existingAlarm = await this.ctx.storage.getAlarm();
        const now = Date.now();
        if (!existingAlarm || existingAlarm < now) {
          await this.ctx.storage.setAlarm(now + 5_000);
        }
      }
    });
  }

  /**
   * Migrate config by replacing null values with defaults from DEFAULT_CONFIG.
   * This ensures configs saved before new fields were added get proper defaults.
   */
  private migrateConfig(storedConfig: Partial<AgentConfig>): AgentConfig {
    const migrated = { ...DEFAULT_CONFIG };
    for (const key of Object.keys(DEFAULT_CONFIG) as (keyof AgentConfig)[]) {
      const storedValue = storedConfig[key];
      // Only use stored value if it's not null/undefined
      if (storedValue !== null && storedValue !== undefined) {
        (migrated as Record<string, unknown>)[key] = storedValue;
      }
    }
    return migrated;
  }

  private initializeLLM() {
    const provider = this.state.config.llm_provider || this.env.LLM_PROVIDER || "openai-raw";
    const model = this.state.config.llm_model || this.env.LLM_MODEL || "gpt-4o-mini";

    const effectiveEnv: Env = {
      ...this.env,
      LLM_PROVIDER: provider as Env["LLM_PROVIDER"],
      LLM_MODEL: model,
    };

    this._llm = createLLMProvider(effectiveEnv);
    if (this._llm) {
      console.log(`[MahoragaHarness] LLM Provider initialized: ${provider} (${model})`);
    } else {
      console.log("[MahoragaHarness] WARNING: No valid LLM provider configured");
    }
  }

  // ============================================================================
  // [CUSTOMIZABLE] ALARM HANDLER - Main entry point for scheduled work
  // ============================================================================
  // This runs every 30 seconds. Modify to change:
  // - What runs and when (intervals, market hours checks)
  // - Order of operations (data → research → trading)
  // - Add new features (e.g., portfolio rebalancing, alerts)
  // ============================================================================

  async alarm(): Promise<void> {
    if (!this.state.enabled) {
      this.log("System", "alarm_skipped", { reason: "Agent not enabled" });
      return;
    }

    const now = Date.now();
    const RESEARCH_INTERVAL_MS = 120_000;
    const POSITION_RESEARCH_INTERVAL_MS = 300_000;

    try {
      const alpaca = createAlpacaProviders(this.env);
      const clock = await alpaca.trading.getClock();

      // ═══════════════════════════════════════════════════════════════════════
      // CRISIS MODE CHECK - Run before any trading logic
      // Monitors market stress indicators and takes protective actions
      // ═══════════════════════════════════════════════════════════════════════
      if (this.state.config.crisis_mode_enabled) {
        await this.runCrisisCheck();

        // If full crisis (level 3), execute emergency actions and skip normal trading
        if (this.isCrisisFullPanic()) {
          this.log("Crisis", "full_panic_mode", {
            message: "CRISIS LEVEL 3 - Halting all trading activities",
          });
          await this.executeCrisisActions(alpaca);
          await this.persist();
          await this.scheduleNextAlarm();
          return; // Skip all normal trading
        }

        // If high alert (level 2), execute protective actions but continue monitoring
        if (this.state.crisisState.level >= 2) {
          await this.executeCrisisActions(alpaca);
        }
      }
      // ═══════════════════════════════════════════════════════════════════════

      if (now - this.state.lastDataGatherRun >= this.state.config.data_poll_interval_ms) {
        await this.runDataGatherers();
        this.state.lastDataGatherRun = now;
      }

      if (now - this.state.lastResearchRun >= RESEARCH_INTERVAL_MS) {
        await this.researchTopSignals(5);
        this.state.lastResearchRun = now;
      }

      if (this.isPreMarketWindow() && !this.state.premarketPlan) {
        await this.runPreMarketAnalysis();
      }

      const positions = await alpaca.trading.getPositions();

      if (this.state.config.crypto_enabled) {
        await this.runCryptoTrading(alpaca, positions);
      }

      // DEX momentum trading (Solana tokens via DexScreener/Jupiter)
      if (this.state.config.dex_enabled) {
        await this.gatherDexMomentum();
        await this.runDexTrading();
        // Always record snapshot when DEX is enabled (for chart history)
        await this.recordDexSnapshot();
      }

      if (clock.is_open) {
        if (this.isMarketJustOpened() && this.state.premarketPlan) {
          await this.executePremarketPlan();
        }

        if (now - this.state.lastAnalystRun >= this.state.config.analyst_interval_ms) {
          await this.runAnalyst();
          this.state.lastAnalystRun = now;
        }

        if (positions.length > 0 && now - this.state.lastResearchRun >= POSITION_RESEARCH_INTERVAL_MS) {
          for (const pos of positions) {
            if (pos.asset_class !== "us_option") {
              await this.researchPosition(pos.symbol, pos);
            }
          }
        }

        if (this.isOptionsEnabled()) {
          const optionsExits = await this.checkOptionsExits(positions);
          for (const exit of optionsExits) {
            await this.executeSell(alpaca, exit.symbol, exit.reason);
          }
        }

        if (this.isTwitterEnabled()) {
          const heldSymbols = positions.map(p => p.symbol);
          const breakingNews = await this.checkTwitterBreakingNews(heldSymbols);
          for (const news of breakingNews) {
            if (news.is_breaking) {
              this.log("System", "twitter_breaking_news", {
                symbol: news.symbol,
                headline: news.headline.slice(0, 100),
              });
            }
          }
        }
      }

      await this.persist();
    } catch (error) {
      this.log("System", "alarm_error", { error: String(error) });
    }

    await this.scheduleNextAlarm();
  }

  private async scheduleNextAlarm(): Promise<void> {
    const nextRun = Date.now() + 30_000;  // 30 seconds
    await this.ctx.storage.setAlarm(nextRun);
  }

  // ============================================================================
  // HTTP HANDLER (for dashboard/control)
  // ============================================================================
  // Add new endpoints here for custom dashboard controls.
  // Example: /webhook for external alerts, /backtest for simulation
  // ============================================================================

  private constantTimeCompare(a: string, b: string): boolean {
    if (a.length !== b.length) return false;
    let mismatch = 0;
    for (let i = 0; i < a.length; i++) {
      mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
    }
    return mismatch === 0;
  }

  private isAuthorized(request: Request): boolean {
    const token = this.env.MAHORAGA_API_TOKEN;
    if (!token) {
      console.warn("[MahoragaHarness] MAHORAGA_API_TOKEN not set - denying request");
      return false;
    }
    const authHeader = request.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return false;
    }
    return this.constantTimeCompare(authHeader.slice(7), token);
  }

  private isKillSwitchAuthorized(request: Request): boolean {
    const secret = this.env.KILL_SWITCH_SECRET;
    if (!secret) {
      return false;
    }
    const authHeader = request.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return false;
    }
    return this.constantTimeCompare(authHeader.slice(7), secret);
  }

  private unauthorizedResponse(): Response {
    return new Response(
      JSON.stringify({ error: "Unauthorized. Requires: Authorization: Bearer <MAHORAGA_API_TOKEN>" }),
      { status: 401, headers: { "Content-Type": "application/json" } }
    );
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const action = url.pathname.slice(1);

    const protectedActions = ["enable", "disable", "config", "trigger", "status", "logs", "costs", "signals", "setup/status", "dex/reset", "dex/clear-cooldowns", "dex/clear-breaker", "crisis/toggle", "crisis/check"];
    if (protectedActions.includes(action)) {
      if (!this.isAuthorized(request)) {
        return this.unauthorizedResponse();
      }
    }

    try {
      switch (action) {
        case "status":
          return this.handleStatus();

        case "setup/status":
          return this.jsonResponse({ ok: true, data: { configured: true } });

        case "config":
          if (request.method === "POST") {
            return this.handleUpdateConfig(request);
          }
          return this.jsonResponse({ ok: true, data: this.state.config });

        case "enable":
          return this.handleEnable();

        case "disable":
          return this.handleDisable();

        case "logs":
          return this.handleGetLogs(url);

        case "costs":
          return this.jsonResponse({ costs: this.state.costTracker });

        case "signals":
          return this.jsonResponse({ signals: this.state.signalCache });

        case "trigger":
          await this.alarm();
          return this.jsonResponse({ ok: true, message: "Alarm triggered" });

        case "kill":
          if (!this.isKillSwitchAuthorized(request)) {
            return new Response(
              JSON.stringify({ error: "Forbidden. Requires: Authorization: Bearer <KILL_SWITCH_SECRET>" }),
              { status: 403, headers: { "Content-Type": "application/json" } }
            );
          }
          return this.handleKillSwitch();

        case "dex/reset":
          return this.handleDexReset();

        case "dex/clear-cooldowns":
          return this.handleDexClearCooldowns();

        case "dex/clear-breaker":
          return this.handleDexClearBreaker();

        case "crisis/toggle":
          return this.handleCrisisToggle(request);

        case "crisis/check":
          // Force an immediate crisis indicator check
          await this.runCrisisCheck();
          await this.persist();
          return this.jsonResponse({
            ok: true,
            crisisState: this.state.crisisState,
          });

        default:
          return new Response("Not found", { status: 404 });
      }
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }

  private async handleStatus(): Promise<Response> {
    const alpaca = createAlpacaProviders(this.env);

    let account: Account | null = null;
    let positions: Position[] = [];
    let clock: MarketClock | null = null;

    try {
      [account, positions, clock] = await Promise.all([
        alpaca.trading.getAccount(),
        alpaca.trading.getPositions(),
        alpaca.trading.getClock(),
      ]);

      for (const pos of positions || []) {
        const entry = this.state.positionEntries[pos.symbol];
        if (entry && entry.entry_price === 0 && pos.avg_entry_price) {
          entry.entry_price = pos.avg_entry_price;
          entry.peak_price = Math.max(entry.peak_price, pos.current_price);
        }
      }
    } catch (e) {
      // Ignore - will return null
    }

    // Fetch real SOL price (cached for 5 minutes)
    const solPriceUsd = await getSolPriceUsd();

    // Calculate DEX positions with current P&L
    const dexPositionsWithPnL = Object.entries(this.state.dexPositions).map(([tokenAddress, pos]) => {
      const currentSignal = this.state.dexSignals.find(s => s.tokenAddress === tokenAddress);
      const currentPrice = currentSignal?.priceUsd || pos.entryPrice;

      // Handle legacy positions where tokenAmount/entrySol wasn't stored or is NaN
      const entrySol = (pos.entrySol == null || Number.isNaN(pos.entrySol))
        ? (this.state.config.dex_max_position_sol ?? 0.1)
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

    return this.jsonResponse({
      ok: true,
      data: {
        enabled: this.state.enabled,
        account,
        positions,
        clock,
        config: this.state.config,
        signals: this.state.signalCache,
        logs: this.state.logs.slice(-100),
        costs: this.state.costTracker,
        lastAnalystRun: this.state.lastAnalystRun,
        lastResearchRun: this.state.lastResearchRun,
        signalResearch: this.state.signalResearch,
        positionResearch: this.state.positionResearch,
        positionEntries: this.state.positionEntries,
        twitterConfirmations: this.state.twitterConfirmations,
        premarketPlan: this.state.premarketPlan,
        stalenessAnalysis: this.state.stalenessAnalysis,
        // DEX positions with live P&L
        dexPositions: dexPositionsWithPnL,
        dexSignals: this.state.dexSignals.slice(0, 10), // Top 10 momentum signals
        dexPaperTrading: {
          enabled: true,
          paperBalance: (this.state.dexPaperBalance == null || Number.isNaN(this.state.dexPaperBalance)) ? 1.0 : this.state.dexPaperBalance,
          realizedPnL: (this.state.dexRealizedPnL == null || Number.isNaN(this.state.dexRealizedPnL)) ? 0 : this.state.dexRealizedPnL,
          totalTrades: this.state.dexTradeHistory?.length ?? 0,
          winningTrades: this.state.dexTradeHistory?.filter(t => t.pnlPct > 0).length ?? 0,
          losingTrades: this.state.dexTradeHistory?.filter(t => t.pnlPct <= 0).length ?? 0,
          recentTrades: this.state.dexTradeHistory?.slice(-50) ?? [], // Keep last 50 trades for history
          // Trading metrics (#15, #16, #17)
          ...calculateDexTradingMetrics(this.state.dexTradeHistory ?? [], this.state),
          // Circuit breaker status (#10)
          circuitBreakerActive: this.state.dexCircuitBreakerUntil ? Date.now() < this.state.dexCircuitBreakerUntil : false,
          circuitBreakerUntil: this.state.dexCircuitBreakerUntil,
          recentStopLosses: this.state.dexRecentStopLosses?.length ?? 0,
          // Drawdown protection status (#11)
          drawdownPaused: this.state.dexDrawdownPaused ?? false,
          peakValue: this.state.dexPeakValue ?? 0,
          currentDrawdownPct: this.state.dexPeakValue && this.state.dexPeakValue > 0
            ? ((this.state.dexPeakValue - (this.state.dexPaperBalance ?? 0)) / this.state.dexPeakValue * 100)
            : 0,
        },
        dexPortfolioHistory: this.state.dexPortfolioHistory?.slice(-50) ?? [], // Last 50 snapshots for charting
        // Crisis Mode status
        crisisState: this.state.crisisState,
        lastCrisisCheck: this.state.lastCrisisCheck,
      },
    });
  }

  private async handleUpdateConfig(request: Request): Promise<Response> {
    const body = await request.json() as Partial<AgentConfig>;
    this.state.config = { ...this.state.config, ...body };
    this.initializeLLM();
    await this.persist();
    return this.jsonResponse({ ok: true, config: this.state.config });
  }

  private async handleEnable(): Promise<Response> {
    this.state.enabled = true;
    await this.persist();
    await this.scheduleNextAlarm();
    this.log("System", "agent_enabled", {});
    return this.jsonResponse({ ok: true, enabled: true });
  }

  private async handleDisable(): Promise<Response> {
    this.state.enabled = false;
    await this.ctx.storage.deleteAlarm();
    await this.persist();
    this.log("System", "agent_disabled", {});
    return this.jsonResponse({ ok: true, enabled: false });
  }

  private async handleDexReset(): Promise<Response> {
    // Use configured starting balance, fallback to 1 SOL
    const startingBalance = this.state.config.dex_starting_balance_sol || 1.0;
    this.state.dexPositions = {};
    this.state.dexSignals = [];
    this.state.dexTradeHistory = [];
    this.state.dexRealizedPnL = 0;
    this.state.dexPaperBalance = startingBalance;
    this.state.dexPortfolioHistory = []; // Clear history on reset
    // Reset streak and drawdown tracking (#15, #16, #17)
    this.state.dexMaxConsecutiveLosses = 0;
    this.state.dexCurrentLossStreak = 0;
    this.state.dexMaxDrawdownPct = 0;
    this.state.dexMaxDrawdownDuration = 0;
    this.state.dexDrawdownStartTime = null;
    this.state.dexPeakBalance = startingBalance;
    // Reset circuit breaker state (#10)
    this.state.dexRecentStopLosses = [];
    this.state.dexCircuitBreakerUntil = null;
    // Reset drawdown protection state (#11)
    this.state.dexPeakValue = startingBalance;
    this.state.dexDrawdownPaused = false;
    // Reset stop loss cooldowns (#8) - allow fresh entries on all tokens
    this.state.dexStopLossCooldowns = {};
    await this.persist();
    this.log("DexMomentum", "paper_reset", { startingBalance: startingBalance + " SOL" });
    return this.jsonResponse({
      ok: true,
      message: "DEX paper trading reset",
      paperBalance: startingBalance,
    });
  }

  private async handleDexClearCooldowns(): Promise<Response> {
    const clearedCount = Object.keys(this.state.dexStopLossCooldowns || {}).length;
    this.state.dexStopLossCooldowns = {};
    await this.persist();
    this.log("DexMomentum", "cooldowns_cleared", { count: clearedCount });
    return this.jsonResponse({
      ok: true,
      message: `Cleared ${clearedCount} token cooldowns`,
      clearedCount,
    });
  }

  private async handleDexClearBreaker(): Promise<Response> {
    const wasActive = !!this.state.dexCircuitBreakerUntil;
    this.state.dexCircuitBreakerUntil = null;
    this.state.dexRecentStopLosses = [];
    await this.persist();
    this.log("DexMomentum", "breaker_manually_cleared", { wasActive });
    return this.jsonResponse({
      ok: true,
      message: wasActive ? "Circuit breaker cleared" : "Circuit breaker was not active",
      wasActive,
    });
  }

  private handleGetLogs(url: URL): Response {
    const limit = parseInt(url.searchParams.get("limit") || "100");
    const logs = this.state.logs.slice(-limit);
    return this.jsonResponse({ logs });
  }

  private async handleKillSwitch(): Promise<Response> {
    this.state.enabled = false;
    await this.ctx.storage.deleteAlarm();
    this.state.signalCache = [];
    this.state.signalResearch = {};
    this.state.premarketPlan = null;
    await this.persist();
    this.log("System", "kill_switch_activated", { timestamp: new Date().toISOString() });
    return this.jsonResponse({
      ok: true,
      message: "KILL SWITCH ACTIVATED. Agent disabled, alarms cancelled, signal cache cleared.",
      note: "Existing positions are NOT automatically closed. Review and close manually if needed."
    });
  }

  private async handleCrisisToggle(request: Request): Promise<Response> {
    const body = await request.json() as { manualOverride?: boolean; level?: CrisisLevel };

    // Toggle manual override
    if (body.manualOverride !== undefined) {
      this.state.crisisState.manualOverride = body.manualOverride;
      this.log("Crisis", "manual_override_changed", {
        manualOverride: body.manualOverride,
      });
    }

    // Manually set crisis level (only when override is active)
    if (body.level !== undefined && this.state.crisisState.manualOverride) {
      const previousLevel = this.state.crisisState.level;
      this.state.crisisState.level = body.level;
      this.state.crisisState.lastLevelChange = Date.now();
      this.log("Crisis", "manual_level_set", {
        previous: previousLevel,
        current: body.level,
      });
    }

    await this.persist();

    return this.jsonResponse({
      ok: true,
      crisisState: this.state.crisisState,
    });
  }

  // ============================================================================
  // SECTION 4: DATA GATHERING
  // ============================================================================
  // [CUSTOMIZABLE] This is where you add NEW DATA SOURCES.
  // 
  // To add a new source:
  // 1. Create a new gather method (e.g., gatherNewsAPI)
  // 2. Add it to runDataGatherers() Promise.all
  // 3. Add source weight to SOURCE_CONFIG.weights
  // 4. Return Signal[] with your source name
  //
  // Each gatherer returns Signal[] which get merged into signalCache.
  // ============================================================================

  private async runDataGatherers(): Promise<void> {
    this.log("System", "gathering_data", {});

    await tickerCache.refreshSecTickersIfNeeded();

    const [stocktwitsSignals, redditSignals, cryptoSignals] = await Promise.all([
      this.gatherStockTwits(),
      this.gatherReddit(),
      this.gatherCrypto(),
    ]);

    const allSignals = [...stocktwitsSignals, ...redditSignals, ...cryptoSignals];

    const MAX_SIGNALS = 200;
    const MAX_AGE_MS = 24 * 60 * 60 * 1000;
    const now = Date.now();

    const freshSignals = allSignals
      .filter(s => now - s.timestamp < MAX_AGE_MS)
      .sort((a, b) => Math.abs(b.sentiment) - Math.abs(a.sentiment))
      .slice(0, MAX_SIGNALS);

    this.state.signalCache = freshSignals;

    this.log("System", "data_gathered", {
      stocktwits: stocktwitsSignals.length,
      reddit: redditSignals.length,
      crypto: cryptoSignals.length,
      total: this.state.signalCache.length,
    });
  }

  private async gatherStockTwits(): Promise<Signal[]> {
    const signals: Signal[] = [];
    const sourceWeight = SOURCE_CONFIG.weights.stocktwits;

    const stocktwitsHeaders = {
      "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "application/json",
      "Accept-Language": "en-US,en;q=0.9",
    };

    const fetchWithRetry = async (url: string, maxRetries = 3): Promise<Response | null> => {
      for (let i = 0; i < maxRetries; i++) {
        try {
          const res = await fetch(url, { headers: stocktwitsHeaders });
          if (res.ok) return res;
          if (res.status === 403) {
            await this.sleep(1000 * Math.pow(2, i));
            continue;
          }
          return null;
        } catch {
          await this.sleep(1000 * Math.pow(2, i));
        }
      }
      return null;
    };

    try {
      const trendingRes = await fetchWithRetry("https://api.stocktwits.com/api/2/trending/symbols.json");
      if (!trendingRes) {
        this.log("StockTwits", "cloudflare_blocked", { 
          message: "StockTwits API blocked by Cloudflare - using Reddit only" 
        });
        return [];
      }
      const trendingData = await trendingRes.json() as { symbols?: Array<{ symbol: string }> };
      const trending = trendingData.symbols || [];

      for (const sym of trending.slice(0, 15)) {
        try {
          const streamRes = await fetchWithRetry(`https://api.stocktwits.com/api/2/streams/symbol/${sym.symbol}.json?limit=30`);
          if (!streamRes) continue;
          const streamData = await streamRes.json() as { messages?: Array<{ entities?: { sentiment?: { basic?: string } }; created_at?: string }> };
          const messages = streamData.messages || [];

          let bullish = 0, bearish = 0, totalTimeDecay = 0;
          for (const msg of messages) {
            const sentiment = msg.entities?.sentiment?.basic;
            const msgTime = new Date(msg.created_at || Date.now()).getTime() / 1000;
            const timeDecay = calculateTimeDecay(msgTime);
            totalTimeDecay += timeDecay;

            if (sentiment === "Bullish") bullish += timeDecay;
            else if (sentiment === "Bearish") bearish += timeDecay;
          }

          const total = messages.length;
          const effectiveTotal = totalTimeDecay || 1;
          const score = effectiveTotal > 0 ? (bullish - bearish) / effectiveTotal : 0;
          const avgFreshness = total > 0 ? totalTimeDecay / total : 0;

          if (total >= 5) {
            const weightedSentiment = score * sourceWeight * avgFreshness;

            signals.push({
              symbol: sym.symbol,
              source: "stocktwits",
              source_detail: "stocktwits_trending",
              sentiment: weightedSentiment,
              raw_sentiment: score,
              volume: total,
              bullish: Math.round(bullish),
              bearish: Math.round(bearish),
              freshness: avgFreshness,
              source_weight: sourceWeight,
              reason: `StockTwits: ${Math.round(bullish)}B/${Math.round(bearish)}b (${(score * 100).toFixed(0)}%) [fresh:${(avgFreshness * 100).toFixed(0)}%]`,
              timestamp: Date.now(),
            });
          }

          await this.sleep(200);
        } catch {
          continue;
        }
      }
    } catch (error) {
      this.log("StockTwits", "error", { message: String(error) });
    }

    return signals;
  }

  private async gatherReddit(): Promise<Signal[]> {
    const subreddits = ["wallstreetbets", "stocks", "investing", "options"];
    const tickerData = new Map<string, {
      mentions: number;
      weightedSentiment: number;
      rawSentiment: number;
      totalQuality: number;
      upvotes: number;
      comments: number;
      sources: Set<string>;
      bestFlair: string | null;
      bestFlairMult: number;
      freshestPost: number;
    }>();

    for (const sub of subreddits) {
      const sourceWeight = SOURCE_CONFIG.weights[`reddit_${sub}` as keyof typeof SOURCE_CONFIG.weights] || 0.7;

      try {
        const res = await fetch(`https://www.reddit.com/r/${sub}/hot.json?limit=25`, {
          headers: { "User-Agent": "Mahoraga/2.0" },
        });
        if (!res.ok) continue;
        const data = await res.json() as { data?: { children?: Array<{ data: { title?: string; selftext?: string; created_utc?: number; ups?: number; num_comments?: number; link_flair_text?: string } }> } };
        const posts = data.data?.children?.map(c => c.data) || [];

        for (const post of posts) {
          const text = `${post.title || ""} ${post.selftext || ""}`;
          const tickers = extractTickers(text, this.state.config.ticker_blacklist);
          const rawSentiment = detectSentiment(text);

          const timeDecay = calculateTimeDecay(post.created_utc || Date.now() / 1000);
          const engagementMult = getEngagementMultiplier(post.ups || 0, post.num_comments || 0);
          const flairMult = getFlairMultiplier(post.link_flair_text);
          const qualityScore = timeDecay * engagementMult * flairMult * sourceWeight;

          for (const ticker of tickers) {
            if (!tickerData.has(ticker)) {
              tickerData.set(ticker, {
                mentions: 0,
                weightedSentiment: 0,
                rawSentiment: 0,
                totalQuality: 0,
                upvotes: 0,
                comments: 0,
                sources: new Set(),
                bestFlair: null,
                bestFlairMult: 0,
                freshestPost: 0,
              });
            }
            const d = tickerData.get(ticker)!;
            d.mentions++;
            d.rawSentiment += rawSentiment;
            d.weightedSentiment += rawSentiment * qualityScore;
            d.totalQuality += qualityScore;
            d.upvotes += post.ups || 0;
            d.comments += post.num_comments || 0;
            d.sources.add(sub);

            if (flairMult > d.bestFlairMult) {
              d.bestFlair = post.link_flair_text || null;
              d.bestFlairMult = flairMult;
            }

            if ((post.created_utc || 0) > d.freshestPost) {
              d.freshestPost = post.created_utc || 0;
            }
          }
        }

        await this.sleep(1000);
      } catch {
        continue;
      }
    }

    const signals: Signal[] = [];
    const alpaca = createAlpacaProviders(this.env);

    for (const [symbol, data] of tickerData) {
      if (data.mentions >= 2) {
        if (!tickerCache.isKnownSecTicker(symbol)) {
          const cached = tickerCache.getCachedValidation(symbol);
          if (cached === false) continue;
          if (cached === undefined) {
            const isValid = await tickerCache.validateWithAlpaca(symbol, alpaca);
            if (!isValid) {
              this.log("Reddit", "invalid_ticker_filtered", { symbol });
              continue;
            }
          }
        }

        const avgRawSentiment = data.rawSentiment / data.mentions;
        const avgQuality = data.totalQuality / data.mentions;
        const finalSentiment = data.totalQuality > 0
          ? data.weightedSentiment / data.mentions
          : avgRawSentiment * 0.5;
        const freshness = calculateTimeDecay(data.freshestPost);

        signals.push({
          symbol,
          source: "reddit",
          source_detail: `reddit_${Array.from(data.sources).join("+")}`,
          sentiment: finalSentiment,
          raw_sentiment: avgRawSentiment,
          volume: data.mentions,
          upvotes: data.upvotes,
          comments: data.comments,
          quality_score: avgQuality,
          freshness,
          best_flair: data.bestFlair,
          subreddits: Array.from(data.sources),
          source_weight: avgQuality,
          reason: `Reddit(${Array.from(data.sources).join(",")}): ${data.mentions} mentions, ${data.upvotes} upvotes, quality:${(avgQuality * 100).toFixed(0)}%`,
          timestamp: Date.now(),
        });
      }
    }

    return signals;
  }

  private async gatherCrypto(): Promise<Signal[]> {
    if (!this.state.config.crypto_enabled) return [];

    const signals: Signal[] = [];
    const symbols = this.state.config.crypto_symbols || ["BTC/USD", "ETH/USD", "SOL/USD"];
    const alpaca = createAlpacaProviders(this.env);

    for (const symbol of symbols) {
      try {
        const snapshot = await alpaca.marketData.getCryptoSnapshot(symbol);
        if (!snapshot) continue;

        const price = snapshot.latest_trade?.price || 0;
        const prevClose = snapshot.prev_daily_bar?.c || 0;

        if (!price || !prevClose) continue;

        const momentum = ((price - prevClose) / prevClose) * 100;
        const threshold = this.state.config.crypto_momentum_threshold || 2.0;
        const hasSignificantMove = Math.abs(momentum) >= threshold;
        const isBullish = momentum > 0;

        const rawSentiment = hasSignificantMove && isBullish ? Math.min(Math.abs(momentum) / 5, 1) : 0.1;

        signals.push({
          symbol,
          source: "crypto",
          source_detail: "crypto_momentum",
          sentiment: rawSentiment,
          raw_sentiment: rawSentiment,
          volume: snapshot.daily_bar?.v || 0,
          freshness: 1.0,
          source_weight: 0.8,
          reason: `Crypto: ${momentum >= 0 ? '+' : ''}${momentum.toFixed(2)}% (24h)`,
          bullish: isBullish ? 1 : 0,
          bearish: isBullish ? 0 : 1,
          isCrypto: true,
          momentum,
          price,
          timestamp: Date.now(),
        });

        await this.sleep(200);
      } catch (error) {
        this.log("Crypto", "error", { symbol, message: String(error) });
      }
    }

    this.log("Crypto", "gathered_signals", { count: signals.length });
    return signals;
  }

  /**
   * Gather momentum signals from Solana DEXs via DexScreener.
   * Finds tokens aged 3-14 days with proven momentum (not brand new rugs).
   */
  private async gatherDexMomentum(): Promise<void> {
    if (!this.state.config.dex_enabled) return;

    const SCAN_INTERVAL_MS = 30_000; // 30 seconds between scans
    if (Date.now() - this.state.lastDexScanRun < SCAN_INTERVAL_MS) return;

    try {
      const dexScreener = createDexScreenerProvider();

      const signals = await dexScreener.findMomentumTokens({
        // Multi-tier system config
        // Micro-spray (30min-2h) [TOGGLE]
        microSprayEnabled: this.state.config.dex_microspray_enabled ?? false,
        microSprayMinAgeMinutes: 30,
        microSprayMaxAgeHours: 2,
        microSprayMinLiquidity: 10000,
        // Breakout (2-6h) [TOGGLE]
        breakoutEnabled: this.state.config.dex_breakout_enabled ?? false,
        breakoutMinAgeHours: 2,
        breakoutMaxAgeHours: 6,
        breakoutMinLiquidity: 15000,
        breakoutMin5mPump: this.state.config.dex_breakout_min_5m_pump ?? 50,
        // Lottery (current working tier)
        lotteryEnabled: this.state.config.dex_lottery_enabled ?? true,
        lotteryMinAgeHours: this.state.config.dex_lottery_min_age_hours ?? 1,
        lotteryMaxAgeHours: this.state.config.dex_lottery_max_age_hours ?? 6,
        lotteryMinLiquidity: this.state.config.dex_lottery_min_liquidity ?? 15000,
        lotteryMinVolume: 5000,
        // Early tier
        earlyMinAgeDays: this.state.config.dex_early_min_age_days ?? 0.25,
        earlyMaxAgeDays: this.state.config.dex_early_max_age_days ?? 3,
        earlyMinLiquidity: this.state.config.dex_early_min_liquidity ?? 30000,
        earlyMinLegitimacyScore: this.state.config.dex_early_min_legitimacy ?? 40,
        // Established tier
        establishedMinAgeDays: this.state.config.dex_established_min_age_days ?? this.state.config.dex_min_age_days ?? 3,
        establishedMaxAgeDays: this.state.config.dex_established_max_age_days ?? this.state.config.dex_max_age_days ?? 14,
        establishedMinLiquidity: this.state.config.dex_established_min_liquidity ?? this.state.config.dex_min_liquidity ?? 50000,
        // Shared filters
        minVolume24h: this.state.config.dex_min_volume_24h,
        minPriceChange24h: this.state.config.dex_min_price_change,
      });

      this.state.dexSignals = signals;
      this.state.lastDexScanRun = Date.now();

      // Add to signalCache so they show in dashboard active signals
      const now = Date.now();
      const dexAsSignals: Signal[] = signals.map(s => ({
        symbol: s.symbol,
        source: "dexscreener",
        source_detail: `dex_${s.dexId}`,
        sentiment: Math.min(1, s.momentumScore / 100), // Normalize to 0-1
        raw_sentiment: s.momentumScore / 100,
        volume: s.volume24h,
        freshness: 1.0, // Fresh scan
        source_weight: 0.8, // High weight for momentum signals
        reason: `DEX ${s.tier === 'early' ? '🌱' : '🌳'} +${s.priceChange24h.toFixed(0)}%/24h +${s.priceChange6h.toFixed(0)}%/6h, $${Math.round(s.liquidity).toLocaleString()} liq, ${s.ageDays.toFixed(1)}d, legit:${s.legitimacyScore}`,
        timestamp: now,
        isCrypto: true,
        momentum: s.priceChange24h / 100,
        price: s.priceUsd,
      }));

      // Merge with existing signals (remove old DEX signals first)
      this.state.signalCache = [
        ...this.state.signalCache.filter(s => s.source !== "dexscreener"),
        ...dexAsSignals,
      ];

      this.log("DexMomentum", "scan_complete", {
        found: signals.length,
        addedToSignals: dexAsSignals.length,
        top3: signals.slice(0, 3).map(s => ({
          symbol: s.symbol,
          priceChange24h: s.priceChange24h.toFixed(1) + "%",
          liquidity: "$" + Math.round(s.liquidity).toLocaleString(),
          momentumScore: s.momentumScore.toFixed(1),
        })),
      });
    } catch (error) {
      this.log("DexMomentum", "scan_error", { error: String(error) });
    }
  }

  /**
   * Run DEX momentum trading logic with PAPER TRADING.
   * Creates virtual positions to test strategy without real funds.
   * Tracks P&L and trade history for validation.
   */
  private async runDexTrading(): Promise<void> {
    if (!this.state.config.dex_enabled) return;
    if (this.state.dexSignals.length === 0) return;

    // Ensure paper trading state is initialized (migration fix for null/NaN values)
    if (this.state.dexPaperBalance == null || Number.isNaN(this.state.dexPaperBalance)) {
      this.state.dexPaperBalance = 1.0;
    }

    // Fetch real SOL price once for this trading cycle (cached for 5 minutes)
    const solPriceUsd = await getSolPriceUsd();
    const gasFee = this.state.config.dex_gas_fee_sol ?? 0.005;
    if (this.state.dexTradeHistory == null) this.state.dexTradeHistory = [];
    if (this.state.dexRealizedPnL == null || Number.isNaN(this.state.dexRealizedPnL)) {
      this.state.dexRealizedPnL = 0;
    }
    // Initialize streak and drawdown tracking fields (#15, #16, #17)
    if (this.state.dexMaxConsecutiveLosses == null) this.state.dexMaxConsecutiveLosses = 0;
    if (this.state.dexCurrentLossStreak == null) this.state.dexCurrentLossStreak = 0;
    if (this.state.dexMaxDrawdownPct == null) this.state.dexMaxDrawdownPct = 0;
    if (this.state.dexMaxDrawdownDuration == null) this.state.dexMaxDrawdownDuration = 0;
    if (this.state.dexDrawdownStartTime === undefined) this.state.dexDrawdownStartTime = null;
    if (this.state.dexPeakBalance == null || Number.isNaN(this.state.dexPeakBalance)) {
      this.state.dexPeakBalance = this.state.dexPaperBalance;
    }

    const heldTokens = new Set(Object.keys(this.state.dexPositions));

    // Check exits for existing positions
    for (const [tokenAddress, position] of Object.entries(this.state.dexPositions)) {
      const signal = this.state.dexSignals.find(s => s.tokenAddress === tokenAddress);

      // Calculate P&L based on current price vs entry
      const currentPrice = signal?.priceUsd || position.entryPrice;
      const plPct = ((currentPrice - position.entryPrice) / position.entryPrice) * 100;

      // Update peak price
      if (currentPrice > position.peakPrice) {
        position.peakPrice = currentPrice;
      }

      let shouldExit = false;
      let exitReason: "take_profit" | "stop_loss" | "lost_momentum" | "trailing_stop" = "take_profit";

      // Task #13: Check liquidity safety before any exit
      // If liquidity is too low relative to position size, we might get stuck
      const positionValueUsd = position.tokenAmount * currentPrice;
      const currentLiquidity = signal?.liquidity || position.entryLiquidity * 0.5; // Assume 50% decay if signal lost
      const minLiquidityRatio = 5; // Position should be at most 20% of liquidity for safe exit
      const canSafelyExit = currentLiquidity >= positionValueUsd * minLiquidityRatio;

      // Lost momentum - token fell off radar
      // KEY FIX: If position is GREEN, don't exit! Let trailing stop handle it.
      // Only exit on lost momentum if we're RED and it's been missing for a while.
      if (!signal) {
        // Increment missed scan counter
        position.missedScans = (position.missedScans || 0) + 1;

        // If position is profitable, DON'T exit just because it's not trending
        // The trailing stop will protect gains - no need to panic sell a winner
        if (plPct > 0) {
          this.log("DexMomentum", "signal_miss_but_green", {
            symbol: position.symbol,
            missedScans: position.missedScans,
            plPct: plPct.toFixed(1) + "%",
            reason: "Position is profitable - letting trailing stop manage exit, not panic selling",
          });
          // Don't exit - trailing stop will handle it if price drops
        }
        // If position is RED and missing for extended period (10+ scans = 5 min), consider exit
        else if (position.missedScans >= 10) {
          if (canSafelyExit) {
            shouldExit = true;
            exitReason = "lost_momentum";
            this.log("DexMomentum", "lost_momentum_exit", {
              symbol: position.symbol,
              missedScans: position.missedScans,
              plPct: plPct.toFixed(1) + "%",
              reason: "Position is RED and token missing from signals for 5+ minutes",
            });
          } else {
            this.log("DexMomentum", "exit_blocked_low_liquidity", {
              symbol: position.symbol,
              reason: "Token lost momentum but liquidity too low for safe exit",
              positionValueUsd: positionValueUsd.toFixed(2),
              estimatedLiquidity: currentLiquidity.toFixed(2),
            });
          }
        } else {
          this.log("DexMomentum", "signal_miss_grace", {
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
            this.log("DexMomentum", "momentum_decay_exit", {
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
          this.log("DexMomentum", "momentum_decay_but_green", {
            symbol: position.symbol,
            entryMomentumScore: position.entryMomentumScore.toFixed(1),
            currentMomentumScore: signal.momentumScore.toFixed(1),
            plPct: plPct.toFixed(1) + "%",
            reason: "Momentum decayed but position is GREEN - letting trailing stop manage",
          });
        }
        // Take profit
        else if (plPct >= this.state.config.dex_take_profit_pct) {
        if (canSafelyExit) {
          shouldExit = true;
          exitReason = "take_profit";
        } else {
          this.log("DexMomentum", "take_profit_delayed_low_liquidity", {
            symbol: position.symbol,
            plPct: plPct.toFixed(2),
            positionValueUsd: positionValueUsd.toFixed(2),
            liquidity: currentLiquidity.toFixed(2),
          });
        }
      }
      // Trailing stop loss (#9) - activates after position is up by activation_pct
      // For lottery tier, auto-activate at lottery_trailing_activation (default 100%)
      else if (this.state.config.dex_trailing_stop_enabled) {
        const peakGainPct = ((position.peakPrice - position.entryPrice) / position.entryPrice) * 100;

        // Micro-spray, breakout, and lottery have aggressive trailing stops
        const isHighRiskTier = position.tier === 'microspray' || position.tier === 'breakout' || position.tier === 'lottery';
        const activationPct = isHighRiskTier
          ? (this.state.config.dex_lottery_trailing_activation ?? 100) // All high-risk tiers use same activation
          : (this.state.config.dex_trailing_stop_activation_pct ?? 50);
        const distancePct = isHighRiskTier
          ? 20 // High-risk tiers: tighter trailing stop (20% from peak)
          : (this.state.config.dex_trailing_stop_distance_pct ?? 25);

        // Check if trailing stop is activated (position reached activation threshold at some point)
        if (peakGainPct >= activationPct) {
          // Trailing stop price is distance_pct below peak
          const trailingStopPrice = position.peakPrice * (1 - distancePct / 100);
          if (currentPrice <= trailingStopPrice) {
            // For trailing stop, prefer safe exit but log warning if liquidity is low
            if (!canSafelyExit) {
              this.log("DexMomentum", "trailing_stop_low_liquidity_warning", {
                symbol: position.symbol,
                positionValueUsd: positionValueUsd.toFixed(2),
                liquidity: currentLiquidity.toFixed(2),
                warning: "Exiting with potentially high slippage due to low liquidity",
              });
            }
            shouldExit = true;
            exitReason = "trailing_stop";
          }
        }
        // Fixed stop loss if trailing stop not yet activated
        else if (plPct <= -this.state.config.dex_stop_loss_pct) {
          // Stop loss always triggers (even with low liquidity - better to take high slippage than bigger loss)
          if (!canSafelyExit) {
            this.log("DexMomentum", "stop_loss_low_liquidity_warning", {
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
      else if (plPct <= -this.state.config.dex_stop_loss_pct) {
        // Stop loss always triggers (even with low liquidity)
        if (!canSafelyExit) {
          this.log("DexMomentum", "stop_loss_low_liquidity_warning", {
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
      } // End of else block (signal found)

      if (shouldExit) {
        // Record stop loss cooldown (#8) for stop_loss and trailing_stop exits
        // Store exit price for price-based re-entry logic (use currentPrice before slippage)
        if (exitReason === "stop_loss" || exitReason === "trailing_stop") {
          if (!this.state.dexStopLossCooldowns) this.state.dexStopLossCooldowns = {};
          const cooldownHours = this.state.config.dex_stop_loss_cooldown_hours ?? 2;
          this.state.dexStopLossCooldowns[tokenAddress] = {
            exitPrice: currentPrice,
            exitTime: Date.now(),
            fallbackExpiry: Date.now() + (cooldownHours * 60 * 60 * 1000),
          };
        }

        // Apply slippage to exit price (selling pushes price down = worse exit)
        const slippageModel = this.state.config.dex_slippage_model || "realistic";
        const positionValueUsd = position.tokenAmount * currentPrice;
        const liquidity = signal?.liquidity || 10000; // Fallback liquidity for lost momentum tokens
        const sellSlippage = calculateDexSlippage(
          slippageModel,
          positionValueUsd,
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

        this.state.dexTradeHistory.push(tradeRecord);
        this.state.dexRealizedPnL += pnlSol;
        this.state.dexPaperBalance += position.entrySol + pnlSol;

        // Deduct gas fee for the sell transaction
        this.state.dexPaperBalance -= gasFee;
        this.log("DexMomentum", "gas_fee_deducted", {
          action: "sell",
          symbol: position.symbol,
          gasFee: gasFee.toFixed(4) + " SOL",
          gasFeeUsd: "$" + (gasFee * solPriceUsd).toFixed(2),
        });

        // Update streak and drawdown tracking (#17)
        const isWin = pnlSol > 0;
        updateStreakAndDrawdownState(isWin, this.state.dexPaperBalance, this.state);

        // Record stop loss for circuit breaker (#10)
        if (exitReason === "stop_loss") {
          if (!this.state.dexRecentStopLosses) this.state.dexRecentStopLosses = [];
          this.state.dexRecentStopLosses.push({
            timestamp: Date.now(),
            symbol: position.symbol,
          });

          // Check if circuit breaker should trigger
          const windowMs = (this.state.config.dex_circuit_breaker_window_hours || 1) * 60 * 60 * 1000;
          const recentLosses = this.state.dexRecentStopLosses.filter(
            sl => Date.now() - sl.timestamp < windowMs
          );
          const maxLosses = this.state.config.dex_circuit_breaker_losses || 3;

          if (recentLosses.length >= maxLosses) {
            const pauseMs = (this.state.config.dex_circuit_breaker_pause_hours || 4) * 60 * 60 * 1000;
            this.state.dexCircuitBreakerUntil = Date.now() + pauseMs;
            this.log("DexMomentum", "circuit_breaker_triggered", {
              stopLossCount: recentLosses.length,
              windowHours: this.state.config.dex_circuit_breaker_window_hours || 1,
              pauseUntil: new Date(this.state.dexCircuitBreakerUntil).toISOString(),
              pauseHours: this.state.config.dex_circuit_breaker_pause_hours || 4,
              recentSymbols: recentLosses.map(sl => sl.symbol).join(", "),
            });
          }
        }

        // Remove position
        delete this.state.dexPositions[tokenAddress];

        this.log("DexMomentum", "paper_sell", {
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
          totalRealizedPnL: this.state.dexRealizedPnL.toFixed(4) + " SOL",
          paperBalance: this.state.dexPaperBalance.toFixed(4) + " SOL",
        });
      }
    }

    // Look for new entries
    const positionCount = Object.keys(this.state.dexPositions).length;
    if (positionCount >= this.state.config.dex_max_positions) return;

    // Check circuit breaker (#10) - pause new entries if too many stop losses
    // Now with stabilization-based early clearing
    if (this.state.dexCircuitBreakerUntil && Date.now() < this.state.dexCircuitBreakerUntil) {
      const minCooldownMs = (this.state.config.dex_breaker_min_cooldown_minutes ?? 30) * 60 * 1000;
      const breakerStartTime = this.state.dexCircuitBreakerUntil -
        ((this.state.config.dex_circuit_breaker_pause_hours ?? 1) * 60 * 60 * 1000);
      const minCooldownPassed = Date.now() >= breakerStartTime + minCooldownMs;

      // Check for early clear conditions after minimum cooldown
      if (minCooldownPassed) {
        // Condition 1: An open position has recovered to positive
        const hasRecoveredPosition = Object.values(this.state.dexPositions).some(pos => {
          const signal = this.state.dexSignals.find(s => s.tokenAddress === pos.tokenAddress);
          const currentPrice = signal?.priceUsd || pos.entryPrice;
          const plPct = ((currentPrice - pos.entryPrice) / pos.entryPrice) * 100;
          return plPct > 0;
        });

        // Condition 2: High conviction signal available
        const highConvictionSignal = this.state.dexSignals.some(s =>
          s.momentumScore >= (this.state.config.dex_reentry_min_momentum ?? 70) &&
          !Object.keys(this.state.dexPositions).includes(s.tokenAddress)
        );

        if (hasRecoveredPosition || highConvictionSignal) {
          this.state.dexCircuitBreakerUntil = null;
          this.log("DexMomentum", "circuit_breaker_early_clear", {
            reason: hasRecoveredPosition ? "position_recovered" : "high_conviction_signal",
            minutesPaused: Math.round((Date.now() - breakerStartTime) / 60000),
          });
        } else {
          this.log("DexMomentum", "circuit_breaker_active", {
            pausedUntil: new Date(this.state.dexCircuitBreakerUntil).toISOString(),
            remainingMinutes: Math.round((this.state.dexCircuitBreakerUntil - Date.now()) / 60000),
            minCooldownPassed: true,
            waitingFor: "position recovery or high conviction signal (momentum > 70)",
          });
          return;
        }
      } else {
        this.log("DexMomentum", "circuit_breaker_active", {
          pausedUntil: new Date(this.state.dexCircuitBreakerUntil).toISOString(),
          remainingMinutes: Math.round((this.state.dexCircuitBreakerUntil - Date.now()) / 60000),
          minCooldownPassed: false,
        });
        return;
      }
    } else if (this.state.dexCircuitBreakerUntil && Date.now() >= this.state.dexCircuitBreakerUntil) {
      // Circuit breaker time expired, clear it
      this.state.dexCircuitBreakerUntil = null;
      this.log("DexMomentum", "circuit_breaker_cleared", { reason: "time_expired" });
    }

    // Check drawdown pause (#11) - pause new entries if max drawdown exceeded
    if (this.state.dexDrawdownPaused) {
      this.log("DexMomentum", "drawdown_pause_active", {
        reason: "Max drawdown limit exceeded",
      });
      return;
    }

    // Calculate position size:
    // 1. Use percentage of current balance
    // 2. Cap at max_position_sol
    // 3. Ensure minimum viable position (0.01 SOL)
    const pctSize = (this.state.config.dex_position_size_pct || 33) / 100;
    const maxCap = this.state.config.dex_max_position_sol || 1.0;
    const minPosition = 0.01; // Minimum viable position

    if (this.state.dexPaperBalance < minPosition) {
      return; // Not enough paper balance
    }

    // Clean up old cooldowns (#8) - remove entries older than 24 hours to prevent memory bloat
    if (this.state.dexStopLossCooldowns) {
      const now = Date.now();
      const maxCooldownAge = 24 * 60 * 60 * 1000; // 24 hours
      for (const [tokenAddr, cooldown] of Object.entries(this.state.dexStopLossCooldowns)) {
        // Handle both old format (number) and new format (object)
        const exitTime = typeof cooldown === 'number' ? cooldown : cooldown.exitTime;
        if (now - exitTime > maxCooldownAge) {
          delete this.state.dexStopLossCooldowns[tokenAddr];
        }
      }
    }

    const candidates = this.state.dexSignals
      .filter(s => !heldTokens.has(s.tokenAddress))
      .filter(s => s.momentumScore >= 50) // Minimum momentum score threshold
      // Check stop loss cooldown (#8) - price-based re-entry logic
      .filter(s => {
        if (!this.state.dexStopLossCooldowns) return true;
        const cooldown = this.state.dexStopLossCooldowns[s.tokenAddress];
        if (!cooldown) return true;

        // Handle legacy format (just a number timestamp)
        if (typeof cooldown === 'number') {
          return Date.now() >= cooldown;
        }

        const recoveryPct = this.state.config.dex_reentry_recovery_pct ?? 15;
        const minMomentum = this.state.config.dex_reentry_min_momentum ?? 70;

        // Allow re-entry if price has recovered X% above exit price
        const priceRecoveryThreshold = cooldown.exitPrice * (1 + recoveryPct / 100);
        if (s.priceUsd >= priceRecoveryThreshold) {
          this.log("DexMomentum", "cooldown_cleared_price_recovery", {
            symbol: s.symbol,
            exitPrice: cooldown.exitPrice.toFixed(6),
            currentPrice: s.priceUsd.toFixed(6),
            recoveryPct: (((s.priceUsd - cooldown.exitPrice) / cooldown.exitPrice) * 100).toFixed(1) + "%",
          });
          delete this.state.dexStopLossCooldowns[s.tokenAddress];
          return true;
        }

        // Allow re-entry if momentum score is very strong AND minimum time has passed
        // This prevents immediate re-entry on dead cat bounces
        const minCooldownMs = 5 * 60 * 1000; // 5 minutes minimum after any stop loss
        const timeSinceExit = Date.now() - cooldown.exitTime;

        if (s.momentumScore >= minMomentum && timeSinceExit >= minCooldownMs) {
          this.log("DexMomentum", "cooldown_cleared_high_momentum", {
            symbol: s.symbol,
            momentumScore: s.momentumScore.toFixed(1),
            threshold: minMomentum,
            minutesSinceExit: Math.round(timeSinceExit / 60000),
          });
          delete this.state.dexStopLossCooldowns[s.tokenAddress];
          return true;
        } else if (s.momentumScore >= minMomentum && timeSinceExit < minCooldownMs) {
          this.log("DexMomentum", "cooldown_waiting_min_time", {
            symbol: s.symbol,
            momentumScore: s.momentumScore.toFixed(1),
            minutesSinceExit: Math.round(timeSinceExit / 60000),
            minMinutesRequired: 5,
          });
          return false;
        }

        // Fallback: allow re-entry after time expires
        if (Date.now() >= cooldown.fallbackExpiry) {
          this.log("DexMomentum", "cooldown_cleared_time_expired", {
            symbol: s.symbol,
          });
          delete this.state.dexStopLossCooldowns[s.tokenAddress];
          return true;
        }

        return false;
      })
      .slice(0, 3);

    // Count current positions by tier for limit checks
    const tierCounts = {
      microspray: 0,
      breakout: 0,
      lottery: 0,
    };
    for (const p of Object.values(this.state.dexPositions)) {
      if (p.tier === 'microspray') tierCounts.microspray++;
      else if (p.tier === 'breakout') tierCounts.breakout++;
      else if (p.tier === 'lottery') tierCounts.lottery++;
    }
    const maxMicroSpray = this.state.config.dex_microspray_max_positions ?? 10;
    const maxBreakout = this.state.config.dex_breakout_max_positions ?? 5;
    const maxLotteryPositions = this.state.config.dex_lottery_max_positions ?? 5;

    this.log("DexMomentum", "buy_candidates", {
      count: candidates.length,
      candidates: candidates.map(c => `${c.symbol}(${c.tier})`).join(", "),
    });

    // Create Birdeye provider once outside loop so throttle works across all candidates
    const birdeye = this.state.config.dex_chart_analysis_enabled && this.env.BIRDEYE_API_KEY
      ? createBirdeyeProvider(this.env.BIRDEYE_API_KEY)
      : null;

    for (const candidate of candidates) {
      if (Object.keys(this.state.dexPositions).length >= this.state.config.dex_max_positions) break;

      // Check tier-specific position limits
      if (candidate.tier === 'microspray' && tierCounts.microspray >= maxMicroSpray) {
        this.log("DexMomentum", "microspray_limit_reached", {
          symbol: candidate.symbol,
          current: tierCounts.microspray,
          max: maxMicroSpray,
        });
        continue;
      }
      if (candidate.tier === 'breakout' && tierCounts.breakout >= maxBreakout) {
        this.log("DexMomentum", "breakout_limit_reached", {
          symbol: candidate.symbol,
          current: tierCounts.breakout,
          max: maxBreakout,
        });
        continue;
      }
      if (candidate.tier === 'lottery' && tierCounts.lottery >= maxLotteryPositions) {
        this.log("DexMomentum", "lottery_limit_reached", {
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
            const minScore = this.state.config.dex_chart_min_entry_score ?? 40;

            if (chartAnalysis) {
              this.log("DexMomentum", "chart_analysis", {
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
                this.log("DexMomentum", "skip_bad_chart", {
                  symbol: candidate.symbol,
                  entryScore: chartAnalysis.entryScore,
                  minRequired: minScore,
                  recommendation: chartAnalysis.recommendation,
                  reason: chartAnalysis.patterns.find(p => p.signal === "bearish")?.description || "Low entry score",
                });
                continue; // Skip this candidate
              }
            } else {
              this.log("DexMomentum", "chart_analysis_no_data", {
                symbol: candidate.symbol,
                reason: "Insufficient candle data (token too new)",
              });
            }
        } catch (e) {
          // Chart analysis failed - continue without it (don't block trade)
          this.log("DexMomentum", "chart_analysis_error", {
            symbol: candidate.symbol,
            error: String(e),
          });
        }
      }

      // Calculate position size for this trade (tier-specific)
      let solAmount: number;

      if (candidate.tier === 'microspray') {
        // Micro-spray tier: ultra-tiny position
        solAmount = this.state.config.dex_microspray_position_sol ?? 0.005;
        this.log("DexMomentum", "microspray_tier_sizing", {
          symbol: candidate.symbol,
          tier: "microspray",
          fixedSize: solAmount.toFixed(4) + " SOL",
          ageMinutes: (candidate.ageHours * 60).toFixed(0),
        });
        tierCounts.microspray++;
      } else if (candidate.tier === 'breakout') {
        // Breakout tier: small position for rapid pump plays
        solAmount = this.state.config.dex_breakout_position_sol ?? 0.015;
        this.log("DexMomentum", "breakout_tier_sizing", {
          symbol: candidate.symbol,
          tier: "breakout",
          fixedSize: solAmount.toFixed(4) + " SOL",
          priceChange5m: candidate.priceChange5m?.toFixed(1) + "%",
          ageHours: candidate.ageHours.toFixed(1),
        });
        tierCounts.breakout++;
      } else if (candidate.tier === 'lottery') {
        // Lottery tier: fixed tiny position (lottery ticket)
        solAmount = this.state.config.dex_lottery_position_sol ?? 0.02;
        this.log("DexMomentum", "lottery_tier_sizing", {
          symbol: candidate.symbol,
          tier: "lottery",
          fixedSize: solAmount.toFixed(4) + " SOL",
          ageHours: candidate.ageHours.toFixed(1),
        });
        tierCounts.lottery++;
      } else if (candidate.tier === 'early') {
        // Early tier: reduced position size
        const earlyMultiplier = (this.state.config.dex_early_position_size_pct ?? 50) / 100;
        const tierPctSize = pctSize * earlyMultiplier;
        const calculatedSize = this.state.dexPaperBalance * tierPctSize;
        solAmount = Math.min(calculatedSize, maxCap);
        this.log("DexMomentum", "early_tier_sizing", {
          symbol: candidate.symbol,
          tier: "early",
          normalPct: (pctSize * 100).toFixed(0) + "%",
          adjustedPct: (tierPctSize * 100).toFixed(0) + "%",
          legitimacy: candidate.legitimacyScore,
        });
      } else {
        // Established tier: normal position size
        const calculatedSize = this.state.dexPaperBalance * pctSize;
        solAmount = Math.min(calculatedSize, maxCap);
      }

      if (this.state.dexPaperBalance < minPosition || solAmount < minPosition) break;

      // Calculate total portfolio value for concentration limit check
      let totalPositionValueSol = 0;
      for (const [tokenAddr, pos] of Object.entries(this.state.dexPositions)) {
        const sig = this.state.dexSignals.find(s => s.tokenAddress === tokenAddr);
        const price = sig?.priceUsd || pos.entryPrice;
        const valueUsd = pos.tokenAmount * price;
        totalPositionValueSol += valueUsd / solPriceUsd;
      }
      const totalPortfolioSol = this.state.dexPaperBalance + totalPositionValueSol;

      // Apply position concentration limit (default 40%)
      // Note: This only limits new positions - if a position grows beyond the limit due to gains, that's fine
      const maxConcentrationPct = this.state.config.dex_max_single_position_pct || 40;
      const maxPositionSol = totalPortfolioSol * (maxConcentrationPct / 100);
      let reducedDueToConcentration = false;
      const originalSolAmount = solAmount;

      if (solAmount > maxPositionSol) {
        solAmount = maxPositionSol;
        reducedDueToConcentration = true;
      }

      // Ensure we still have a viable position after concentration limit
      if (solAmount < minPosition) {
        this.log("DexMomentum", "skip_concentration_limit", {
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
      const slippageModel = this.state.config.dex_slippage_model || "realistic";
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
        entryMomentumScore: candidate.momentumScore,  // Track for decay detection (#12)
        entryLiquidity: candidate.liquidity,          // Track for exit safety (#13)
        tier: candidate.tier,                         // Track for tier-specific rules
      };

      this.state.dexPositions[candidate.tokenAddress] = position;
      this.state.dexPaperBalance -= solAmount;

      // Deduct gas fee for the buy transaction
      this.state.dexPaperBalance -= gasFee;
      this.log("DexMomentum", "gas_fee_deducted", {
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
        paperBalance: this.state.dexPaperBalance.toFixed(4) + " SOL remaining",
        url: candidate.url,
        mode: "PAPER TRADING",
      };

      if (reducedDueToConcentration) {
        logData.concentrationLimitApplied = true;
        logData.originalSize = originalSolAmount.toFixed(4) + " SOL";
        logData.reducedTo = solAmount.toFixed(4) + " SOL";
        logData.concentrationLimit = maxConcentrationPct + "%";
        logData.portfolioValue = totalPortfolioSol.toFixed(4) + " SOL";
        this.log("DexMomentum", "paper_buy_reduced", logData);
      } else {
        this.log("DexMomentum", "paper_buy", logData);
      }
    }
  }

  private async recordDexSnapshot(): Promise<void> {
    // Fetch real SOL price (cached for 5 minutes)
    const solPriceUsd = await getSolPriceUsd();
    let positionValueSol = 0;

    for (const [tokenAddress, pos] of Object.entries(this.state.dexPositions)) {
      const signal = this.state.dexSignals.find(s => s.tokenAddress === tokenAddress);
      const currentPrice = signal?.priceUsd || pos.entryPrice;
      const currentValueUsd = pos.tokenAmount * currentPrice;
      positionValueSol += currentValueUsd / solPriceUsd;
    }

    const snapshot: DexPortfolioSnapshot = {
      timestamp: Date.now(),
      totalValueSol: this.state.dexPaperBalance + positionValueSol,
      paperBalanceSol: this.state.dexPaperBalance,
      positionValueSol,
      realizedPnLSol: this.state.dexRealizedPnL,
    };

    // Initialize if needed
    if (!this.state.dexPortfolioHistory) {
      this.state.dexPortfolioHistory = [];
    }

    this.state.dexPortfolioHistory.push(snapshot);

    // Keep last 100 snapshots (roughly 50 minutes at 30s intervals, or longer if running less frequently)
    if (this.state.dexPortfolioHistory.length > 100) {
      this.state.dexPortfolioHistory = this.state.dexPortfolioHistory.slice(-100);
    }

    // ========== Maximum Drawdown Protection (#11) ==========
    const totalValueSol = this.state.dexPaperBalance + positionValueSol;

    // Initialize peak value if not set (use starting balance or current value)
    if (!this.state.dexPeakValue || this.state.dexPeakValue === 0) {
      this.state.dexPeakValue = this.state.config.dex_starting_balance_sol || 1.0;
    }

    // Update peak value (high water mark)
    if (totalValueSol > this.state.dexPeakValue) {
      this.state.dexPeakValue = totalValueSol;
      // Reset drawdown pause if we make new highs
      if (this.state.dexDrawdownPaused) {
        this.state.dexDrawdownPaused = false;
        this.log("DexMomentum", "drawdown_pause_lifted", {
          newPeakValue: totalValueSol.toFixed(4) + " SOL",
          reason: "New high water mark reached",
        });
      }
    }

    // Calculate current drawdown
    const drawdownPct = ((this.state.dexPeakValue - totalValueSol) / this.state.dexPeakValue) * 100;
    const maxDrawdownPct = this.state.config.dex_max_drawdown_pct || 25;

    // Check if drawdown exceeds limit
    if (drawdownPct >= maxDrawdownPct && !this.state.dexDrawdownPaused) {
      this.state.dexDrawdownPaused = true;
      this.log("DexMomentum", "max_drawdown_triggered", {
        currentValue: totalValueSol.toFixed(4) + " SOL",
        peakValue: this.state.dexPeakValue.toFixed(4) + " SOL",
        drawdownPct: drawdownPct.toFixed(1) + "%",
        maxDrawdownPct: maxDrawdownPct + "%",
        action: "New entries paused until recovery",
      });
    }
  }

  private async runCryptoTrading(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    positions: Position[]
  ): Promise<void> {
    if (!this.state.config.crypto_enabled) return;

    const cryptoSymbols = new Set(this.state.config.crypto_symbols || []);
    const cryptoPositions = positions.filter(p => cryptoSymbols.has(p.symbol) || p.symbol.includes("/"));
    const heldCrypto = new Set(cryptoPositions.map(p => p.symbol));

    for (const pos of cryptoPositions) {
      const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100;

      if (plPct >= this.state.config.crypto_take_profit_pct) {
        this.log("Crypto", "take_profit", { symbol: pos.symbol, pnl: plPct.toFixed(2) });
        await this.executeSell(alpaca, pos.symbol, `Crypto take profit at +${plPct.toFixed(1)}%`);
        continue;
      }

      if (plPct <= -this.state.config.crypto_stop_loss_pct) {
        this.log("Crypto", "stop_loss", { symbol: pos.symbol, pnl: plPct.toFixed(2) });
        await this.executeSell(alpaca, pos.symbol, `Crypto stop loss at ${plPct.toFixed(1)}%`);
        continue;
      }
    }

    const maxCryptoPositions = Math.min(this.state.config.crypto_symbols?.length || 3, 3);
    if (cryptoPositions.length >= maxCryptoPositions) return;

    const cryptoSignals = this.state.signalCache
      .filter(s => s.isCrypto)
      .filter(s => !heldCrypto.has(s.symbol))
      .filter(s => s.sentiment > 0)
      .sort((a, b) => (b.momentum || 0) - (a.momentum || 0));

    for (const signal of cryptoSignals.slice(0, 2)) {
      if (cryptoPositions.length >= maxCryptoPositions) break;

      const existingResearch = this.state.signalResearch[signal.symbol];
      const CRYPTO_RESEARCH_TTL_MS = 300_000;

      let research: ResearchResult | null = existingResearch ?? null;
      if (!existingResearch || Date.now() - existingResearch.timestamp > CRYPTO_RESEARCH_TTL_MS) {
        research = await this.researchCrypto(signal.symbol, signal.momentum || 0, signal.sentiment);
      }

      if (!research || research.verdict !== "BUY") {
        this.log("Crypto", "research_skip", {
          symbol: signal.symbol,
          verdict: research?.verdict || "NO_RESEARCH",
          confidence: research?.confidence || 0
        });
        continue;
      }

      if (research.confidence < this.state.config.min_analyst_confidence) {
        this.log("Crypto", "low_confidence", { symbol: signal.symbol, confidence: research.confidence });
        continue;
      }

      const account = await alpaca.trading.getAccount();
      const result = await this.executeCryptoBuy(alpaca, signal.symbol, research.confidence, account);

      if (result) {
        heldCrypto.add(signal.symbol);
        cryptoPositions.push({ symbol: signal.symbol } as Position);
        break;
      }
    }
  }

  private async researchCrypto(
    symbol: string,
    momentum: number,
    sentiment: number
  ): Promise<ResearchResult | null> {
    if (!this._llm) {
      this.log("Crypto", "skipped_no_llm", { symbol, reason: "LLM Provider not configured" });
      return null;
    }

    try {
      const alpaca = createAlpacaProviders(this.env);
      const snapshot = await alpaca.marketData.getCryptoSnapshot(symbol).catch(() => null);
      const price = snapshot?.latest_trade?.price || 0;
      const dailyChange = snapshot ? ((snapshot.daily_bar.c - snapshot.prev_daily_bar.c) / snapshot.prev_daily_bar.c) * 100 : 0;

      const prompt = `Should we BUY this cryptocurrency based on momentum and market conditions?

SYMBOL: ${symbol}
PRICE: $${price.toFixed(2)}
24H CHANGE: ${dailyChange.toFixed(2)}%
MOMENTUM SCORE: ${(momentum * 100).toFixed(0)}%
SENTIMENT: ${(sentiment * 100).toFixed(0)}% bullish

Evaluate if this is a good entry. Consider:
- Is the momentum sustainable or a trap?
- Any major news/events affecting this crypto?
- Risk/reward at current price level?

JSON response:
{
  "verdict": "BUY|SKIP|WAIT",
  "confidence": 0.0-1.0,
  "entry_quality": "excellent|good|fair|poor",
  "reasoning": "brief reason",
  "red_flags": ["any concerns"],
  "catalysts": ["positive factors"]
}`;

      const response = await this._llm.complete({
        model: this.state.config.llm_model, // Use config model (usually cheap one)
        messages: [
          { role: "system", content: "You are a crypto analyst. Be skeptical of FOMO. Crypto is volatile - only recommend BUY for strong setups. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 250,
        temperature: 0.3,
        response_format: { type: "json_object" }
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost(this.state.config.llm_model, usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        verdict: "BUY" | "SKIP" | "WAIT";
        confidence: number;
        entry_quality: "excellent" | "good" | "fair" | "poor";
        reasoning: string;
        red_flags: string[];
        catalysts: string[];
      };

      const result: ResearchResult = {
        symbol,
        verdict: analysis.verdict,
        confidence: analysis.confidence,
        entry_quality: analysis.entry_quality,
        reasoning: analysis.reasoning,
        red_flags: analysis.red_flags || [],
        catalysts: analysis.catalysts || [],
        timestamp: Date.now(),
      };

      this.state.signalResearch[symbol] = result;
      this.log("Crypto", "researched", {
        symbol,
        verdict: result.verdict,
        confidence: result.confidence,
        quality: result.entry_quality,
      });

      return result;
    } catch (error) {
      this.log("Crypto", "research_error", { symbol, error: String(error) });
      return null;
    }
  }

  private async executeCryptoBuy(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    confidence: number,
    account: Account
  ): Promise<boolean> {
    const sizePct = Math.min(20, this.state.config.position_size_pct_of_cash);
    const positionSize = Math.min(
      account.cash * (sizePct / 100) * confidence,
      this.state.config.crypto_max_position_value
    );

    if (positionSize < 10) {
      this.log("Crypto", "buy_skipped", { symbol, reason: "Position too small" });
      return false;
    }

    try {
      const order = await alpaca.trading.createOrder({
        symbol,
        notional: Math.round(positionSize * 100) / 100,
        side: "buy",
        type: "market",
        time_in_force: "gtc",
      });

      this.log("Crypto", "buy_executed", { symbol, status: order.status, size: positionSize });
      return true;
    } catch (error) {
      this.log("Crypto", "buy_failed", { symbol, error: String(error) });
      return false;
    }
  }

  // ============================================================================
  // SECTION 5: TWITTER INTEGRATION
  // ============================================================================
  // [TOGGLE] Enable with TWITTER_BEARER_TOKEN secret
  // [TUNE] MAX_DAILY_READS controls API budget (default: 200/day)
  // 
  // Twitter is used for CONFIRMATION only - it boosts/reduces confidence
  // on signals from other sources, doesn't generate signals itself.
  // ============================================================================

  private isTwitterEnabled(): boolean {
    return !!this.env.TWITTER_BEARER_TOKEN;
  }

  private canSpendTwitterRead(): boolean {
    const ONE_DAY_MS = 86400_000;
    const MAX_DAILY_READS = 200;

    const now = Date.now();
    if (now - this.state.twitterDailyReadReset > ONE_DAY_MS) {
      this.state.twitterDailyReads = 0;
      this.state.twitterDailyReadReset = now;
    }
    return this.state.twitterDailyReads < MAX_DAILY_READS;
  }

  private spendTwitterRead(count = 1): void {
    this.state.twitterDailyReads += count;
    this.log("Twitter", "read_spent", {
      count,
      daily_total: this.state.twitterDailyReads,
      budget_remaining: 200 - this.state.twitterDailyReads,
    });
  }

  private async twitterSearchRecent(query: string, maxResults = 10): Promise<Array<{
    id: string;
    text: string;
    created_at: string;
    author: string;
    author_followers: number;
    retweets: number;
    likes: number;
  }>> {
    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead()) return [];

    try {
      const params = new URLSearchParams({
        query,
        max_results: Math.min(maxResults, 10).toString(),
        "tweet.fields": "created_at,public_metrics,author_id",
        expansions: "author_id",
        "user.fields": "username,public_metrics",
      });

      const res = await fetch(`https://api.twitter.com/2/tweets/search/recent?${params}`, {
        headers: {
          Authorization: `Bearer ${this.env.TWITTER_BEARER_TOKEN}`,
          "Content-Type": "application/json",
        },
      });

      if (!res.ok) {
        this.log("Twitter", "api_error", { status: res.status });
        return [];
      }

      const data = await res.json() as {
        data?: Array<{
          id: string;
          text: string;
          created_at: string;
          author_id: string;
          public_metrics?: { retweet_count?: number; like_count?: number };
        }>;
        includes?: {
          users?: Array<{
            id: string;
            username: string;
            public_metrics?: { followers_count?: number };
          }>;
        };
      };

      this.spendTwitterRead(1);

      return (data.data || []).map(tweet => {
        const user = data.includes?.users?.find(u => u.id === tweet.author_id);
        return {
          id: tweet.id,
          text: tweet.text,
          created_at: tweet.created_at,
          author: user?.username || "unknown",
          author_followers: user?.public_metrics?.followers_count || 0,
          retweets: tweet.public_metrics?.retweet_count || 0,
          likes: tweet.public_metrics?.like_count || 0,
        };
      });
    } catch (error) {
      this.log("Twitter", "error", { message: String(error) });
      return [];
    }
  }

  private async gatherTwitterConfirmation(symbol: string, existingSentiment: number): Promise<TwitterConfirmation | null> {
    const MIN_SENTIMENT_FOR_CONFIRMATION = 0.3;
    const CACHE_TTL_MS = 300_000;

    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead()) return null;
    if (Math.abs(existingSentiment) < MIN_SENTIMENT_FOR_CONFIRMATION) return null;

    const cached = this.state.twitterConfirmations[symbol];
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      return cached;
    }

    const actionableKeywords = ["unusual", "flow", "sweep", "block", "whale", "breaking", "alert", "upgrade", "downgrade"];
    const query = `$${symbol} (${actionableKeywords.slice(0, 5).join(" OR ")}) -is:retweet lang:en`;
    const tweets = await this.twitterSearchRecent(query, 10);

    if (tweets.length === 0) return null;

    let bullish = 0, bearish = 0, totalWeight = 0;
    const highlights: Array<{ author: string; text: string; likes: number }> = [];

    const bullWords = ["buy", "call", "long", "bullish", "upgrade", "beat", "squeeze", "moon", "breakout"];
    const bearWords = ["sell", "put", "short", "bearish", "downgrade", "miss", "crash", "dump", "breakdown"];

    for (const tweet of tweets) {
      const text = tweet.text.toLowerCase();

      const authorWeight = Math.min(1.5, Math.log10(tweet.author_followers + 1) / 5);
      const engagementWeight = Math.min(1.3, 1 + (tweet.likes + tweet.retweets * 2) / 1000);
      const weight = authorWeight * engagementWeight;

      let sentiment = 0;
      for (const w of bullWords) if (text.includes(w)) sentiment += 1;
      for (const w of bearWords) if (text.includes(w)) sentiment -= 1;

      if (sentiment > 0) bullish += weight;
      else if (sentiment < 0) bearish += weight;
      totalWeight += weight;

      if (tweet.likes > 50 || tweet.author_followers > 10000) {
        highlights.push({
          author: tweet.author,
          text: tweet.text.slice(0, 150),
          likes: tweet.likes,
        });
      }
    }

    const twitterSentiment = totalWeight > 0 ? (bullish - bearish) / totalWeight : 0;
    const twitterBullish = twitterSentiment > 0.2;
    const twitterBearish = twitterSentiment < -0.2;
    const existingBullish = existingSentiment > 0;

    const result: TwitterConfirmation = {
      symbol,
      tweet_count: tweets.length,
      sentiment: twitterSentiment,
      confirms_existing: (twitterBullish && existingBullish) || (twitterBearish && !existingBullish),
      highlights: highlights.slice(0, 3),
      timestamp: Date.now(),
    };

    this.state.twitterConfirmations[symbol] = result;
    this.log("Twitter", "signal_confirmed", {
      symbol,
      sentiment: twitterSentiment.toFixed(2),
      confirms: result.confirms_existing,
      tweet_count: tweets.length,
    });

    return result;
  }

  private async checkTwitterBreakingNews(symbols: string[]): Promise<Array<{
    symbol: string;
    headline: string;
    author: string;
    age_minutes: number;
    is_breaking: boolean;
  }>> {
    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead() || symbols.length === 0) return [];

    const toCheck = symbols.slice(0, 3);
    const newsQuery = `(from:FirstSquawk OR from:DeItaone OR from:Newsquawk) (${toCheck.map(s => `$${s}`).join(" OR ")}) -is:retweet`;
    const tweets = await this.twitterSearchRecent(newsQuery, 5);

    const results: Array<{
      symbol: string;
      headline: string;
      author: string;
      age_minutes: number;
      is_breaking: boolean;
    }> = [];

    const MAX_NEWS_AGE_MS = 1800_000;
    const BREAKING_THRESHOLD_MS = 600_000;

    for (const tweet of tweets) {
      const tweetAge = Date.now() - new Date(tweet.created_at).getTime();
      if (tweetAge > MAX_NEWS_AGE_MS) continue;

      const mentionedSymbol = toCheck.find(s =>
        tweet.text.toUpperCase().includes(`$${s}`) ||
        tweet.text.toUpperCase().includes(` ${s} `)
      );

      if (mentionedSymbol) {
        results.push({
          symbol: mentionedSymbol,
          headline: tweet.text.slice(0, 200),
          author: tweet.author,
          age_minutes: Math.round(tweetAge / 60000),
          is_breaking: tweetAge < BREAKING_THRESHOLD_MS,
        });
      }
    }

    if (results.length > 0) {
      this.log("Twitter", "breaking_news_found", {
        count: results.length,
        symbols: results.map(r => r.symbol),
      });
    }

    return results;
  }

  // ============================================================================
  // SECTION 6: LLM RESEARCH
  // ============================================================================
  // [CUSTOMIZABLE] Modify prompts to change how the AI analyzes signals.
  // 
  // Key methods:
  // - researchSignal(): Evaluates individual symbols (BUY/SKIP/WAIT)
  // - researchPosition(): Analyzes held positions (HOLD/SELL/ADD)
  // - analyzeSignalsWithLLM(): Batch analysis for trading decisions
  //
  // [TUNE] Change llm_model and llm_analyst_model in config for cost/quality
  // ============================================================================

  private async researchSignal(
    symbol: string,
    sentimentScore: number,
    sources: string[]
  ): Promise<ResearchResult | null> {
    if (!this._llm) {
      this.log("SignalResearch", "skipped_no_llm", { symbol, reason: "LLM Provider not configured" });
      return null;
    }

    const cached = this.state.signalResearch[symbol];
    const CACHE_TTL_MS = 180_000;
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      return cached;
    }

    try {
      const alpaca = createAlpacaProviders(this.env);
      const isCrypto = isCryptoSymbol(symbol, this.state.config.crypto_symbols || []);
      let price = 0;
      if (isCrypto) {
        const normalized = normalizeCryptoSymbol(symbol);
        const snapshot = await alpaca.marketData.getCryptoSnapshot(normalized).catch(() => null);
        price = snapshot?.latest_trade?.price || snapshot?.latest_quote?.ask_price || snapshot?.latest_quote?.bid_price || 0;
      } else {
        const snapshot = await alpaca.marketData.getSnapshot(symbol).catch(() => null);
        price = snapshot?.latest_trade?.price || snapshot?.latest_quote?.ask_price || snapshot?.latest_quote?.bid_price || 0;
      }

      const prompt = `Should we BUY this ${isCrypto ? "crypto" : "stock"} based on social sentiment and fundamentals?

SYMBOL: ${symbol}
SENTIMENT: ${(sentimentScore * 100).toFixed(0)}% bullish (sources: ${sources.join(", ")})

CURRENT DATA:
- Price: $${price}

Evaluate if this is a good entry. Consider: Is the sentiment justified? Is it too late (already pumped)? Any red flags?

JSON response:
{
  "verdict": "BUY|SKIP|WAIT",
  "confidence": 0.0-1.0,
  "entry_quality": "excellent|good|fair|poor",
  "reasoning": "brief reason",
  "red_flags": ["any concerns"],
  "catalysts": ["positive factors"]
}`;

      const response = await this._llm.complete({
        model: this.state.config.llm_model,
        messages: [
          { role: "system", content: "You are a stock research analyst. Be skeptical of hype. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 250,
        temperature: 0.3,
        response_format: { type: "json_object" }
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost(this.state.config.llm_model, usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        verdict: "BUY" | "SKIP" | "WAIT";
        confidence: number;
        entry_quality: "excellent" | "good" | "fair" | "poor";
        reasoning: string;
        red_flags: string[];
        catalysts: string[];
      };

      const result: ResearchResult = {
        symbol,
        verdict: analysis.verdict,
        confidence: analysis.confidence,
        entry_quality: analysis.entry_quality,
        reasoning: analysis.reasoning,
        red_flags: analysis.red_flags || [],
        catalysts: analysis.catalysts || [],
        timestamp: Date.now(),
      };

      this.state.signalResearch[symbol] = result;
      this.log("SignalResearch", "signal_researched", {
        symbol,
        verdict: result.verdict,
        confidence: result.confidence,
        quality: result.entry_quality,
      });

      if (result.verdict === "BUY") {
        await this.sendDiscordNotification("research", {
          symbol: result.symbol,
          verdict: result.verdict,
          confidence: result.confidence,
          quality: result.entry_quality,
          sentiment: sentimentScore,
          sources,
          reasoning: result.reasoning,
          catalysts: result.catalysts,
          red_flags: result.red_flags,
        });
      }

      return result;
    } catch (error) {
      this.log("SignalResearch", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async researchTopSignals(limit = 5): Promise<ResearchResult[]> {
    const alpaca = createAlpacaProviders(this.env);
    const positions = await alpaca.trading.getPositions();
    const heldSymbols = new Set(positions.map(p => p.symbol));

    const allSignals = this.state.signalCache;
    const notHeld = allSignals.filter(s => !heldSymbols.has(s.symbol));
    // Use raw_sentiment for threshold (before weighting), weighted sentiment for sorting
    const aboveThreshold = notHeld.filter(s => s.raw_sentiment >= this.state.config.min_sentiment_score);
    // Filter out stocks if stocks_enabled is false (crypto-only mode)
    // Default to true if not set (stocks ON by default)
    const stocksEnabled = this.state.config.stocks_enabled ?? true;
    const tradeable = aboveThreshold.filter(s => stocksEnabled || s.isCrypto);
    const candidates = tradeable
      .sort((a, b) => b.sentiment - a.sentiment)
      .slice(0, limit);

    if (candidates.length === 0) {
      this.log("SignalResearch", "no_candidates", {
        total_signals: allSignals.length,
        not_held: notHeld.length,
        above_threshold: aboveThreshold.length,
        tradeable: tradeable.length,
        stocks_enabled: this.state.config.stocks_enabled,
        min_sentiment: this.state.config.min_sentiment_score,
      });
      return [];
    }

    this.log("SignalResearch", "researching_signals", { count: candidates.length });

    const aggregated = new Map<string, { symbol: string; sentiment: number; sources: string[] }>();
    for (const sig of candidates) {
      if (!aggregated.has(sig.symbol)) {
        aggregated.set(sig.symbol, { symbol: sig.symbol, sentiment: sig.sentiment, sources: [sig.source] });
      } else {
        aggregated.get(sig.symbol)!.sources.push(sig.source);
      }
    }

    const results: ResearchResult[] = [];
    for (const [symbol, data] of aggregated) {
      const analysis = await this.researchSignal(symbol, data.sentiment, data.sources);
      if (analysis) {
        results.push(analysis);
      }
      await this.sleep(500);
    }

    return results;
  }

  private async researchPosition(
    symbol: string,
    position: Position
  ): Promise<{
    recommendation: "SELL" | "HOLD" | "ADD";
    risk_level: "low" | "medium" | "high";
    reasoning: string;
    key_factors: string[];
  } | null> {
    if (!this._llm) return null;

    const plPct = (position.unrealized_pl / (position.market_value - position.unrealized_pl)) * 100;

    const prompt = `Analyze this position for risk and opportunity:

POSITION: ${symbol}
- Shares: ${position.qty}
- Market Value: $${position.market_value.toFixed(2)}
- P&L: $${position.unrealized_pl.toFixed(2)} (${plPct.toFixed(1)}%)
- Current Price: $${position.current_price}

Provide a brief risk assessment and recommendation (HOLD, SELL, or ADD). JSON format:
{
  "recommendation": "HOLD|SELL|ADD",
  "risk_level": "low|medium|high",
  "reasoning": "brief reason",
  "key_factors": ["factor1", "factor2"]
}`;

    try {
      const response = await this._llm.complete({
        model: this.state.config.llm_model,
        messages: [
          { role: "system", content: "You are a position risk analyst. Be concise. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 200,
        temperature: 0.3,
        response_format: { type: "json_object" }
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost(this.state.config.llm_model, usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        recommendation: "HOLD" | "SELL" | "ADD";
        risk_level: "low" | "medium" | "high";
        reasoning: string;
        key_factors: string[];
      };

      this.state.positionResearch[symbol] = { ...analysis, timestamp: Date.now() };
      this.log("PositionResearch", "position_analyzed", {
        symbol,
        recommendation: analysis.recommendation,
        risk: analysis.risk_level,
      });

      return analysis;
    } catch (error) {
      this.log("PositionResearch", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async analyzeSignalsWithLLM(
    signals: Signal[],
    positions: Position[],
    account: Account
  ): Promise<{
    recommendations: Array<{
      action: "BUY" | "SELL" | "HOLD";
      symbol: string;
      confidence: number;
      reasoning: string;
      suggested_size_pct?: number;
    }>;
    market_summary: string;
    high_conviction: string[];
  }> {
    if (!this._llm || signals.length === 0) {
      return { recommendations: [], market_summary: "No signals to analyze", high_conviction: [] };
    }

    const aggregated = new Map<string, { symbol: string; sources: string[]; totalSentiment: number; count: number }>();
    for (const sig of signals) {
      if (!aggregated.has(sig.symbol)) {
        aggregated.set(sig.symbol, { symbol: sig.symbol, sources: [], totalSentiment: 0, count: 0 });
      }
      const agg = aggregated.get(sig.symbol)!;
      agg.sources.push(sig.source);
      agg.totalSentiment += sig.sentiment;
      agg.count++;
    }

    const candidates = Array.from(aggregated.values())
      .map(a => ({ ...a, avgSentiment: a.totalSentiment / a.count }))
      .filter(a => a.avgSentiment >= this.state.config.min_sentiment_score * 0.5)
      .sort((a, b) => b.avgSentiment - a.avgSentiment)
      .slice(0, 10);

    if (candidates.length === 0) {
      return { recommendations: [], market_summary: "No candidates above threshold", high_conviction: [] };
    }

    const positionSymbols = new Set(positions.map(p => p.symbol));
    const prompt = `Current Time: ${new Date().toISOString()}

ACCOUNT STATUS:
- Equity: $${account.equity.toFixed(2)}
- Cash: $${account.cash.toFixed(2)}
- Current Positions: ${positions.length}/${this.state.config.max_positions}

CURRENT POSITIONS:
${positions.length === 0 ? "None" : positions.map(p => {
      const entry = this.state.positionEntries[p.symbol];
      const holdMinutes = entry ? Math.round((Date.now() - entry.entry_time) / (1000 * 60)) : 0;
      const holdStr = holdMinutes >= 60 ? `${(holdMinutes / 60).toFixed(1)}h` : `${holdMinutes}m`;
      return `- ${p.symbol}: ${p.qty} shares, P&L: $${p.unrealized_pl.toFixed(2)} (${((p.unrealized_pl / (p.market_value - p.unrealized_pl)) * 100).toFixed(1)}%), held ${holdStr}`;
    }).join("\n")}

TOP SENTIMENT CANDIDATES:
${candidates.map(c =>
      `- ${c.symbol}: avg sentiment ${(c.avgSentiment * 100).toFixed(0)}%, sources: ${c.sources.join(", ")}, ${positionSymbols.has(c.symbol) ? "[CURRENTLY HELD]" : "[NOT HELD]"}`
    ).join("\n")}

RAW SIGNALS (top 20):
${signals.slice(0, 20).map(s =>
      `- ${s.symbol} (${s.source}): ${s.reason}`
    ).join("\n")}

TRADING RULES:
- Max position size: $${this.state.config.max_position_value}
- Take profit target: ${this.state.config.take_profit_pct}%
- Stop loss: ${this.state.config.stop_loss_pct}%
- Min confidence to trade: ${this.state.config.min_analyst_confidence}
- Min hold time before selling: ${this.state.config.llm_min_hold_minutes ?? 30} minutes

Analyze and provide BUY/SELL/HOLD recommendations:`;

    try {
      const response = await this._llm.complete({
        model: this.state.config.llm_analyst_model,
        messages: [
          {
            role: "system",
            content: `You are a senior trading analyst AI. Make the FINAL trading decisions based on social sentiment signals.

Rules:
- Only recommend BUY for symbols with strong conviction from multiple data points
- Recommend SELL only for positions that have been held long enough AND show deteriorating sentiment or major red flags
- Give positions time to develop - avoid selling too early just because gains are small
- Positions held less than 1-2 hours should generally be given more time unless hitting stop loss
- Consider the QUALITY of sentiment, not just quantity
- Output valid JSON only

Response format:
{
  "recommendations": [
    { "action": "BUY"|"SELL"|"HOLD", "symbol": "TICKER", "confidence": 0.0-1.0, "reasoning": "detailed reasoning", "suggested_size_pct": 10-30 }
  ],
  "market_summary": "overall market read and sentiment",
  "high_conviction_plays": ["symbols you feel strongest about"]
}`,
          },
          { role: "user", content: prompt },
        ],
        max_tokens: 800,
        temperature: 0.4,
        response_format: { type: "json_object" }
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost(this.state.config.llm_analyst_model, usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        recommendations: Array<{
          action: "BUY" | "SELL" | "HOLD";
          symbol: string;
          confidence: number;
          reasoning: string;
          suggested_size_pct?: number;
        }>;
        market_summary: string;
        high_conviction_plays?: string[];
      };

      this.log("Analyst", "analysis_complete", {
        candidates: candidates.length,
        recommendations: analysis.recommendations?.length || 0,
      });

      return {
        recommendations: analysis.recommendations || [],
        market_summary: analysis.market_summary || "",
        high_conviction: analysis.high_conviction_plays || [],
      };
    } catch (error) {
      this.log("Analyst", "error", { message: String(error) });
      return { recommendations: [], market_summary: `Analysis failed: ${error}`, high_conviction: [] };
    }
  }

  // ============================================================================
  // SECTION 7: ANALYST & TRADING LOGIC
  // ============================================================================
  // [CUSTOMIZABLE] Core trading decision logic lives here.
  //
  // runAnalyst(): Main trading loop - checks exits, then looks for entries
  // executeBuy(): Position sizing and order execution
  // executeSell(): Closes positions with reason logging
  //
  // [TUNE] Position sizing formula in executeBuy()
  // [TUNE] Entry/exit conditions in runAnalyst()
  // ============================================================================

  private async runAnalyst(): Promise<void> {
    const alpaca = createAlpacaProviders(this.env);

    const [account, positions, clock] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
      alpaca.trading.getClock(),
    ]);

    if (!account || !clock.is_open) {
      this.log("System", "analyst_skipped", { reason: "Account unavailable or market closed" });
      return;
    }

    const heldSymbols = new Set(positions.map(p => p.symbol));

    // Check position exits
    for (const pos of positions) {
      if (pos.asset_class === "us_option") continue;  // Options handled separately

      const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100;

      // Take profit
      if (plPct >= this.state.config.take_profit_pct) {
        await this.executeSell(alpaca, pos.symbol, `Take profit at +${plPct.toFixed(1)}%`);
        continue;
      }

      // Stop loss
      if (plPct <= -this.state.config.stop_loss_pct) {
        await this.executeSell(alpaca, pos.symbol, `Stop loss at ${plPct.toFixed(1)}%`);
        continue;
      }

      // Check staleness
      if (this.state.config.stale_position_enabled) {
        const stalenessResult = this.analyzeStaleness(pos.symbol, pos.current_price, 0);
        this.state.stalenessAnalysis[pos.symbol] = stalenessResult;

        if (stalenessResult.isStale) {
          await this.executeSell(alpaca, pos.symbol, `STALE: ${stalenessResult.reason}`);
        }
      }
    }

    if (positions.length < this.state.config.max_positions && this.state.signalCache.length > 0) {
      // Check if symbol is crypto
      const isCrypto = (symbol: string) => isCryptoSymbol(symbol, this.state.config.crypto_symbols || []);
      // Default to true if not set (stocks ON by default)
      const stocksEnabled = this.state.config.stocks_enabled ?? true;

      const researchedBuys = Object.values(this.state.signalResearch)
        .filter(r => r.verdict === "BUY" && r.confidence >= this.state.config.min_analyst_confidence)
        .filter(r => !heldSymbols.has(r.symbol))
        // Filter out stocks if stocks_enabled is false (crypto-only mode)
        .filter(r => stocksEnabled || isCrypto(r.symbol))
        .sort((a, b) => b.confidence - a.confidence);

      for (const research of researchedBuys.slice(0, 3)) {
        if (positions.length >= this.state.config.max_positions) break;
        if (heldSymbols.has(research.symbol)) continue;

        const originalSignal = this.state.signalCache.find(s => s.symbol === research.symbol);
        let finalConfidence = research.confidence;

        if (this.isTwitterEnabled() && originalSignal) {
          const twitterConfirm = await this.gatherTwitterConfirmation(research.symbol, originalSignal.sentiment);
          if (twitterConfirm?.confirms_existing) {
            finalConfidence = Math.min(1.0, finalConfidence * 1.15);
            this.log("System", "twitter_boost", { symbol: research.symbol, new_confidence: finalConfidence });
          } else if (twitterConfirm && !twitterConfirm.confirms_existing && twitterConfirm.sentiment !== 0) {
            finalConfidence = finalConfidence * 0.85;
          }
        }

        if (finalConfidence < this.state.config.min_analyst_confidence) continue;

        const shouldUseOptions = this.isOptionsEnabled() &&
          finalConfidence >= this.state.config.options_min_confidence &&
          research.entry_quality === "excellent";

        if (shouldUseOptions) {
          const contract = await this.findBestOptionsContract(research.symbol, "bullish", account.equity);
          if (contract) {
            const optionsResult = await this.executeOptionsOrder(contract, 1, account.equity);
            if (optionsResult) {
              this.log("System", "options_position_opened", { symbol: research.symbol, contract: contract.symbol });
            }
          }
        }

        const result = await this.executeBuy(alpaca, research.symbol, finalConfidence, account);
        if (result) {
          heldSymbols.add(research.symbol);
          this.state.positionEntries[research.symbol] = {
            symbol: research.symbol,
            entry_time: Date.now(),
            entry_price: 0,
            entry_sentiment: originalSignal?.sentiment || finalConfidence,
            entry_social_volume: originalSignal?.volume || 0,
            entry_sources: originalSignal?.subreddits || [originalSignal?.source || "research"],
            entry_reason: research.reasoning,
            peak_price: 0,
            peak_sentiment: originalSignal?.sentiment || finalConfidence,
          };
        }
      }

      const analysis = await this.analyzeSignalsWithLLM(this.state.signalCache, positions, account);
      const researchedSymbols = new Set(researchedBuys.map(r => r.symbol));

      for (const rec of analysis.recommendations) {
        if (rec.confidence < this.state.config.min_analyst_confidence) continue;

        if (rec.action === "SELL" && heldSymbols.has(rec.symbol)) {
          const entry = this.state.positionEntries[rec.symbol];
          const holdMinutes = entry ? (Date.now() - entry.entry_time) / (1000 * 60) : 0;
          const minHoldMinutes = this.state.config.llm_min_hold_minutes ?? 30;
          
          if (holdMinutes < minHoldMinutes) {
            this.log("Analyst", "llm_sell_blocked", { 
              symbol: rec.symbol, 
              holdMinutes: Math.round(holdMinutes),
              minRequired: minHoldMinutes,
              reason: "Position held less than minimum hold time"
            });
            continue;
          }
          
          const result = await this.executeSell(alpaca, rec.symbol, `LLM recommendation: ${rec.reasoning}`);
          if (result) {
            heldSymbols.delete(rec.symbol);
            this.log("Analyst", "llm_sell_executed", { symbol: rec.symbol, confidence: rec.confidence, reasoning: rec.reasoning });
          }
          continue;
        }

        if (rec.action === "BUY") {
          if (positions.length >= this.state.config.max_positions) continue;
          if (heldSymbols.has(rec.symbol)) continue;
          if (researchedSymbols.has(rec.symbol)) continue;

          const result = await this.executeBuy(alpaca, rec.symbol, rec.confidence, account);
          if (result) {
            const originalSignal = this.state.signalCache.find(s => s.symbol === rec.symbol);
            heldSymbols.add(rec.symbol);
            this.state.positionEntries[rec.symbol] = {
              symbol: rec.symbol,
              entry_time: Date.now(),
              entry_price: 0,
              entry_sentiment: originalSignal?.sentiment || rec.confidence,
              entry_social_volume: originalSignal?.volume || 0,
              entry_sources: originalSignal?.subreddits || [originalSignal?.source || "analyst"],
              entry_reason: rec.reasoning,
              peak_price: 0,
              peak_sentiment: originalSignal?.sentiment || rec.confidence,
            };
          }
        }
      }
    }
  }

  private async executeBuy(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    confidence: number,
    account: Account
  ): Promise<boolean> {
    // Crisis mode check - block new entries during high alert
    if (this.isCrisisBlockingEntries()) {
      this.log("Executor", "buy_blocked", {
        symbol,
        reason: "CRISIS_MODE_BLOCKING",
        crisisLevel: this.state.crisisState.level,
        triggered: this.state.crisisState.triggeredIndicators,
      });
      return false;
    }

    if (!symbol || symbol.trim().length === 0) {
      this.log("Executor", "buy_blocked", { reason: "INVARIANT: Empty symbol" });
      return false;
    }

    if (account.cash <= 0) {
      this.log("Executor", "buy_blocked", { symbol, reason: "INVARIANT: No cash available", cash: account.cash });
      return false;
    }

    if (confidence <= 0 || confidence > 1 || !Number.isFinite(confidence)) {
      this.log("Executor", "buy_blocked", { symbol, reason: "INVARIANT: Invalid confidence", confidence });
      return false;
    }

    const sizePct = Math.min(20, this.state.config.position_size_pct_of_cash);
    const crisisMultiplier = this.getCrisisPositionMultiplier();
    const positionSize = Math.min(
      account.cash * (sizePct / 100) * confidence * crisisMultiplier,
      this.state.config.max_position_value * crisisMultiplier
    );

    if (crisisMultiplier < 1.0) {
      this.log("Executor", "crisis_size_reduction", {
        symbol,
        crisisLevel: this.state.crisisState.level,
        multiplier: crisisMultiplier,
      });
    }

    if (positionSize < 10) {
      this.log("Executor", "buy_skipped", { symbol, reason: "Position too small" });
      return false;
    }

    const maxAllowed = this.state.config.max_position_value * 1.01;
    if (positionSize <= 0 || positionSize > maxAllowed || !Number.isFinite(positionSize)) {
      this.log("Executor", "buy_blocked", {
        symbol,
        reason: "INVARIANT: Invalid position size",
        positionSize,
        maxAllowed,
      });
      return false;
    }

    try {
      const isCrypto = isCryptoSymbol(symbol, this.state.config.crypto_symbols || []);
      const orderSymbol = isCrypto ? normalizeCryptoSymbol(symbol) : symbol;
      const timeInForce = isCrypto ? "gtc" : "day";

      if (!isCrypto) {
        const allowedExchanges = this.state.config.allowed_exchanges ?? ["NYSE", "NASDAQ", "ARCA", "AMEX", "BATS"];
        if (allowedExchanges.length > 0) {
          const asset = await alpaca.trading.getAsset(symbol);
          if (!asset) {
            this.log("Executor", "buy_blocked", { symbol, reason: "Asset not found" });
            return false;
          }
          if (!allowedExchanges.includes(asset.exchange)) {
            this.log("Executor", "buy_blocked", { 
              symbol, 
              reason: "Exchange not allowed (OTC/foreign stocks have data issues)",
              exchange: asset.exchange,
              allowedExchanges 
            });
            return false;
          }
        }
      }

      const order = await alpaca.trading.createOrder({
        symbol: orderSymbol,
        notional: Math.round(positionSize * 100) / 100,
        side: "buy",
        type: "market",
        time_in_force: timeInForce,
      });

      this.log("Executor", "buy_executed", { symbol: orderSymbol, isCrypto, status: order.status, size: positionSize });
      return true;
    } catch (error) {
      this.log("Executor", "buy_failed", { symbol, error: String(error) });
      return false;
    }
  }

  private async executeSell(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    reason: string
  ): Promise<boolean> {
    if (!symbol || symbol.trim().length === 0) {
      this.log("Executor", "sell_blocked", { reason: "INVARIANT: Empty symbol" });
      return false;
    }

    if (!reason || reason.trim().length === 0) {
      this.log("Executor", "sell_blocked", { symbol, reason: "INVARIANT: No sell reason provided" });
      return false;
    }

    // PDT Protection: Check if this would be a day trade on an account under $25k
    const isCrypto = isCryptoSymbol(symbol, this.state.config.crypto_symbols || []);
    if (!isCrypto) {
      const entry = this.state.positionEntries[symbol];
      if (entry) {
        const entryDate = new Date(entry.entry_time).toDateString();
        const today = new Date().toDateString();
        const isSameDaySell = entryDate === today;

        if (isSameDaySell) {
          try {
            const account = await alpaca.trading.getAccount();
            const PDT_EQUITY_THRESHOLD = 25000;
            const PDT_TRADE_LIMIT = 3;

            if (account.equity < PDT_EQUITY_THRESHOLD && account.daytrade_count >= PDT_TRADE_LIMIT) {
              this.log("Executor", "sell_blocked_pdt", {
                symbol,
                reason: "PDT protection: Would exceed day trade limit",
                equity: account.equity,
                daytrade_count: account.daytrade_count,
                original_reason: reason,
              });
              return false;
            }

            // Warn if approaching PDT limit
            if (account.equity < PDT_EQUITY_THRESHOLD && account.daytrade_count >= 2) {
              this.log("Executor", "pdt_warning", {
                symbol,
                message: `Day trade ${account.daytrade_count + 1}/3 - approaching PDT limit`,
                equity: account.equity,
              });
            }
          } catch (e) {
            // If we can't check account, allow the trade but log warning
            this.log("Executor", "pdt_check_failed", { symbol, error: String(e) });
          }
        }
      }
    }

    try {
      await alpaca.trading.closePosition(symbol);
      this.log("Executor", "sell_executed", { symbol, reason });

      delete this.state.positionEntries[symbol];
      delete this.state.socialHistory[symbol];
      delete this.state.stalenessAnalysis[symbol];

      return true;
    } catch (error) {
      this.log("Executor", "sell_failed", { symbol, error: String(error) });
      return false;
    }
  }

  // ============================================================================
  // SECTION 8: STALENESS DETECTION
  // ============================================================================
  // [TOGGLE] Enable with stale_position_enabled in config
  // [TUNE] Staleness thresholds (hold time, volume decay, gain requirements)
  //
  // Staleness = positions that lost momentum. Scored 0-100 based on:
  // - Time held (vs max hold days)
  // - Price action (P&L vs targets)
  // - Social volume decay (vs entry volume)
  // ============================================================================

  private analyzeStaleness(symbol: string, currentPrice: number, currentSocialVolume: number): {
    isStale: boolean;
    reason: string;
    staleness_score: number;
  } {
    const entry = this.state.positionEntries[symbol];
    if (!entry) {
      return { isStale: false, reason: "No entry data", staleness_score: 0 };
    }

    const holdHours = (Date.now() - entry.entry_time) / (1000 * 60 * 60);
    const holdDays = holdHours / 24;
    const pnlPct = entry.entry_price > 0
      ? ((currentPrice - entry.entry_price) / entry.entry_price) * 100
      : 0;

    if (holdHours < this.state.config.stale_min_hold_hours) {
      return { isStale: false, reason: `Too early (${holdHours.toFixed(1)}h)`, staleness_score: 0 };
    }

    let stalenessScore = 0;

    // Time-based (max 40 points)
    if (holdDays >= this.state.config.stale_max_hold_days) {
      stalenessScore += 40;
    } else if (holdDays >= this.state.config.stale_mid_hold_days) {
      stalenessScore += 20 * (holdDays - this.state.config.stale_mid_hold_days) /
        (this.state.config.stale_max_hold_days - this.state.config.stale_mid_hold_days);
    }

    // Price action (max 30 points)
    if (pnlPct < 0) {
      stalenessScore += Math.min(30, Math.abs(pnlPct) * 3);
    } else if (pnlPct < this.state.config.stale_mid_min_gain_pct && holdDays >= this.state.config.stale_mid_hold_days) {
      stalenessScore += 15;
    }

    // Social volume decay (max 30 points)
    const volumeRatio = entry.entry_social_volume > 0
      ? currentSocialVolume / entry.entry_social_volume
      : 1;
    if (volumeRatio <= this.state.config.stale_social_volume_decay) {
      stalenessScore += 30;
    } else if (volumeRatio <= 0.5) {
      stalenessScore += 15;
    }

    stalenessScore = Math.min(100, stalenessScore);

    const isStale = stalenessScore >= 70 ||
      (holdDays >= this.state.config.stale_max_hold_days && pnlPct < this.state.config.stale_min_gain_pct);

    return {
      isStale,
      reason: isStale
        ? `Staleness score ${stalenessScore}/100, held ${holdDays.toFixed(1)} days`
        : `OK (score ${stalenessScore}/100)`,
      staleness_score: stalenessScore,
    };
  }

  // ============================================================================
  // SECTION 9: OPTIONS TRADING
  // ============================================================================
  // [TOGGLE] Enable with options_enabled in config
  // [TUNE] Delta, DTE, and position size limits in config
  //
  // Options are used for HIGH CONVICTION plays only (confidence >= 0.8).
  // Finds ATM/ITM calls for bullish signals, puts for bearish.
  // Wider stop-loss (50%) and higher take-profit (100%) than stocks.
  // ============================================================================

  private isOptionsEnabled(): boolean {
    return this.state.config.options_enabled === true;
  }

  private async findBestOptionsContract(
    symbol: string,
    direction: "bullish" | "bearish",
    equity: number
  ): Promise<{
    symbol: string;
    strike: number;
    expiration: string;
    delta: number;
    mid_price: number;
    max_contracts: number;
  } | null> {
    if (!this.isOptionsEnabled()) return null;

    try {
      const alpaca = createAlpacaProviders(this.env);
      const expirations = await alpaca.options.getExpirations(symbol);

      if (!expirations || expirations.length === 0) {
        this.log("Options", "no_expirations", { symbol });
        return null;
      }

      const today = new Date();
      const validExpirations = expirations.filter(exp => {
        const expDate = new Date(exp);
        const dte = Math.ceil((expDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        return dte >= this.state.config.options_min_dte && dte <= this.state.config.options_max_dte;
      });

      if (validExpirations.length === 0) {
        this.log("Options", "no_valid_expirations", { symbol });
        return null;
      }

      const targetDTE = (this.state.config.options_min_dte + this.state.config.options_max_dte) / 2;
      const bestExpiration = validExpirations.reduce((best: string, exp: string) => {
        const expDate = new Date(exp);
        const dte = Math.ceil((expDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        const currentBestDte = Math.ceil((new Date(best).getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        return Math.abs(dte - targetDTE) < Math.abs(currentBestDte - targetDTE) ? exp : best;
      }, validExpirations[0]!);

      const chain = await alpaca.options.getChain(symbol, bestExpiration);
      if (!chain) {
        this.log("Options", "chain_failed", { symbol, expiration: bestExpiration });
        return null;
      }

      const contracts = direction === "bullish" ? chain.calls : chain.puts;
      if (!contracts || contracts.length === 0) {
        this.log("Options", "no_contracts", { symbol, direction });
        return null;
      }

      const snapshot = await alpaca.marketData.getSnapshot(symbol).catch(() => null);
      const stockPrice = snapshot?.latest_trade?.price || snapshot?.latest_quote?.ask_price || snapshot?.latest_quote?.bid_price || 0;
      if (stockPrice === 0) return null;

      const targetStrike = direction === "bullish"
        ? stockPrice * (1 - (this.state.config.options_target_delta - 0.5) * 0.2)
        : stockPrice * (1 + (this.state.config.options_target_delta - 0.5) * 0.2);

      const sortedContracts = contracts
        .filter(c => c.strike > 0)
        .sort((a, b) => Math.abs(a.strike - targetStrike) - Math.abs(b.strike - targetStrike));

      for (const contract of sortedContracts.slice(0, 5)) {
        const snapshot = await alpaca.options.getSnapshot(contract.symbol);
        if (!snapshot) continue;

        const delta = snapshot.greeks?.delta;
        const absDelta = delta !== undefined ? Math.abs(delta) : null;

        if (absDelta === null || absDelta < this.state.config.options_min_delta || absDelta > this.state.config.options_max_delta) {
          continue;
        }

        const bid = snapshot.latest_quote?.bid_price || 0;
        const ask = snapshot.latest_quote?.ask_price || 0;
        if (bid === 0 || ask === 0) continue;

        const spread = (ask - bid) / ask;
        if (spread > 0.10) continue;

        const midPrice = (bid + ask) / 2;
        const maxCost = equity * this.state.config.options_max_pct_per_trade;
        const maxContracts = Math.floor(maxCost / (midPrice * 100));

        if (maxContracts < 1) continue;

        this.log("Options", "contract_selected", {
          symbol,
          contract: contract.symbol,
          strike: contract.strike,
          expiration: bestExpiration,
          delta: delta?.toFixed(3),
          mid_price: midPrice.toFixed(2),
        });

        return {
          symbol: contract.symbol,
          strike: contract.strike,
          expiration: bestExpiration,
          delta: delta!,
          mid_price: midPrice,
          max_contracts: maxContracts,
        };
      }

      return null;
    } catch (error) {
      this.log("Options", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async executeOptionsOrder(
    contract: { symbol: string; mid_price: number },
    quantity: number,
    equity: number
  ): Promise<boolean> {
    if (!this.isOptionsEnabled()) return false;

    const totalCost = contract.mid_price * quantity * 100;
    const maxAllowed = equity * this.state.config.options_max_pct_per_trade;

    if (totalCost > maxAllowed) {
      quantity = Math.floor(maxAllowed / (contract.mid_price * 100));
      if (quantity < 1) {
        this.log("Options", "skipped_size", { contract: contract.symbol, cost: totalCost, max: maxAllowed });
        return false;
      }
    }

    try {
      const alpaca = createAlpacaProviders(this.env);
      const order = await alpaca.trading.createOrder({
        symbol: contract.symbol,
        qty: quantity,
        side: "buy",
        type: "limit",
        limit_price: Math.round(contract.mid_price * 100) / 100,
        time_in_force: "day",
      });

      this.log("Options", "options_buy_executed", {
        contract: contract.symbol,
        qty: quantity,
        status: order.status,
        estimated_cost: (contract.mid_price * quantity * 100).toFixed(2),
      });

      return true;
    } catch (error) {
      this.log("Options", "options_buy_failed", { contract: contract.symbol, error: String(error) });
      return false;
    }
  }

  private async checkOptionsExits(positions: Position[]): Promise<Array<{
    symbol: string;
    reason: string;
    type: string;
    pnl_pct: number;
  }>> {
    if (!this.isOptionsEnabled()) return [];

    const exits: Array<{ symbol: string; reason: string; type: string; pnl_pct: number }> = [];
    const optionsPositions = positions.filter(p => p.asset_class === "us_option");

    for (const pos of optionsPositions) {
      const entryPrice = pos.avg_entry_price || pos.current_price;
      const plPct = entryPrice > 0 ? ((pos.current_price - entryPrice) / entryPrice) * 100 : 0;

      if (plPct <= -this.state.config.options_stop_loss_pct) {
        exits.push({
          symbol: pos.symbol,
          reason: `Options stop loss at ${plPct.toFixed(1)}%`,
          type: "stop_loss",
          pnl_pct: plPct,
        });
        continue;
      }

      if (plPct >= this.state.config.options_take_profit_pct) {
        exits.push({
          symbol: pos.symbol,
          reason: `Options take profit at +${plPct.toFixed(1)}%`,
          type: "take_profit",
          pnl_pct: plPct,
        });
        continue;
      }
    }

    return exits;
  }

  // ============================================================================
  // SECTION 10: PRE-MARKET ANALYSIS
  // ============================================================================
  // Runs 9:25-9:29 AM ET to prepare a trading plan before market open.
  // Executes the plan at 9:30-9:32 AM when market opens.
  //
  // [TUNE] Change time windows in isPreMarketWindow() / isMarketJustOpened()
  // [TUNE] Plan staleness (PLAN_STALE_MS) in executePremarketPlan()
  // ============================================================================

  private isPreMarketWindow(): boolean {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const day = now.getDay();

    if (day >= 1 && day <= 5) {
      if (hour === 9 && minute >= 25 && minute <= 29) {
        return true;
      }
    }
    return false;
  }

  private isMarketJustOpened(): boolean {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const day = now.getDay();

    if (day >= 1 && day <= 5) {
      if (hour === 9 && minute >= 30 && minute <= 32) {
        return true;
      }
    }
    return false;
  }

  private async runPreMarketAnalysis(): Promise<void> {
    const alpaca = createAlpacaProviders(this.env);
    const [account, positions] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
    ]);

    if (!account || this.state.signalCache.length === 0) return;

    this.log("System", "premarket_analysis_starting", {
      signals: this.state.signalCache.length,
      researched: Object.keys(this.state.signalResearch).length,
    });

    const signalResearch = await this.researchTopSignals(10);
    const analysis = await this.analyzeSignalsWithLLM(this.state.signalCache, positions, account);

    this.state.premarketPlan = {
      timestamp: Date.now(),
      recommendations: analysis.recommendations.map(r => ({
        action: r.action,
        symbol: r.symbol,
        confidence: r.confidence,
        reasoning: r.reasoning,
        suggested_size_pct: r.suggested_size_pct,
      })),
      market_summary: analysis.market_summary,
      high_conviction: analysis.high_conviction,
      researched_buys: signalResearch.filter(r => r.verdict === "BUY"),
    };

    const buyRecs = this.state.premarketPlan.recommendations.filter(r => r.action === "BUY").length;
    const sellRecs = this.state.premarketPlan.recommendations.filter(r => r.action === "SELL").length;

    this.log("System", "premarket_analysis_complete", {
      buy_recommendations: buyRecs,
      sell_recommendations: sellRecs,
      high_conviction: this.state.premarketPlan.high_conviction,
    });
  }

  private async executePremarketPlan(): Promise<void> {
    const PLAN_STALE_MS = 600_000;

    if (!this.state.premarketPlan || Date.now() - this.state.premarketPlan.timestamp > PLAN_STALE_MS) {
      this.log("System", "no_premarket_plan", { reason: "Plan missing or stale" });
      return;
    }

    const alpaca = createAlpacaProviders(this.env);
    const [account, positions] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
    ]);

    if (!account) return;

    const heldSymbols = new Set(positions.map(p => p.symbol));

    this.log("System", "executing_premarket_plan", {
      recommendations: this.state.premarketPlan.recommendations.length,
    });

    for (const rec of this.state.premarketPlan.recommendations) {
      if (rec.action === "SELL" && rec.confidence >= this.state.config.min_analyst_confidence) {
        await this.executeSell(alpaca, rec.symbol, `Pre-market plan: ${rec.reasoning}`);
      }
    }

    for (const rec of this.state.premarketPlan.recommendations) {
      if (rec.action === "BUY" && rec.confidence >= this.state.config.min_analyst_confidence) {
        if (heldSymbols.has(rec.symbol)) continue;
        if (positions.length >= this.state.config.max_positions) break;

        const result = await this.executeBuy(alpaca, rec.symbol, rec.confidence, account);
        if (result) {
          heldSymbols.add(rec.symbol);

          const originalSignal = this.state.signalCache.find(s => s.symbol === rec.symbol);
          this.state.positionEntries[rec.symbol] = {
            symbol: rec.symbol,
            entry_time: Date.now(),
            entry_price: 0,
            entry_sentiment: originalSignal?.sentiment || 0,
            entry_social_volume: originalSignal?.volume || 0,
            entry_sources: originalSignal?.subreddits || [originalSignal?.source || "premarket"],
            entry_reason: rec.reasoning,
            peak_price: 0,
            peak_sentiment: originalSignal?.sentiment || 0,
          };
        }
      }
    }

    this.state.premarketPlan = null;
  }

  // ============================================================================
  // SECTION 10.5: CRISIS MODE - BLACK SWAN PROTECTION
  // ============================================================================
  // Monitor market stress indicators and protect portfolio during crises.
  // Runs alongside normal trading - you keep making money until crisis hits,
  // then auto-protective measures kick in based on severity level.
  // ============================================================================

  /**
   * Run crisis indicator check - called periodically from alarm handler
   * Fetches indicators, evaluates crisis level, and takes protective actions
   */
  private async runCrisisCheck(): Promise<void> {
    const config = this.state.config;
    if (!config.crisis_mode_enabled) return;

    // Manual override check
    if (this.state.crisisState.manualOverride) {
      this.log("Crisis", "manual_override_active", {
        level: this.state.crisisState.level,
      });
      return;
    }

    const now = Date.now();
    const checkInterval = config.crisis_check_interval_ms || 300_000;

    // Only check if enough time has passed
    if (now - this.state.lastCrisisCheck < checkInterval) {
      return;
    }

    this.log("Crisis", "checking_indicators", {});

    try {
      // Fetch all indicators concurrently (pass FRED API key if available)
      const fredApiKey = (this.env as unknown as Record<string, string>).FRED_API_KEY;
      const indicators = await fetchCrisisIndicators(fredApiKey);
      this.state.crisisState.indicators = indicators;
      this.state.lastCrisisCheck = now;

      // Evaluate crisis level
      const { level, triggeredIndicators } = evaluateCrisisLevel(indicators, config);
      const previousLevel = this.state.crisisState.level;

      // Update state
      this.state.crisisState.level = level;
      this.state.crisisState.triggeredIndicators = triggeredIndicators;

      // Log level changes
      if (level !== previousLevel) {
        this.state.crisisState.lastLevelChange = now;
        const levelNames = ["NORMAL", "ELEVATED", "HIGH ALERT", "FULL CRISIS"];
        this.log("Crisis", "level_changed", {
          previous: levelNames[previousLevel],
          current: levelNames[level],
          triggered: triggeredIndicators,
          indicators: {
            vix: indicators.vix,
            hySpread: indicators.highYieldSpread,
            btc: indicators.btcPrice,
            btcWeekly: indicators.btcWeeklyChange,
            usdt: indicators.stablecoinPeg,
            gsRatio: indicators.goldSilverRatio,
          },
        });

        // Send Discord notification for significant level changes
        if (level >= 2 || (level === 1 && previousLevel === 0)) {
          await this.sendCrisisDiscordNotification(
            `🚨 CRISIS LEVEL: ${levelNames[level]}`,
            `Triggered indicators:\n${triggeredIndicators.join("\n")}`,
            level >= 2 ? 0xFF0000 : 0xFFA500 // Red for crisis, orange for elevated
          );
        }
      }

      // Log current status
      this.log("Crisis", "status", {
        level,
        triggered: triggeredIndicators.length,
        vix: indicators.vix?.toFixed(1) ?? "N/A",
        btc: indicators.btcPrice?.toFixed(0) ?? "N/A",
      });

    } catch (error) {
      this.log("Crisis", "check_error", { error: String(error) });
    }
  }

  /**
   * Check if crisis mode is blocking new entries
   * Returns true if new positions should NOT be opened
   */
  private isCrisisBlockingEntries(): boolean {
    if (!this.state.config.crisis_mode_enabled) return false;
    if (this.state.crisisState.manualOverride) return false;

    // Level 2+ blocks new entries
    return this.state.crisisState.level >= 2;
  }

  /**
   * Check if crisis mode is blocking ALL trading (full crisis)
   * Returns true if we should close all positions immediately
   */
  private isCrisisFullPanic(): boolean {
    if (!this.state.config.crisis_mode_enabled) return false;
    if (this.state.crisisState.manualOverride) return false;

    // Level 3 = full crisis, close everything
    return this.state.crisisState.level >= 3;
  }

  /**
   * Get adjusted position size for current crisis level
   * Returns multiplier (0.0 to 1.0) to apply to position sizes
   */
  private getCrisisPositionMultiplier(): number {
    if (!this.state.config.crisis_mode_enabled) return 1.0;
    if (this.state.crisisState.manualOverride) return 1.0;

    const level = this.state.crisisState.level;
    switch (level) {
      case 0: return 1.0;       // Normal - full size
      case 1: return 0.5;       // Elevated - half size
      case 2: return 0.0;       // High alert - no new positions
      case 3: return 0.0;       // Full crisis - no new positions
      default: return 1.0;
    }
  }

  /**
   * Get adjusted stop loss for current crisis level
   * Returns tighter stop loss percentage during elevated risk
   */
  /**
   * Get adjusted stop loss for current crisis level
   * Returns tighter stop loss percentage during elevated risk
   * @internal Reserved for future integration with position management
   */
  public getCrisisAdjustedStopLoss(normalStopLoss: number): number {
    if (!this.state.config.crisis_mode_enabled) return normalStopLoss;
    if (this.state.crisisState.manualOverride) return normalStopLoss;

    const level = this.state.crisisState.level;
    const config = this.state.config;

    switch (level) {
      case 0: return normalStopLoss;
      case 1: return Math.min(normalStopLoss, config.crisis_level1_stop_loss_pct);
      case 2: return Math.min(normalStopLoss, config.crisis_level1_stop_loss_pct * 0.8); // Even tighter
      case 3: return 0; // Sell immediately
      default: return normalStopLoss;
    }
  }

  /**
   * Execute crisis protection actions based on current level
   * Called after crisis check detects elevated levels
   */
  private async executeCrisisActions(alpaca: ReturnType<typeof createAlpacaProviders>): Promise<void> {
    const level = this.state.crisisState.level;
    if (level === 0) return;

    const levelNames = ["NORMAL", "ELEVATED", "HIGH ALERT", "FULL CRISIS"];
    this.log("Crisis", "executing_actions", { level: levelNames[level] });

    try {
      const positions = await alpaca.trading.getPositions();

      if (level >= 3) {
        // FULL CRISIS: Close ALL positions immediately
        this.log("Crisis", "full_crisis_liquidation", {
          positions: positions.length,
        });

        for (const pos of positions) {
          try {
            await this.executeSell(alpaca, pos.symbol, "CRISIS_LEVEL_3_LIQUIDATION");
            this.state.crisisState.positionsClosedInCrisis.push(pos.symbol);
          } catch (err) {
            this.log("Crisis", "liquidation_error", { symbol: pos.symbol, error: String(err) });
          }
        }

        // Also close DEX positions
        await this.closeAllDexPositions("CRISIS_LEVEL_3_LIQUIDATION");

        await this.sendCrisisDiscordNotification(
          "🚨 FULL CRISIS - ALL POSITIONS LIQUIDATED",
          `Closed ${positions.length} stock positions and all DEX positions`,
          0xFF0000
        );

      } else if (level >= 2) {
        // HIGH ALERT: Close losing positions, keep winners with trailing stops
        const config = this.state.config;
        const minProfitToHold = config.crisis_level2_min_profit_to_hold;

        for (const pos of positions) {
          const plPct = pos.unrealized_plpc * 100;

          if (plPct < minProfitToHold) {
            this.log("Crisis", "closing_underwater_position", {
              symbol: pos.symbol,
              plPct: plPct.toFixed(2),
              threshold: minProfitToHold,
            });

            try {
              await this.executeSell(alpaca, pos.symbol, `CRISIS_LEVEL_2_UNDERWATER_${plPct.toFixed(1)}PCT`);
              this.state.crisisState.positionsClosedInCrisis.push(pos.symbol);
            } catch (err) {
              this.log("Crisis", "close_error", { symbol: pos.symbol, error: String(err) });
            }
          }
        }
      }
      // Level 1: Just reduces position sizes and tightens stops (handled elsewhere)

    } catch (error) {
      this.log("Crisis", "action_error", { error: String(error) });
    }
  }

  /**
   * Close all DEX positions during crisis
   */
  private async closeAllDexPositions(reason: string): Promise<void> {
    const positions = Object.values(this.state.dexPositions);
    if (positions.length === 0) return;

    this.log("Crisis", "closing_all_dex_positions", {
      count: positions.length,
      reason,
    });

    for (const pos of positions) {
      // Find current signal for price
      const signal = this.state.dexSignals.find(s => s.tokenAddress === pos.tokenAddress);
      const currentPrice = signal?.priceUsd ?? pos.entryPrice;

      const pnlPct = ((currentPrice - pos.entryPrice) / pos.entryPrice) * 100;
      const exitValue = (currentPrice / pos.entryPrice) * pos.entrySol;
      const pnlSol = exitValue - pos.entrySol;

      // Record trade
      this.state.dexTradeHistory.push({
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
      this.state.dexPaperBalance += exitValue;
      this.state.dexRealizedPnL += pnlSol;

      // Remove position
      delete this.state.dexPositions[pos.tokenAddress];

      this.log("Crisis", "dex_position_closed", {
        symbol: pos.symbol,
        pnlPct: pnlPct.toFixed(2),
        pnlSol: pnlSol.toFixed(4),
        reason,
      });
    }
  }

  // ============================================================================
  // SECTION 11: UTILITIES
  // ============================================================================
  // Logging, cost tracking, persistence, and Discord notifications.
  // Generally don't need to modify unless adding new notification channels.
  // ============================================================================

  private log(agent: string, action: string, details: Record<string, unknown>): void {
    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      agent,
      action,
      ...details,
    };
    this.state.logs.push(entry);

    // Keep last 500 logs
    if (this.state.logs.length > 500) {
      this.state.logs = this.state.logs.slice(-500);
    }

    // Log to console for wrangler tail
    console.log(`[${entry.timestamp}] [${agent}] ${action}`, JSON.stringify(details));
  }

  public trackLLMCost(model: string, tokensIn: number, tokensOut: number): number {
    const pricing: Record<string, { input: number; output: number }> = {
      "gpt-4o": { input: 2.5, output: 10 },
      "gpt-4o-mini": { input: 0.15, output: 0.6 },
    };

    const rates = pricing[model] ?? pricing["gpt-4o"]!;
    const cost = (tokensIn * rates.input + tokensOut * rates.output) / 1_000_000;

    this.state.costTracker.total_usd += cost;
    this.state.costTracker.calls++;
    this.state.costTracker.tokens_in += tokensIn;
    this.state.costTracker.tokens_out += tokensOut;

    return cost;
  }

  private async persist(): Promise<void> {
    await this.ctx.storage.put("state", this.state);
  }

  private jsonResponse(data: unknown): Response {
    return new Response(JSON.stringify(data, null, 2), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  get llm(): LLMProvider | null {
    return this._llm;
  }

  private discordCooldowns: Map<string, number> = new Map();
  private readonly DISCORD_COOLDOWN_MS = 30 * 60 * 1000;

  private async sendDiscordNotification(
    type: "signal" | "research",
    data: {
      symbol: string;
      sentiment?: number;
      sources?: string[];
      verdict?: string;
      confidence?: number;
      quality?: string;
      reasoning?: string;
      catalysts?: string[];
      red_flags?: string[];
    }
  ): Promise<void> {
    if (!this.env.DISCORD_WEBHOOK_URL) return;

    const cacheKey = data.symbol;
    const lastNotification = this.discordCooldowns.get(cacheKey);
    if (lastNotification && Date.now() - lastNotification < this.DISCORD_COOLDOWN_MS) {
      return;
    }

    try {
      let embed: {
        title: string;
        color: number;
        fields: Array<{ name: string; value: string; inline: boolean }>;
        description?: string;
        timestamp: string;
        footer: { text: string };
      };

      if (type === "signal") {
        embed = {
          title: `🔔 SIGNAL: $${data.symbol}`,
          color: 0xfbbf24,
          fields: [
            { name: "Sentiment", value: `${((data.sentiment || 0) * 100).toFixed(0)}% bullish`, inline: true },
            { name: "Sources", value: data.sources?.join(", ") || "StockTwits", inline: true },
          ],
          description: "High sentiment detected, researching...",
          timestamp: new Date().toISOString(),
          footer: { text: "MAHORAGA • Not financial advice • DYOR" },
        };
      } else {
        const verdictEmoji = data.verdict === "BUY" ? "✅" : data.verdict === "SKIP" ? "⏭️" : "⏸️";
        const color = data.verdict === "BUY" ? 0x22c55e : data.verdict === "SKIP" ? 0x6b7280 : 0xfbbf24;

        embed = {
          title: `${verdictEmoji} $${data.symbol} → ${data.verdict}`,
          color,
          fields: [
            { name: "Confidence", value: `${((data.confidence || 0) * 100).toFixed(0)}%`, inline: true },
            { name: "Quality", value: data.quality || "N/A", inline: true },
            { name: "Sentiment", value: `${((data.sentiment || 0) * 100).toFixed(0)}%`, inline: true },
          ],
          timestamp: new Date().toISOString(),
          footer: { text: "MAHORAGA • Not financial advice • DYOR" },
        };

        if (data.reasoning) {
          embed.description = data.reasoning.substring(0, 300) + (data.reasoning.length > 300 ? "..." : "");
        }

        if (data.catalysts && data.catalysts.length > 0) {
          embed.fields.push({ name: "Catalysts", value: data.catalysts.slice(0, 3).join(", "), inline: false });
        }

        if (data.red_flags && data.red_flags.length > 0) {
          embed.fields.push({ name: "⚠️ Red Flags", value: data.red_flags.slice(0, 3).join(", "), inline: false });
        }
      }

      await fetch(this.env.DISCORD_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ embeds: [embed] }),
      });

      this.discordCooldowns.set(cacheKey, Date.now());
      this.log("Discord", "notification_sent", { type, symbol: data.symbol });
    } catch (err) {
      this.log("Discord", "notification_failed", { error: String(err) });
    }
  }

  /**
   * Send a crisis-specific Discord notification with custom title/description/color
   */
  private async sendCrisisDiscordNotification(
    title: string,
    description: string,
    color: number
  ): Promise<void> {
    if (!this.env.DISCORD_WEBHOOK_URL) return;

    // Rate limit crisis notifications to once per 5 minutes per crisis level
    const cacheKey = `crisis_${this.state.crisisState.level}`;
    const lastNotification = this.discordCooldowns.get(cacheKey);
    if (lastNotification && Date.now() - lastNotification < 5 * 60 * 1000) {
      return;
    }

    try {
      const embed = {
        title,
        description,
        color,
        fields: [
          { name: "Crisis Level", value: String(this.state.crisisState.level), inline: true },
          { name: "VIX", value: this.state.crisisState.indicators.vix?.toFixed(1) ?? "N/A", inline: true },
          { name: "BTC", value: this.state.crisisState.indicators.btcPrice ? `$${this.state.crisisState.indicators.btcPrice.toFixed(0)}` : "N/A", inline: true },
        ],
        timestamp: new Date().toISOString(),
        footer: { text: "MAHORAGA Crisis Monitor" },
      };

      await fetch(this.env.DISCORD_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ embeds: [embed] }),
      });

      this.discordCooldowns.set(cacheKey, Date.now());
      this.log("Discord", "crisis_notification_sent", { title, level: this.state.crisisState.level });
    } catch (err) {
      this.log("Discord", "crisis_notification_failed", { error: String(err) });
    }
  }
}

// ============================================================================
// SECTION 12: EXPORTS & HELPERS
// ============================================================================
// Helper functions to interact with the DO from your worker.
// ============================================================================

export function getHarnessStub(env: Env): DurableObjectStub {
  if (!env.MAHORAGA_HARNESS) {
    throw new Error("MAHORAGA_HARNESS binding not configured - check wrangler.toml");
  }
  const id = env.MAHORAGA_HARNESS.idFromName("main");
  return env.MAHORAGA_HARNESS.get(id);
}

export async function getHarnessStatus(env: Env): Promise<unknown> {
  const stub = getHarnessStub(env);
  const response = await stub.fetch(new Request("http://harness/status"));
  return response.json();
}

export async function enableHarness(env: Env): Promise<void> {
  const stub = getHarnessStub(env);
  await stub.fetch(new Request("http://harness/enable"));
}

export async function disableHarness(env: Env): Promise<void> {
  const stub = getHarnessStub(env);
  await stub.fetch(new Request("http://harness/disable"));
}
