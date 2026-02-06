/**
 * Default configuration for MahoragaHarness
 *
 * This file contains the default configuration values for the trading agent.
 */

import type { AgentConfig, AgentState, CrisisIndicators } from "./types";

// ============================================================================
// DEFAULT CONFIGURATION
// ============================================================================

export const DEFAULT_CONFIG: AgentConfig = {
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
  options_max_total_exposure: 0.1,
  options_min_dte: 30,
  options_max_dte: 60,
  options_target_delta: 0.45,
  options_min_delta: 0.3,
  options_max_delta: 0.7,
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
  dex_microspray_enabled: false, // Toggle OFF - enable when ready to test
  dex_microspray_position_sol: 0.005, // 0.005 SOL per position (~$0.50)
  dex_microspray_max_positions: 10, // Spray up to 10 micro positions
  // Breakout - detect rapid 5-min pumps [OFF by default]
  dex_breakout_enabled: false, // Toggle OFF - enable when ready to test
  dex_breakout_min_5m_pump: 50, // Must be up 50%+ in 5 minutes
  dex_breakout_position_sol: 0.015, // 0.015 SOL per position (~$1.50)
  dex_breakout_max_positions: 5, // Max 5 breakout positions
  // Lottery - current working tier
  dex_lottery_enabled: true,
  dex_lottery_min_age_hours: 1,
  dex_lottery_max_age_hours: 6,
  dex_lottery_min_liquidity: 15000,
  dex_lottery_position_sol: 0.02,
  dex_lottery_max_positions: 5,
  dex_lottery_trailing_activation: 100,
  dex_lottery_stop_loss_pct: 35, // Stop loss for lottery tier
  // Microspray tier (ultra-small positions)
  dex_microspray_stop_loss_pct: 35, // Stop loss for microspray tier
  // Breakout tier
  dex_breakout_stop_loss_pct: 35, // Stop loss for breakout tier
  // Tier 1: Early Gems
  dex_early_min_age_days: 0.25, // 6 hours minimum (after lottery window)
  dex_early_max_age_days: 3, // Tier 1 ends at 3 days
  dex_early_min_liquidity: 30000, // $30k minimum for early tier
  dex_early_min_legitimacy: 40, // Must have website OR socials (40 points)
  dex_early_position_size_pct: 50, // Use 50% of normal position size (higher risk)
  dex_early_stop_loss_pct: 35, // Stop loss for early tier
  // Tier 2: Established
  dex_established_min_age_days: 3, // Tier 2 starts at 3 days
  dex_established_max_age_days: 14, // Tier 2 ends at 14 days
  dex_established_min_liquidity: 50000, // $50k minimum for established tier

  // Legacy config (still used as fallbacks)
  dex_min_age_days: 1, // Skip very new tokens (<24h)
  dex_max_age_days: 90, // Expanded range to catch more momentum
  dex_min_liquidity: 20000, // Lower threshold for small caps
  dex_min_volume_24h: 5000, // Lower volume threshold
  dex_min_price_change: 3, // Lower price change threshold
  dex_max_position_sol: 1.0, // Max cap per position
  dex_position_size_pct: 33, // Use ~1/3 of balance per position (divides across max_positions)
  dex_take_profit_pct: 100, // Take profit at 100% - let winners run
  dex_stop_loss_pct: 35, // Stop loss at 35% - survive meme coin volatility (was 30%, too tight)
  dex_max_positions: 3,
  dex_slippage_model: "realistic", // Simulate realistic DEX slippage
  dex_gas_fee_sol: 0.005, // ~$1 gas fee per trade at $200/SOL
  dex_circuit_breaker_losses: 3, // Pause after 3 stop losses
  dex_circuit_breaker_window_hours: 24, // Within 24 hours
  dex_circuit_breaker_pause_hours: 1, // Pause for 1 hour (was 6) - shorter cooldown
  dex_max_drawdown_pct: 35, // Pause trading at 35% drawdown (was 25%)
  dex_max_single_position_pct: 40, // Max 40% of portfolio in one token
  dex_stop_loss_cooldown_hours: 2, // Fallback time cooldown (used if price check unavailable)
  dex_reentry_recovery_pct: 15, // Re-enter when price is 15% above exit price
  dex_reentry_min_momentum: 70, // OR re-enter when momentum score > 70
  dex_breaker_min_cooldown_minutes: 30, // Minimum 30 min pause before breaker can clear
  dex_trailing_stop_enabled: true, // Enable trailing stop loss
  dex_trailing_stop_activation_pct: 50, // Trailing stop activates after 50% gain (let it run first)
  dex_trailing_stop_distance_pct: 25, // Trailing stop is 25% below peak (room for pullbacks)
  // Scaling trailing stop - earlier activation with proportional protection
  dex_scaling_trailing_enabled: true, // Enable scaling trailing stop (activates at +10%)
  dex_scaling_trailing_activation_pct: 10, // Activate at +10% gain
  dex_scaling_max_drawdown_pct: 45, // Max drawdown from peak capped at 45%
  // Chart pattern analysis defaults
  dex_chart_analysis_enabled: true, // Enable Birdeye chart analysis before entry
  dex_chart_min_entry_score: 40, // Minimum entry score (0-100) - avoid worst setups
  // Crisis Mode defaults
  crisis_mode_enabled: true, // Crisis detection enabled by default
  crisis_vix_elevated: 25, // VIX above 25 = elevated risk
  crisis_vix_high: 35, // VIX above 35 = high alert
  crisis_vix_critical: 45, // VIX above 45 = full crisis
  crisis_hy_spread_warning: 400, // HY spread above 400bps = warning
  crisis_hy_spread_critical: 600, // HY spread above 600bps = crisis
  crisis_btc_weekly_drop_pct: -20, // BTC down 20%+ in a week = red flag (% is the real signal, not absolute price)
  crisis_stocks_above_200ma_warning: 30, // Less than 30% above 200MA = warning
  crisis_stocks_above_200ma_critical: 20, // Less than 20% above 200MA = crisis
  crisis_stablecoin_depeg_threshold: 0.985, // USDT below $0.985 = crisis
  crisis_gold_silver_ratio_low: 60, // G/S ratio below 60 = monetary crisis signal
  crisis_check_interval_ms: 300_000, // Check every 5 minutes
  crisis_level1_position_reduction: 50, // Reduce position sizes by 50%
  crisis_level1_stop_loss_pct: 5, // Tighter stop loss
  crisis_level2_min_profit_to_hold: 2, // Need 2% profit to hold at level 2

  // New expanded crisis indicators defaults
  crisis_yield_curve_inversion_warning: 0.25, // Yield curve below 25bps = flattening warning
  crisis_yield_curve_inversion_critical: -0.5, // Yield curve below -50bps = recession warning
  crisis_ted_spread_warning: 0.5, // TED spread above 50bps = banking stress
  crisis_ted_spread_critical: 1.0, // TED spread above 100bps = banking crisis
  crisis_dxy_elevated: 105, // Dollar index above 105 = risk-off mode
  crisis_dxy_critical: 110, // Dollar index above 110 = flight to safety
  crisis_usdjpy_warning: 145, // Yen strengthening below 145 = carry unwind starts (current ~150)
  crisis_usdjpy_critical: 138, // Yen below 138 = carry trade blowing up
  crisis_kre_weekly_warning: -10, // Regional banks down 10%/week = stress
  crisis_kre_weekly_critical: -20, // Regional banks down 20%/week = crisis
  crisis_silver_weekly_warning: 10, // Silver up 10%/week = monetary concerns
  crisis_silver_weekly_critical: 20, // Silver up 20%/week = monetary crisis
  crisis_fed_balance_change_warning: 2, // Fed balance sheet change 2%/week = intervention
  crisis_fed_balance_change_critical: 5, // Fed balance sheet change 5%/week = emergency
};

// ============================================================================
// DEFAULT CRISIS INDICATORS
// ============================================================================

export const DEFAULT_CRISIS_INDICATORS: CrisisIndicators = {
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
};

// ============================================================================
// DEFAULT STATE
// ============================================================================

export const DEFAULT_STATE: AgentState = {
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
    indicators: DEFAULT_CRISIS_INDICATORS,
    triggeredIndicators: [],
    pausedUntil: null,
    lastLevelChange: 0,
    positionsClosedInCrisis: [],
    manualOverride: false,
  },
  lastCrisisCheck: 0,
};

// ============================================================================
// TICKER BLACKLIST - Common English words and trading slang to filter out
// ============================================================================

export const TICKER_BLACKLIST = new Set([
  // Finance/trading terms
  "CEO",
  "CFO",
  "COO",
  "CTO",
  "IPO",
  "EPS",
  "GDP",
  "SEC",
  "FDA",
  "USA",
  "USD",
  "ETF",
  "NYSE",
  "API",
  "ATH",
  "ATL",
  "IMO",
  "FOMO",
  "YOLO",
  "DD",
  "TA",
  "FA",
  "ROI",
  "PE",
  "PB",
  "PS",
  "EV",
  "DCF",
  "WSB",
  "RIP",
  "LOL",
  "OMG",
  "WTF",
  "FUD",
  "HODL",
  "APE",
  "MOASS",
  "DRS",
  "NFT",
  "DAO",
  // Common English words (2-4 letters that look like tickers)
  "THE",
  "AND",
  "FOR",
  "ARE",
  "BUT",
  "NOT",
  "YOU",
  "ALL",
  "CAN",
  "HER",
  "WAS",
  "ONE",
  "OUR",
  "OUT",
  "DAY",
  "HAD",
  "HAS",
  "HIS",
  "HOW",
  "ITS",
  "LET",
  "MAY",
  "NEW",
  "NOW",
  "OLD",
  "SEE",
  "WAY",
  "WHO",
  "BOY",
  "DID",
  "GET",
  "HIM",
  "HIT",
  "LOW",
  "MAN",
  "RUN",
  "SAY",
  "SHE",
  "TOO",
  "USE",
  "DAD",
  "MOM",
  "GOT",
  "PUT",
  "SAW",
  "SAT",
  "SET",
  "SIT",
  "TRY",
  "THAT",
  "THIS",
  "WITH",
  "HAVE",
  "FROM",
  "THEY",
  "BEEN",
  "CALL",
  "WILL",
  "EACH",
  "MAKE",
  "LIKE",
  "TIME",
  "JUST",
  "KNOW",
  "TAKE",
  "COME",
  "MADE",
  "FIND",
  "MORE",
  "LONG",
  "HERE",
  "MANY",
  "SOME",
  "THAN",
  "THEM",
  "THEN",
  "ONLY",
  "OVER",
  "SUCH",
  "YEAR",
  "INTO",
  "MOST",
  "ALSO",
  "BACK",
  "GOOD",
  "WELL",
  "EVEN",
  "WANT",
  "GIVE",
  "MUCH",
  "WORK",
  "FIRST",
  "AFTER",
  "AS",
  "AT",
  "BE",
  "BY",
  "DO",
  "GO",
  "IF",
  "IN",
  "IS",
  "IT",
  "MY",
  "NO",
  "OF",
  "ON",
  "OR",
  "SO",
  "TO",
  "UP",
  "US",
  "WE",
  "AN",
  "AM",
  "AH",
  "OH",
  "OK",
  "HI",
  "YA",
  "YO",
  // More trading slang
  "BULL",
  "BEAR",
  "PUTS",
  "HOLD",
  "SELL",
  "MOON",
  "PUMP",
  "DUMP",
  "BAGS",
  "TEND",
  // Additional common words that appear as false positives
  "START",
  "ABOUT",
  "NAME",
  "NEXT",
  "PLAY",
  "LIVE",
  "GAME",
  "BEST",
  "LINK",
  "READ",
  "POST",
  "NEWS",
  "FREE",
  "LOOK",
  "HELP",
  "OPEN",
  "FULL",
  "VIEW",
  "REAL",
  "SEND",
  "HIGH",
  "DROP",
  "FAST",
  "SAFE",
  "RISK",
  "TURN",
  "PLAN",
  "DEAL",
  "MOVE",
  "HUGE",
  "EASY",
  "HARD",
  "LATE",
  "WAIT",
  "SOON",
  "STOP",
  "EXIT",
  "GAIN",
  "LOSS",
  "GROW",
  "FALL",
  "JUMP",
  "KEEP",
  "COPY",
  "EDIT",
  "SAVE",
  "NOTE",
  "TIPS",
  "IDEA",
  "PLUS",
  "ZERO",
  "SELF",
  "BOTH",
  "BETA",
  "TEST",
  "INFO",
  "DATA",
  "CASH",
  "WHAT",
  "WHEN",
  "WHERE",
  "WHY",
  "WATCH",
  "LOVE",
  "HATE",
  "TECH",
  "HOPE",
  "FEAR",
  "WEEK",
  "LAST",
  "PART",
  "SIDE",
  "STEP",
  "SURE",
  "TELL",
  "THINK",
  "TOLD",
  "TRUE",
  "TYPE",
  "UNIT",
  "USED",
  "VERY",
  "WENT",
  "WERE",
  "YEAH",
  "YOUR",
  "ELSE",
  "AWAY",
  "OTHER",
  "PRICE",
  "THEIR",
  "STILL",
  "CHEAP",
  "THESE",
  "LEAP",
  "EVERY",
  "SINCE",
  "BEING",
  "THOSE",
  "DOING",
  "COULD",
  "WOULD",
  "SHOULD",
  "MIGHT",
  "MUST",
  "SHALL",
]);
