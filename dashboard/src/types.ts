export interface Account {
  equity: number
  cash: number
  buying_power: number
  portfolio_value: number
}

export interface Position {
  symbol: string
  qty: number
  side: string
  market_value: number
  unrealized_pl: number
  current_price: number
}

export interface Clock {
  is_open: boolean
  next_open: string
  next_close: string
}

export interface Signal {
  symbol: string
  source: string
  sentiment: number
  volume: number
  reason: string
  bullish?: number
  bearish?: number
  score?: number
  upvotes?: number
  isCrypto?: boolean
  momentum?: number
  price?: number
}

export interface LogEntry {
  timestamp: string
  agent: string
  action: string
  symbol?: string
  [key: string]: unknown
}

export interface CostTracker {
  total_usd: number
  calls: number
  tokens_in: number
  tokens_out: number
}

export interface Config {
  mcp_url: string
  data_poll_interval_ms: number
  analyst_interval_ms: number
  max_position_value: number
  max_positions: number
  min_sentiment_score: number
  min_analyst_confidence: number
  sell_sentiment_threshold: number
  take_profit_pct: number
  stop_loss_pct: number
  position_size_pct_of_cash: number
  llm_provider?: 'openai-raw' | 'ai-sdk' | 'cloudflare-gateway'
  llm_model: string
  llm_analyst_model?: string
  llm_max_tokens: number
  starting_equity?: number

  // Stale position management
  stale_position_enabled?: boolean
  stale_min_hold_hours?: number
  stale_max_hold_days?: number
  stale_min_gain_pct?: number
  stale_mid_hold_days?: number
  stale_mid_min_gain_pct?: number
  stale_social_volume_decay?: number
  stale_no_mentions_hours?: number

  // Options config
  options_enabled?: boolean
  options_min_confidence?: number
  options_max_pct_per_trade?: number
  options_max_total_exposure?: number
  options_min_dte?: number
  options_max_dte?: number
  options_target_delta?: number
  options_min_delta?: number
  options_max_delta?: number
  options_stop_loss_pct?: number
  options_take_profit_pct?: number
  options_max_positions?: number

  // Crypto trading config (24/7)
  crypto_enabled?: boolean
  crypto_symbols?: string[]
  crypto_momentum_threshold?: number
  crypto_max_position_value?: number
  crypto_take_profit_pct?: number
  crypto_stop_loss_pct?: number

  // Custom ticker blacklist (insider trading restrictions, etc.)
  ticker_blacklist?: string[]

  // Stock trading toggle (disable for crypto-only mode)
  stocks_enabled?: boolean

  // DEX Momentum Trading (Solana gems via DexScreener/Jupiter)
  dex_enabled?: boolean
  dex_starting_balance_sol?: number
  // Multi-tier system with toggles
  // Micro-spray (30min-2h) - ultra-tiny bets [TOGGLE]
  dex_microspray_enabled?: boolean       // Enable micro-spray tier
  dex_microspray_position_sol?: number   // Ultra-tiny position (default 0.005 SOL)
  dex_microspray_max_positions?: number  // Max concurrent micro-spray positions (default 10)
  // Breakout (2-6h) - detect rapid 5-min pumps [TOGGLE]
  dex_breakout_enabled?: boolean         // Enable breakout tier
  dex_breakout_min_5m_pump?: number      // Minimum 5-min pump % to trigger (default 50)
  dex_breakout_position_sol?: number     // Position size (default 0.015 SOL)
  dex_breakout_max_positions?: number    // Max concurrent breakout positions (default 5)
  // Lottery (1-6h) - current working tier
  dex_lottery_enabled?: boolean          // Enable lottery tier
  dex_lottery_min_age_hours?: number     // Min age in hours (default 1)
  dex_lottery_max_age_hours?: number     // Max age in hours (default 6)
  dex_lottery_min_liquidity?: number     // Min liquidity (default $15k)
  dex_lottery_position_sol?: number      // Fixed tiny position size in SOL (default 0.02)
  dex_lottery_max_positions?: number     // Max concurrent lottery positions (default 5)
  dex_lottery_trailing_activation?: number // Auto-enable trailing stop at this gain % (default 100)
  // Tier 1: Early gems (6h-3d)
  dex_early_min_age_days?: number      // Tier 1: Early gems (default 0.25 = 6 hours)
  dex_early_max_age_days?: number      // Tier 1: Max age (default 3 days)
  dex_early_min_liquidity?: number     // Tier 1: Lower liquidity ok (default $30k)
  dex_early_min_legitimacy?: number    // Tier 1: Must have socials/website (default 40)
  dex_early_position_size_pct?: number // Tier 1: Smaller positions (default 50% of normal)
  // Tier 2: Established (3-14d)
  dex_established_min_age_days?: number // Tier 2: Established (default 3 days)
  dex_established_max_age_days?: number // Tier 2: Max age (default 14 days)
  dex_established_min_liquidity?: number // Tier 2: Higher liquidity (default $50k)
  // Legacy (still used as fallbacks)
  dex_min_age_days?: number
  dex_max_age_days?: number
  dex_min_liquidity?: number
  dex_min_volume_24h?: number
  dex_min_price_change?: number
  dex_max_position_sol?: number
  dex_position_size_pct?: number
  dex_take_profit_pct?: number
  dex_stop_loss_pct?: number
  dex_max_positions?: number
  dex_slippage_model?: 'none' | 'conservative' | 'realistic'
  dex_gas_fee_sol?: number
  dex_circuit_breaker_losses?: number
  dex_circuit_breaker_window_hours?: number
  dex_circuit_breaker_pause_hours?: number
  dex_max_drawdown_pct?: number
  dex_max_single_position_pct?: number
  dex_reentry_recovery_pct?: number
  dex_reentry_min_momentum?: number
  dex_breaker_min_cooldown_minutes?: number

  // Crisis Mode - Black Swan Protection
  crisis_mode_enabled?: boolean
  crisis_vix_elevated?: number
  crisis_vix_high?: number
  crisis_vix_critical?: number
  crisis_hy_spread_warning?: number
  crisis_hy_spread_critical?: number
  crisis_btc_breakdown_price?: number
  crisis_btc_weekly_drop_pct?: number
  crisis_stocks_above_200ma_warning?: number
  crisis_stocks_above_200ma_critical?: number
  crisis_stablecoin_depeg_threshold?: number
  crisis_gold_silver_ratio_low?: number
  crisis_check_interval_ms?: number
  crisis_level1_position_reduction?: number
  crisis_level1_stop_loss_pct?: number
  crisis_level2_min_profit_to_hold?: number

  // New expanded crisis thresholds
  crisis_yield_curve_inversion_warning?: number
  crisis_yield_curve_inversion_critical?: number
  crisis_ted_spread_warning?: number
  crisis_ted_spread_critical?: number
  crisis_dxy_elevated?: number
  crisis_dxy_critical?: number
  crisis_usdjpy_warning?: number
  crisis_usdjpy_critical?: number
  crisis_kre_weekly_warning?: number
  crisis_kre_weekly_critical?: number
  crisis_silver_weekly_warning?: number
  crisis_silver_weekly_critical?: number
  crisis_fed_balance_change_warning?: number
  crisis_fed_balance_change_critical?: number
}

export interface DexPosition {
  tokenAddress: string
  symbol: string
  entryPrice: number
  entrySol: number
  entryTime: number
  tokenAmount: number
  peakPrice: number
  currentPrice: number
  currentValue: number
  unrealizedPl: number
  unrealizedPlPct: number
  holdingHours: number
  entryMomentumScore?: number  // Track entry momentum for decay detection (#12)
  entryLiquidity?: number      // Track entry liquidity for exit safety (#13)
  tier?: 'microspray' | 'breakout' | 'lottery' | 'early' | 'established'  // Track for tier-specific rules
}

export interface DexMomentumSignal {
  symbol: string
  tokenAddress: string
  pairAddress: string
  name: string
  priceUsd: number
  priceChange24h: number
  priceChange6h: number
  priceChange1h: number
  priceChange5m: number
  volume24h: number
  volume6h: number
  volume1h: number
  liquidity: number
  marketCap: number
  ageHours: number
  ageDays: number
  buyRatio24h: number
  buyRatio1h: number
  txnCount24h: number
  momentumScore: number
  dexId: string
  url: string
  // Multi-tier system
  tier: 'microspray' | 'breakout' | 'lottery' | 'early' | 'established'
  legitimacyScore: number
  legitimacySignals: {
    hasWebsite: boolean
    hasTwitter: boolean
    hasTelegram: boolean
    boostCount: number
    sellsExist: boolean
  }
}

export interface SignalResearch {
  verdict: 'BUY' | 'SKIP' | 'WAIT'
  confidence: number
  entry_quality: 'excellent' | 'good' | 'fair' | 'poor'
  reasoning: string
  red_flags: string[]
  catalysts: string[]
  sentiment: number
  timestamp: number
}

export interface PositionResearch {
  recommendation: 'HOLD' | 'SELL' | 'ADD'
  risk_level: 'low' | 'medium' | 'high'
  reasoning: string
  key_factors: string[]
  timestamp: number
}

export interface PositionEntry {
  symbol: string
  entry_time: number
  entry_price: number
  entry_sentiment: number
  entry_social_volume: number
  entry_sources: string[]
  entry_reason: string
  peak_price: number
  peak_sentiment: number
}

export interface TwitterConfirmation {
  symbol: string
  query: string
  tweetCount: number
  sentiment: number
  bullishCount: number
  bearishCount: number
  influencerMentions: number
  averageEngagement: number
  timestamp: number
}

export interface PremarketPlan {
  timestamp: number
  summary: string
  recommendations: Array<{
    symbol: string
    action: 'BUY' | 'SELL' | 'HOLD' | 'SKIP'
    confidence: number
    reasoning: string
    entry_price?: number
    target_price?: number
    stop_loss?: number
  }>
  highConvictionPlays: string[]
  marketOutlook: string
}

export interface StalenessAnalysis {
  symbol: string
  score: number
  holdDays: number
  gainPct: number
  socialVolumeDecay: number
  shouldExit: boolean
  reasons: string[]
}

export interface OvernightActivity {
  signalsGathered: number
  signalsResearched: number
  buySignals: number
  twitterConfirmations: number
  premarketPlanReady: boolean
  lastUpdated: number
}

export interface PortfolioSnapshot {
  timestamp: number
  equity: number
  pl: number
  pl_pct: number
}

export interface PositionHistory {
  symbol: string
  prices: number[]
  timestamps: number[]
}

export interface Status {
  account: Account | null
  positions: Position[]
  clock: Clock | null
  config: Config
  signals: Signal[]
  logs: LogEntry[]
  costs: CostTracker
  lastAnalystRun: number
  lastResearchRun: number
  signalResearch: Record<string, SignalResearch>
  positionResearch: Record<string, PositionResearch>
  portfolioHistory?: PortfolioSnapshot[]
  positionHistory?: Record<string, PositionHistory>
  positionEntries?: Record<string, PositionEntry>
  twitterConfirmations?: Record<string, TwitterConfirmation>
  premarketPlan?: PremarketPlan | null
  stalenessAnalysis?: Record<string, StalenessAnalysis>
  overnightActivity?: OvernightActivity
  // DEX momentum trading
  dexPositions?: DexPosition[]
  dexSignals?: DexMomentumSignal[]
  dexPaperTrading?: {
    enabled: boolean
    paperBalance: number
    realizedPnL: number
    totalTrades: number
    winningTrades: number
    losingTrades: number
    recentTrades: Array<{
      symbol: string
      tokenAddress: string
      entryPrice: number
      exitPrice: number
      entrySol: number
      entryTime: number
      exitTime: number
      pnlPct: number
      pnlSol: number
      exitReason: 'take_profit' | 'stop_loss' | 'lost_momentum' | 'manual'
    }>
    // Trading metrics (#15, #16, #17)
    winRate: number
    avgWinPct: number
    avgLossPct: number
    expectancy: number
    profitFactor: number
    sharpeRatio: number
    maxConsecutiveLosses: number
    currentLossStreak: number
    maxDrawdownPct: number
    maxDrawdownDuration: number
    currentDrawdownPct: number
  }
  dexPortfolioHistory?: DexPortfolioSnapshot[]
  // Crisis Mode
  crisisState?: CrisisState
  lastCrisisCheck?: number
}

export interface DexPortfolioSnapshot {
  timestamp: number
  totalValueSol: number
  paperBalanceSol: number
  positionValueSol: number
  realizedPnLSol: number
}

// Crisis Mode Types
export type CrisisLevel = 0 | 1 | 2 | 3

export interface CrisisIndicators {
  // Volatility & Fear
  vix: number | null

  // Credit Markets
  highYieldSpread: number | null
  yieldCurve2Y10Y: number | null        // 2Y/10Y Treasury spread (negative = inverted = recession)
  tedSpread: number | null              // TED spread (LIBOR - T-bill, banking stress)

  // Crypto (risk indicator, not safe haven)
  btcPrice: number | null
  btcWeeklyChange: number | null
  stablecoinPeg: number | null

  // Currency & Dollar
  dxy: number | null                    // Dollar Index (spike = risk-off)
  usdJpy: number | null                 // USD/JPY (yen carry trade unwind signal)

  // Banking Stress
  kre: number | null                    // Regional Bank ETF price
  kreWeeklyChange: number | null        // Regional Bank ETF weekly % change

  // Precious Metals
  goldSilverRatio: number | null
  silverWeeklyChange: number | null     // Silver weekly % change (momentum)

  // Market Breadth
  stocksAbove200MA: number | null

  // Fed & Liquidity (from FRED)
  fedBalanceSheet: number | null        // Fed balance sheet in trillions
  fedBalanceSheetChange: number | null  // Weekly change in Fed balance sheet

  lastUpdated: number
}

export interface CrisisState {
  level: CrisisLevel
  indicators: CrisisIndicators
  triggeredIndicators: string[]
  pausedUntil: number | null
  lastLevelChange: number
  positionsClosedInCrisis: string[]
  manualOverride: boolean
}
