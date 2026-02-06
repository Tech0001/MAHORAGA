/**
 * Crisis Mode - Black Swan Protection System
 *
 * Monitors market stress indicators and auto-protects portfolio during crises.
 * Runs alongside normal trading so you can profit during calm periods while
 * being ready to protect capital when the black swans arrive.
 */

import type { CrisisLevel, CrisisIndicators, AgentConfig } from "./types";

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
 * Fetch stocks above 200-day moving average (market breadth indicator)
 * Uses a proxy: calculates % of major sector ETFs above their 200MA
 * Low readings = weak market breadth = bearish signal
 */
async function fetchStocksAbove200MA(): Promise<number | null> {
  try {
    // Use 11 sector ETFs as proxy for market breadth
    const sectorETFs = ["XLK", "XLF", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC"];

    const results = await Promise.all(
      sectorETFs.map(async (symbol) => {
        try {
          const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=250d`;
          const resp = await fetch(url, {
            headers: { "User-Agent": "Mozilla/5.0 (compatible; MAHORAGA/1.0)" }
          });
          if (!resp.ok) return null;

          const data = await resp.json() as {
            chart?: { result?: Array<{
              meta?: { regularMarketPrice?: number };
              indicators?: { quote?: Array<{ close?: (number | null)[] }> }
            }> }
          };

          const result = data.chart?.result?.[0];
          const currentPrice = result?.meta?.regularMarketPrice;
          const closes = result?.indicators?.quote?.[0]?.close;

          if (!currentPrice || !closes || closes.length < 200) return null;

          // Calculate 200-day moving average
          const validCloses = closes.filter((c): c is number => c != null && c > 0);
          if (validCloses.length < 200) return null;

          const last200 = validCloses.slice(-200);
          const ma200 = last200.reduce((a, b) => a + b, 0) / 200;

          return currentPrice > ma200;
        } catch {
          return null;
        }
      })
    );

    // Calculate percentage above 200MA
    const validResults = results.filter((r): r is boolean => r !== null);
    if (validResults.length < 6) return null; // Need at least 6 sectors for meaningful reading

    const aboveCount = validResults.filter(r => r).length;
    const pctAbove = (aboveCount / validResults.length) * 100;

    return pctAbove;
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
export async function fetchCrisisIndicators(fredApiKey?: string): Promise<CrisisIndicators> {
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
    stocksAbove200MA,
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
    fetchStocksAbove200MA(),
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

    // Market Breadth (uses sector ETF proxy)
    stocksAbove200MA,

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
export function evaluateCrisisLevel(
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
