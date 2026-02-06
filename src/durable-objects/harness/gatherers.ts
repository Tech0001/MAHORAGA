/**
 * Data Gatherers Module
 *
 * Extracted data gathering functions from MahoragaHarness.
 * Handles fetching signals from StockTwits, Reddit, crypto markets, and DEX momentum.
 */

import type { HarnessContext } from "./context";
import type { Signal } from "./types";
import { SOURCE_CONFIG } from "./types";
import {
  calculateTimeDecay,
  getEngagementMultiplier,
  getFlairMultiplier,
  extractTickers,
  detectSentiment,
  tickerCache,
} from "./utils";
import { createAlpacaProviders } from "../../providers/alpaca";
import { createDexScreenerProvider } from "../../providers/dexscreener";

/**
 * Helper function for sleeping (used for rate limiting).
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Run all data gatherers and update signal cache.
 * Orchestrates StockTwits, Reddit, and crypto data gathering.
 */
export async function runDataGatherers(ctx: HarnessContext): Promise<void> {
  ctx.log("System", "gathering_data", {});

  await tickerCache.refreshSecTickersIfNeeded();

  const [stocktwitsSignals, redditSignals, cryptoSignals] = await Promise.all([
    gatherStockTwits(ctx),
    gatherReddit(ctx),
    gatherCrypto(ctx),
  ]);

  const allSignals = [...stocktwitsSignals, ...redditSignals, ...cryptoSignals];

  const MAX_SIGNALS = 200;
  const MAX_AGE_MS = 24 * 60 * 60 * 1000;
  const now = Date.now();

  const freshSignals = allSignals
    .filter(s => now - s.timestamp < MAX_AGE_MS)
    .sort((a, b) => Math.abs(b.sentiment) - Math.abs(a.sentiment))
    .slice(0, MAX_SIGNALS);

  ctx.state.signalCache = freshSignals;

  ctx.log("System", "data_gathered", {
    stocktwits: stocktwitsSignals.length,
    reddit: redditSignals.length,
    crypto: cryptoSignals.length,
    total: ctx.state.signalCache.length,
  });
}

/**
 * Gather signals from StockTwits trending symbols.
 */
export async function gatherStockTwits(ctx: HarnessContext): Promise<Signal[]> {
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
          await sleep(1000 * Math.pow(2, i));
          continue;
        }
        return null;
      } catch {
        await sleep(1000 * Math.pow(2, i));
      }
    }
    return null;
  };

  try {
    const trendingRes = await fetchWithRetry("https://api.stocktwits.com/api/2/trending/symbols.json");
    if (!trendingRes) {
      ctx.log("StockTwits", "cloudflare_blocked", {
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

        await sleep(200);
      } catch {
        continue;
      }
    }
  } catch (error) {
    ctx.log("StockTwits", "error", { message: String(error) });
  }

  return signals;
}

/**
 * Gather signals from Reddit trading subreddits.
 */
export async function gatherReddit(ctx: HarnessContext): Promise<Signal[]> {
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
        const tickers = extractTickers(text, ctx.state.config.ticker_blacklist);
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

      await sleep(1000);
    } catch {
      continue;
    }
  }

  const signals: Signal[] = [];
  const alpaca = createAlpacaProviders(ctx.env);

  for (const [symbol, data] of tickerData) {
    if (data.mentions >= 2) {
      if (!tickerCache.isKnownSecTicker(symbol)) {
        const cached = tickerCache.getCachedValidation(symbol);
        if (cached === false) continue;
        if (cached === undefined) {
          const isValid = await tickerCache.validateWithAlpaca(symbol, alpaca);
          if (!isValid) {
            ctx.log("Reddit", "invalid_ticker_filtered", { symbol });
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

/**
 * Gather signals from crypto markets based on momentum.
 */
export async function gatherCrypto(ctx: HarnessContext): Promise<Signal[]> {
  if (!ctx.state.config.crypto_enabled) return [];

  const signals: Signal[] = [];
  const symbols = ctx.state.config.crypto_symbols || ["BTC/USD", "ETH/USD", "SOL/USD"];
  const alpaca = createAlpacaProviders(ctx.env);

  for (const symbol of symbols) {
    try {
      const snapshot = await alpaca.marketData.getCryptoSnapshot(symbol);
      if (!snapshot) continue;

      const price = snapshot.latest_trade?.price || 0;
      const prevClose = snapshot.prev_daily_bar?.c || 0;

      if (!price || !prevClose) continue;

      const momentum = ((price - prevClose) / prevClose) * 100;
      const threshold = ctx.state.config.crypto_momentum_threshold || 2.0;
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

      await sleep(200);
    } catch (error) {
      ctx.log("Crypto", "error", { symbol, message: String(error) });
    }
  }

  ctx.log("Crypto", "gathered_signals", { count: signals.length });
  return signals;
}

/**
 * Gather momentum signals from Solana DEXs via DexScreener.
 * Finds tokens aged 3-14 days with proven momentum (not brand new rugs).
 */
export async function gatherDexMomentum(ctx: HarnessContext): Promise<void> {
  if (!ctx.state.config.dex_enabled) return;

  const SCAN_INTERVAL_MS = 30_000; // 30 seconds between scans
  if (Date.now() - ctx.state.lastDexScanRun < SCAN_INTERVAL_MS) return;

  try {
    const dexScreener = createDexScreenerProvider();

    const signals = await dexScreener.findMomentumTokens({
      // Multi-tier system config
      // Micro-spray (30min-2h) [TOGGLE]
      microSprayEnabled: ctx.state.config.dex_microspray_enabled ?? false,
      microSprayMinAgeMinutes: 30,
      microSprayMaxAgeHours: 2,
      microSprayMinLiquidity: 10000,
      // Breakout (2-6h) [TOGGLE]
      breakoutEnabled: ctx.state.config.dex_breakout_enabled ?? false,
      breakoutMinAgeHours: 2,
      breakoutMaxAgeHours: 6,
      breakoutMinLiquidity: 15000,
      breakoutMin5mPump: ctx.state.config.dex_breakout_min_5m_pump ?? 50,
      // Lottery (current working tier)
      lotteryEnabled: ctx.state.config.dex_lottery_enabled ?? true,
      lotteryMinAgeHours: ctx.state.config.dex_lottery_min_age_hours ?? 1,
      lotteryMaxAgeHours: ctx.state.config.dex_lottery_max_age_hours ?? 6,
      lotteryMinLiquidity: ctx.state.config.dex_lottery_min_liquidity ?? 15000,
      lotteryMinVolume: 5000,
      // Early tier
      earlyMinAgeDays: ctx.state.config.dex_early_min_age_days ?? 0.25,
      earlyMaxAgeDays: ctx.state.config.dex_early_max_age_days ?? 3,
      earlyMinLiquidity: ctx.state.config.dex_early_min_liquidity ?? 30000,
      earlyMinLegitimacyScore: ctx.state.config.dex_early_min_legitimacy ?? 40,
      // Established tier
      establishedMinAgeDays: ctx.state.config.dex_established_min_age_days ?? ctx.state.config.dex_min_age_days ?? 3,
      establishedMaxAgeDays: ctx.state.config.dex_established_max_age_days ?? ctx.state.config.dex_max_age_days ?? 14,
      establishedMinLiquidity: ctx.state.config.dex_established_min_liquidity ?? ctx.state.config.dex_min_liquidity ?? 50000,
      // Shared filters
      minVolume24h: ctx.state.config.dex_min_volume_24h,
      minPriceChange24h: ctx.state.config.dex_min_price_change,
    });

    // Preserve signals for tokens that have open positions (don't drop price data)
    const openPositionAddresses = new Set(Object.keys(ctx.state.dexPositions));
    const newSignalAddresses = new Set(signals.map(s => s.tokenAddress));

    // Keep old signals for open positions that aren't in the new scan
    const preservedSignals = ctx.state.dexSignals.filter(s =>
      openPositionAddresses.has(s.tokenAddress) && !newSignalAddresses.has(s.tokenAddress)
    );

    if (preservedSignals.length > 0) {
      ctx.log("DexMomentum", "preserving_signals_for_positions", {
        preserved: preservedSignals.map(s => s.symbol),
        reason: "Token has open position but not in current scan",
      });
    }

    // Refresh prices for open positions that aren't in the new scan
    if (preservedSignals.length > 0) {
      try {
        const addressesToRefresh = preservedSignals.map(s => s.tokenAddress);
        const freshPairs = await dexScreener.getMultipleTokens("solana", addressesToRefresh);

        // Update preserved signals with fresh prices
        for (const sig of preservedSignals) {
          const freshPair = freshPairs.find(p => p.baseToken?.address === sig.tokenAddress);
          if (freshPair) {
            sig.priceUsd = parseFloat(freshPair.priceUsd || "0");
            sig.liquidity = freshPair.liquidity?.usd || sig.liquidity;
            sig.priceChange24h = freshPair.priceChange?.h24 || 0;
            sig.priceChange6h = freshPair.priceChange?.h6 || 0;
            sig.priceChange1h = freshPair.priceChange?.h1 || 0;
            sig.priceChange5m = freshPair.priceChange?.m5 || 0;

            ctx.log("DexMomentum", "refreshed_position_price", {
              symbol: sig.symbol,
              price: sig.priceUsd.toFixed(10),
              liquidity: "$" + Math.round(sig.liquidity).toLocaleString(),
            });
          }
        }
      } catch (error) {
        ctx.log("DexMomentum", "price_refresh_error", { error: String(error) });
      }
    }

    ctx.state.dexSignals = [...signals, ...preservedSignals];
    ctx.state.lastDexScanRun = Date.now();

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
    ctx.state.signalCache = [
      ...ctx.state.signalCache.filter(s => s.source !== "dexscreener"),
      ...dexAsSignals,
    ];

    ctx.log("DexMomentum", "scan_complete", {
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
    ctx.log("DexMomentum", "scan_error", { error: String(error) });
  }
}
