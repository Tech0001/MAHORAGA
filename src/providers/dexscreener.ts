/**
 * DexScreener API Provider
 *
 * Fetches trending Solana tokens for momentum trading.
 * Filters for tokens aged 3-14 days with proven momentum.
 */

export interface DexPair {
  chainId: string;
  dexId: string;
  url: string;
  pairAddress: string;
  baseToken: {
    address: string;
    name: string;
    symbol: string;
  };
  quoteToken: {
    address: string;
    name: string;
    symbol: string;
  };
  priceNative: string;
  priceUsd: string;
  txns: {
    m5: { buys: number; sells: number };
    h1: { buys: number; sells: number };
    h6: { buys: number; sells: number };
    h24: { buys: number; sells: number };
  };
  volume: {
    h24: number;
    h6: number;
    h1: number;
    m5: number;
  };
  priceChange: {
    m5: number;
    h1: number;
    h6: number;
    h24: number;
  };
  liquidity?: {
    usd: number;
    base: number;
    quote: number;
  };
  fdv?: number;
  marketCap?: number;
  pairCreatedAt?: number;
  info?: {
    imageUrl?: string;
    websites?: Array<{ url: string; label?: string }>;
    socials?: Array<{ url: string; type: string }>;
  };
}

export interface DexTokenProfile {
  url: string;
  chainId: string;
  tokenAddress: string;
  icon?: string;
  description?: string;
  links?: Array<{ url: string; type?: string; label?: string }>;
}

export interface LegitimacySignals {
  hasWebsite: boolean;
  hasTwitter: boolean;
  hasTelegram: boolean;
  boostCount: number;
  sellsExist: boolean;
}

export interface DexMomentumSignal {
  symbol: string;
  tokenAddress: string;
  pairAddress: string;
  name: string;
  priceUsd: number;
  priceChange24h: number;
  priceChange6h: number;
  priceChange1h: number;
  priceChange5m: number;  // For breakout detection
  volume24h: number;
  volume6h: number;
  volume1h: number;       // For breakout volume spike detection
  liquidity: number;
  marketCap: number;
  ageHours: number;
  ageDays: number;
  buyRatio24h: number;
  buyRatio1h: number;
  txnCount24h: number;
  momentumScore: number;
  dexId: string;
  url: string;
  // Multi-tier system: microspray | breakout | lottery | early | established
  tier: "microspray" | "breakout" | "lottery" | "early" | "established";
  legitimacyScore: number;
  legitimacySignals: LegitimacySignals;
}

const DEXSCREENER_BASE = "https://api.dexscreener.com";

export class DexScreenerProvider {
  private rateLimitDelay = 1100; // ~55 requests/min (under 60 limit)
  private lastRequest = 0;

  private async throttle(): Promise<void> {
    const now = Date.now();
    const elapsed = now - this.lastRequest;
    if (elapsed < this.rateLimitDelay) {
      await new Promise(resolve => setTimeout(resolve, this.rateLimitDelay - elapsed));
    }
    this.lastRequest = Date.now();
  }

  /**
   * Get latest token profiles (newly listed/updated)
   */
  async getLatestProfiles(chain: string = "solana"): Promise<DexTokenProfile[]> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/token-profiles/latest/v1`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    const profiles: DexTokenProfile[] = await res.json();
    return profiles.filter(p => p.chainId === chain);
  }

  /**
   * Get token pair data by address
   */
  async getTokenPairs(chain: string, tokenAddress: string): Promise<DexPair[]> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/tokens/v1/${chain}/${tokenAddress}`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get multiple tokens at once (comma-separated, max 30)
   */
  async getMultipleTokens(chain: string, addresses: string[]): Promise<DexPair[]> {
    await this.throttle();

    const joined = addresses.slice(0, 30).join(",");
    const res = await fetch(`${DEXSCREENER_BASE}/tokens/v1/${chain}/${joined}`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Search for tokens by query
   */
  async search(query: string): Promise<{ pairs: DexPair[] }> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/latest/dex/search?q=${encodeURIComponent(query)}`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get top boosted tokens (paid promotions - use with caution)
   */
  async getTopBoosts(): Promise<DexTokenProfile[]> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/token-boosts/top/v1`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get LATEST boosted tokens (different from top - catches new boosts)
   */
  async getLatestBoosts(): Promise<DexTokenProfile[]> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/token-boosts/latest/v1`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get community takeovers - tokens where community is actively taking over
   * Good signal for organic activity
   */
  async getCommunityTakeovers(): Promise<DexTokenProfile[]> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/community-takeovers/latest/v1`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get tokens with active ads - indicates team/budget behind project
   */
  async getLatestAds(): Promise<Array<{ chainId: string; tokenAddress: string; type: string }>> {
    await this.throttle();

    const res = await fetch(`${DEXSCREENER_BASE}/ads/latest/v1`);
    if (!res.ok) throw new Error(`DexScreener API error: ${res.status}`);

    return res.json();
  }

  /**
   * Get trending/top pairs on Solana using orders endpoint
   */
  async getTrendingPairs(): Promise<DexPair[]> {
    await this.throttle();

    // Use the token-boosts endpoint to find tokens getting attention
    const boostsRes = await fetch(`${DEXSCREENER_BASE}/token-boosts/top/v1`);
    if (!boostsRes.ok) return [];

    const boosts: DexTokenProfile[] = await boostsRes.json();
    const solanaBoosts = boosts.filter(b => b.chainId === "solana").slice(0, 30);

    if (solanaBoosts.length === 0) return [];

    // Get pair data for boosted tokens
    const addresses = solanaBoosts.map(b => b.tokenAddress);
    return this.getMultipleTokens("solana", addresses);
  }

  /**
   * Find Solana tokens with momentum using multi-tier system:
   * - "Micro-spray" [TOGGLE]: 30min-2h old, ultra-tiny bets, just honeypot check
   * - "Breakout" [TOGGLE]: 2-6h old, detect rapid 5-min pumps (+50%+ in 5min)
   * - "Lottery": 1-6 hours old, tiny positions, catch pumps (CURRENT WORKING TIER)
   * - "Early Gems": 6h-3 days old, requires legitimacy signals
   * - "Established": 3-14 days old, proven survivors
   */
  async findMomentumTokens(options: {
    // Micro-spray tier (30min-2h) - ultra-tiny bets [TOGGLE - default OFF]
    microSprayEnabled?: boolean;
    microSprayMinAgeMinutes?: number;
    microSprayMaxAgeHours?: number;
    microSprayMinLiquidity?: number;
    // Breakout tier (2-6h) - detect 5-min pump spikes [TOGGLE - default OFF]
    breakoutEnabled?: boolean;
    breakoutMinAgeHours?: number;
    breakoutMaxAgeHours?: number;
    breakoutMinLiquidity?: number;
    breakoutMin5mPump?: number;     // Minimum 5-min price change % to trigger
    // Lottery tier (1-6 hours) - current working tier
    lotteryEnabled?: boolean;
    lotteryMinAgeHours?: number;
    lotteryMaxAgeHours?: number;
    lotteryMinLiquidity?: number;
    lotteryMinVolume?: number;
    // Early tier (6h-3 days) - higher risk, higher reward
    earlyMinAgeDays?: number;
    earlyMaxAgeDays?: number;
    earlyMinLiquidity?: number;
    earlyMinLegitimacyScore?: number;
    // Established tier (3-14 days) - lower risk
    establishedMinAgeDays?: number;
    establishedMaxAgeDays?: number;
    establishedMinLiquidity?: number;
    // Shared filters
    minVolume24h?: number;
    minPriceChange24h?: number;
  } = {}): Promise<DexMomentumSignal[]> {
    const {
      // Micro-spray tier - ultra-tiny lottery tickets [OFF by default]
      microSprayEnabled = false,
      microSprayMinAgeMinutes = 30,  // At least 30 min old
      microSprayMaxAgeHours = 2,     // Max 2 hours
      microSprayMinLiquidity = 10000, // $10k minimum
      // Breakout tier - catch rapid 5-min pumps [OFF by default]
      breakoutEnabled = false,
      breakoutMinAgeHours = 2,       // 2-6 hour window
      breakoutMaxAgeHours = 6,
      breakoutMinLiquidity = 15000,  // $15k minimum
      breakoutMin5mPump = 50,        // Must be up 50%+ in last 5 minutes
      // Lottery tier (current working)
      lotteryEnabled = true,
      lotteryMinAgeHours = 1,
      lotteryMaxAgeHours = 6,
      lotteryMinLiquidity = 15000,
      lotteryMinVolume = 5000,
      // Early tier defaults
      earlyMinAgeDays = 0.25,
      earlyMaxAgeDays = 3,
      earlyMinLiquidity = 30000,
      earlyMinLegitimacyScore = 40,
      // Established tier defaults
      establishedMinAgeDays = 3,
      establishedMaxAgeDays = 14,
      establishedMinLiquidity = 50000,
      // Shared
      minVolume24h = 10000,
      minPriceChange24h = 5,
    } = options;

    // Gather pairs from multiple sources for better coverage
    const allPairs: DexPair[] = [];
    const seenAddresses = new Set<string>();

    // Source 1: Trending/boosted tokens
    try {
      const trendingPairs = await this.getTrendingPairs();
      for (const pair of trendingPairs) {
        if (!seenAddresses.has(pair.baseToken.address)) {
          seenAddresses.add(pair.baseToken.address);
          allPairs.push(pair);
        }
      }
    } catch (e) {
      console.error("Failed to fetch trending:", e);
    }

    // Source 2: Latest profiles (tokens updating their info = active projects)
    try {
      const profiles = await this.getLatestProfiles("solana");
      if (profiles.length > 0) {
        const addresses = profiles
          .map(p => p.tokenAddress)
          .filter(a => !seenAddresses.has(a))
          .slice(0, 30);

        if (addresses.length > 0) {
          const pairs = await this.getMultipleTokens("solana", addresses);
          for (const pair of pairs) {
            if (!seenAddresses.has(pair.baseToken.address)) {
              seenAddresses.add(pair.baseToken.address);
              allPairs.push(pair);
            }
          }
        }
      }
    } catch (e) {
      console.error("Failed to fetch profiles:", e);
    }

    // Source 3: Latest boosts (different from top - catches newly boosted tokens)
    try {
      const latestBoosts = await this.getLatestBoosts();
      const solanaLatestBoosts = (Array.isArray(latestBoosts) ? latestBoosts : [latestBoosts])
        .filter(b => b?.chainId === "solana" && b?.tokenAddress)
        .filter(b => !seenAddresses.has(b.tokenAddress))
        .slice(0, 20);

      if (solanaLatestBoosts.length > 0) {
        const addresses = solanaLatestBoosts.map(b => b.tokenAddress);
        const pairs = await this.getMultipleTokens("solana", addresses);
        for (const pair of pairs) {
          if (!seenAddresses.has(pair.baseToken.address)) {
            seenAddresses.add(pair.baseToken.address);
            allPairs.push(pair);
          }
        }
      }
    } catch (e) {
      console.error("Failed to fetch latest boosts:", e);
    }

    // Source 4: Community takeovers (organic community activity)
    try {
      const takeovers = await this.getCommunityTakeovers();
      const solanaTakeovers = takeovers
        .filter(t => t.chainId === "solana")
        .filter(t => !seenAddresses.has(t.tokenAddress))
        .slice(0, 15);

      if (solanaTakeovers.length > 0) {
        const addresses = solanaTakeovers.map(t => t.tokenAddress);
        const pairs = await this.getMultipleTokens("solana", addresses);
        for (const pair of pairs) {
          if (!seenAddresses.has(pair.baseToken.address)) {
            seenAddresses.add(pair.baseToken.address);
            allPairs.push(pair);
          }
        }
      }
    } catch (e) {
      console.error("Failed to fetch community takeovers:", e);
    }

    // Source 5: Tokens with ads (team/budget behind project)
    try {
      const ads = await this.getLatestAds();
      const solanaAds = ads
        .filter(a => a.chainId === "solana")
        .filter(a => !seenAddresses.has(a.tokenAddress))
        .slice(0, 10);

      if (solanaAds.length > 0) {
        const addresses = solanaAds.map(a => a.tokenAddress);
        const pairs = await this.getMultipleTokens("solana", addresses);
        for (const pair of pairs) {
          if (!seenAddresses.has(pair.baseToken.address)) {
            seenAddresses.add(pair.baseToken.address);
            allPairs.push(pair);
          }
        }
      }
    } catch (e) {
      console.error("Failed to fetch ads:", e);
    }

    // Source 6: Search for trending terms
    const searchTerms = ["pump", "moon", "sol", "meme"];
    for (const term of searchTerms.slice(0, 2)) {
      try {
        const searchResult = await this.search(term);
        const solanaPairs = searchResult.pairs
          .filter(p => p.chainId === "solana")
          .filter(p => !seenAddresses.has(p.baseToken.address))
          .slice(0, 10);

        for (const pair of solanaPairs) {
          seenAddresses.add(pair.baseToken.address);
          allPairs.push(pair);
        }
      } catch (e) {
        console.error(`Failed search for ${term}:`, e);
      }
    }

    console.log(`[DexScreener] Sourced ${allPairs.length} unique tokens from 6 sources`);

    const now = Date.now();
    const signals: DexMomentumSignal[] = [];

    // Debug stats
    let stats = {
      total: 0, noDate: 0, tooNew: 0, tooOld: 0, lowLiq: 0, lowVol: 0,
      negMom: 0, honeypot: 0, lowLegitimacy: 0, noBreakout: 0,
      microSprayPassed: 0, breakoutPassed: 0, lotteryPassed: 0, earlyPassed: 0, establishedPassed: 0
    };

    for (const pair of allPairs) {
      stats.total++;

      // Skip non-Solana
      if (pair.chainId !== "solana") continue;

      // Skip if no creation date
      if (!pair.pairCreatedAt) {
        stats.noDate++;
        continue;
      }

      // Calculate age
      const ageMs = now - pair.pairCreatedAt;
      const ageHours = ageMs / (1000 * 60 * 60);
      const ageDays = ageHours / 24;

      // Get 5-minute price change for breakout detection
      const priceChange5m = pair.priceChange?.m5 || 0;

      // Determine which tier this token could belong to
      const ageMinutes = ageHours * 60;
      const isMicroSprayCandidate = microSprayEnabled && ageMinutes >= microSprayMinAgeMinutes && ageHours < microSprayMaxAgeHours;
      const isBreakoutCandidate = breakoutEnabled && ageHours >= breakoutMinAgeHours && ageHours < breakoutMaxAgeHours;
      const isLotteryCandidate = lotteryEnabled && ageHours >= lotteryMinAgeHours && ageHours < lotteryMaxAgeHours;
      const isEarlyCandidate = ageDays >= earlyMinAgeDays && ageDays < earlyMaxAgeDays;
      const isEstablishedCandidate = ageDays >= establishedMinAgeDays && ageDays <= establishedMaxAgeDays;

      // Skip if doesn't fit any tier
      if (!isMicroSprayCandidate && !isBreakoutCandidate && !isLotteryCandidate && !isEarlyCandidate && !isEstablishedCandidate) {
        if (ageMinutes < microSprayMinAgeMinutes) stats.tooNew++;
        else stats.tooOld++;
        continue;
      }

      // Liquidity filter (tier-specific)
      const liquidity = pair.liquidity?.usd || 0;
      let minLiq = establishedMinLiquidity;
      if (isMicroSprayCandidate) minLiq = microSprayMinLiquidity;
      else if (isBreakoutCandidate) minLiq = breakoutMinLiquidity;
      else if (isLotteryCandidate) minLiq = lotteryMinLiquidity;
      else if (isEarlyCandidate) minLiq = earlyMinLiquidity;

      if (liquidity < minLiq) {
        stats.lowLiq++;
        continue;
      }

      // Volume filter (tier-specific for lottery)
      const volume24h = pair.volume?.h24 || 0;
      const volume1h = pair.volume?.h1 || 0;
      const minVol = isLotteryCandidate ? lotteryMinVolume : (isMicroSprayCandidate ? 1000 : minVolume24h);
      if (volume24h < minVol) {
        stats.lowVol++;
        continue;
      }

      // Price change filter - different by tier
      const priceChange24h = pair.priceChange?.h24 || 0;
      const priceChange1h = pair.priceChange?.h1 || 0;

      // Micro-spray: minimal filters - just honeypot check (no momentum requirement)
      // Let the spray catch everything, winners will emerge
      if (isMicroSprayCandidate && !isBreakoutCandidate && !isLotteryCandidate && !isEarlyCandidate && !isEstablishedCandidate) {
        // No momentum requirement for micro-spray - we're buying everything early
      }
      // Breakout: must have significant 5-min pump
      else if (isBreakoutCandidate && !isLotteryCandidate && !isEarlyCandidate && !isEstablishedCandidate) {
        if (priceChange5m < breakoutMin5mPump) {
          stats.noBreakout++;
          continue; // Not breaking out - skip
        }
      }
      // Lottery: must be pumping (positive 1h change)
      else if (isLotteryCandidate && !isEarlyCandidate && !isEstablishedCandidate) {
        if (priceChange1h < 5) {
          stats.negMom++;
          continue;
        }
      }
      // Early/Established: standard 24h momentum check
      else if (priceChange24h < minPriceChange24h) {
        stats.negMom++;
        continue;
      }

      // Calculate transaction counts
      const buys24h = pair.txns?.h24?.buys || 0;
      const sells24h = pair.txns?.h24?.sells || 0;
      const buys1h = pair.txns?.h1?.buys || 0;
      const sells1h = pair.txns?.h1?.sells || 0;
      const totalTxns24h = buys24h + sells24h;
      const totalTxns1h = buys1h + sells1h;

      // Honeypot protection: must have sells
      // Micro-spray and lottery are more lenient (fewer sells needed since they're newer)
      const minSells = isMicroSprayCandidate ? 3 : (isLotteryCandidate || isBreakoutCandidate ? 5 : 10);
      if (sells24h < minSells) {
        stats.honeypot++;
        continue;
      }

      // Extract legitimacy signals
      const legitimacySignals = this.extractLegitimacySignals(pair, sells24h);
      const legitimacyScore = this.calculateLegitimacyScore(legitimacySignals);

      // For early tier (not lottery/breakout/microspray), must meet legitimacy threshold
      if (isEarlyCandidate && !isEstablishedCandidate && !isLotteryCandidate && !isBreakoutCandidate && !isMicroSprayCandidate && legitimacyScore < earlyMinLegitimacyScore) {
        stats.lowLegitimacy++;
        continue;
      }

      // Determine final tier (priority: established > early > lottery > breakout > microspray)
      // Higher tiers take precedence if token qualifies for multiple
      let tier: "microspray" | "breakout" | "lottery" | "early" | "established";
      if (isEstablishedCandidate) {
        tier = "established";
        stats.establishedPassed++;
      } else if (isEarlyCandidate) {
        tier = "early";
        stats.earlyPassed++;
      } else if (isLotteryCandidate) {
        tier = "lottery";
        stats.lotteryPassed++;
      } else if (isBreakoutCandidate) {
        tier = "breakout";
        stats.breakoutPassed++;
      } else {
        tier = "microspray";
        stats.microSprayPassed++;
      }

      // Calculate buy ratios
      const buyRatio24h = totalTxns24h > 0 ? buys24h / totalTxns24h : 0.5;
      const buyRatio1h = totalTxns1h > 0 ? buys1h / totalTxns1h : 0.5;

      // Get additional metrics (priceChange24h and priceChange1h already declared above)
      const priceChange6h = pair.priceChange?.h6 || 0;
      const volume6h = pair.volume?.h6 || 0;

      // Calculate momentum score with improved formula
      const momentumScore = this.calculateMomentumScore({
        priceChange24h,
        priceChange6h,
        priceChange1h,
        priceChange5m,
        volume24h,
        volume6h,
        liquidity,
        buyRatio24h,
        buyRatio1h,
        ageDays,
        txnCount24h: totalTxns24h,
        tier,
        legitimacyScore,
      });

      signals.push({
        symbol: pair.baseToken.symbol,
        tokenAddress: pair.baseToken.address,
        pairAddress: pair.pairAddress,
        name: pair.baseToken.name,
        priceUsd: parseFloat(pair.priceUsd) || 0,
        priceChange24h,
        priceChange6h,
        priceChange1h,
        priceChange5m,
        volume24h,
        volume6h,
        volume1h,
        liquidity,
        marketCap: pair.marketCap || pair.fdv || 0,
        ageHours,
        ageDays,
        buyRatio24h,
        buyRatio1h,
        txnCount24h: totalTxns24h,
        momentumScore,
        dexId: pair.dexId,
        url: pair.url,
        tier,
        legitimacyScore,
        legitimacySignals,
      });
    }

    // Log filter stats for debugging
    console.log("[DexScreener] Filter stats:", JSON.stringify(stats));

    // Sort by momentum score descending
    return signals.sort((a, b) => b.momentumScore - a.momentumScore);
  }

  /**
   * Extract legitimacy signals from pair data
   */
  private extractLegitimacySignals(pair: DexPair, sells24h: number): LegitimacySignals {
    const websites = pair.info?.websites || [];
    const socials = pair.info?.socials || [];

    return {
      hasWebsite: websites.length > 0,
      hasTwitter: socials.some(s => s.type === "twitter" || s.url?.includes("twitter.com") || s.url?.includes("x.com")),
      hasTelegram: socials.some(s => s.type === "telegram" || s.url?.includes("t.me")),
      boostCount: (pair as any).boosts?.active || 0,
      sellsExist: sells24h >= 10,
    };
  }

  /**
   * Calculate legitimacy score (0-100)
   * Higher score = more likely to be a real project, not a rug
   */
  private calculateLegitimacyScore(signals: LegitimacySignals): number {
    let score = 0;

    // Website (25 points) - shows real project effort
    if (signals.hasWebsite) score += 25;

    // Twitter (25 points) - community presence
    if (signals.hasTwitter) score += 25;

    // Telegram (20 points) - active community
    if (signals.hasTelegram) score += 20;

    // Boosts (0-20 points) - paid promotion = team investing in visibility
    score += Math.min(20, signals.boostCount * 2);

    // Sells exist (10 points) - not a honeypot
    if (signals.sellsExist) score += 10;

    return score;
  }

  /**
   * Improved momentum score calculation
   *
   * Key changes from old formula:
   * - Reduced weight on 24h price (was chasing pumps)
   * - Added momentum consistency (6h trend)
   * - Added liquidity depth bonus (safer exit)
   * - Added transaction velocity (organic interest)
   * - Added volatility penalty (detect spikes)
   * - Tier-aware scoring
   */
  private calculateMomentumScore(params: {
    priceChange24h: number;
    priceChange6h: number;
    priceChange1h: number;
    volume24h: number;
    volume6h: number;
    liquidity: number;
    buyRatio24h: number;
    buyRatio1h: number;
    ageDays: number;
    txnCount24h: number;
    tier: "microspray" | "breakout" | "lottery" | "early" | "established";
    legitimacyScore: number;
    priceChange5m?: number;
  }): number {
    const {
      priceChange24h, priceChange6h, priceChange1h,
      volume24h, volume6h, liquidity,
      buyRatio24h, buyRatio1h, ageDays, txnCount24h,
      tier, legitimacyScore
    } = params;

    // 1. Price momentum - REDUCED weight (0-25)
    // Catching momentum, not buying tops
    // Sweet spot: 20-100% gains, diminishing returns after
    const priceScore = Math.min(25, priceChange24h / 4);

    // 2. Recent momentum - 1h (0-15)
    const recentScore = Math.min(15, Math.max(0, priceChange1h * 0.75));

    // 3. Momentum consistency - is trend sustaining? (0-15)
    // If 6h and 1h are both positive, momentum is real
    // If 1h is positive but 6h is negative, could be a dead cat bounce
    let consistencyScore = 0;
    if (priceChange6h > 0 && priceChange1h > 0) {
      // Both positive = sustaining momentum
      consistencyScore = Math.min(15, priceChange6h / 4);
    } else if (priceChange6h > 0 && priceChange1h <= 0) {
      // 6h positive, 1h negative = momentum fading (small penalty)
      consistencyScore = Math.max(0, 5 - Math.abs(priceChange1h));
    }
    // If 6h negative, no consistency points

    // 4. Liquidity depth bonus (0-15)
    // Higher liquidity = safer exit = better
    // log scale: $30k=0, $100k=7, $500k=12, $1M+=15
    const liqScore = Math.min(15, Math.max(0, Math.log10(liquidity / 10000) * 7.5));

    // 5. Volume health - CAPPED (0-10)
    // High volume/liquidity ratio can mean slippage
    const volumeRatio = volume24h / Math.max(liquidity, 1);
    const volumeScore = Math.min(10, volumeRatio * 3);

    // 5b. Volume acceleration bonus (0-5)
    // If recent 6h volume is strong relative to 24h, momentum is building
    const expectedVol6h = volume24h / 4; // If volume was constant, 6h = 25% of 24h
    const volAcceleration = volume6h / Math.max(expectedVol6h, 1);
    const volAccelScore = volAcceleration > 1 ? Math.min(5, (volAcceleration - 1) * 5) : 0;

    // 6. Buy pressure (0-10)
    // Use both 24h and 1h for momentum
    const buyScore24 = (buyRatio24h - 0.5) * 10; // -5 to +5
    const buyScore1h = (buyRatio1h - 0.5) * 10;  // -5 to +5
    const buyScore = buyScore24 + buyScore1h;    // -10 to +10

    // 7. Transaction velocity - organic interest (0-10)
    // More transactions = more organic interest
    // Normalize by volume to detect wash trading
    const txnPerVolK = txnCount24h / Math.max(volume24h / 1000, 1);
    const organicScore = Math.min(10, txnPerVolK * 1.5);

    // 8. Volatility penalty (-10 to 0)
    // If 1h change is way higher than expected from 6h trend, it's a spike
    // Spikes often reverse
    const expectedHourlyFromDaily = priceChange6h / 6;
    const spikeRatio = Math.abs(priceChange1h) / Math.max(Math.abs(expectedHourlyFromDaily), 1);
    const volatilityPenalty = spikeRatio > 4 ? -Math.min(10, (spikeRatio - 4) * 2) : 0;

    // 9. Tier-specific bonuses
    let tierBonus = 0;
    const priceChange5mVal = params.priceChange5m || 0;

    if (tier === "microspray") {
      // Micro-spray: minimal scoring - just want to catch everything early
      // Small bonus for any positive movement
      tierBonus = priceChange1h > 0 ? Math.min(5, priceChange1h / 10) : 0;
    } else if (tier === "breakout") {
      // Breakout: reward the 5-minute pump strength
      // This is the key signal - bigger pump = higher bonus
      tierBonus = Math.min(15, priceChange5mVal / 10); // +150% 5m = 15 points
    } else if (tier === "lottery") {
      // Lottery tier: reward strong immediate momentum
      // These are fresh launches - we want to see them pumping NOW
      const immediateScore = Math.min(10, priceChange1h / 5); // +50% 1h = 10 points
      tierBonus = immediateScore;
    } else if (tier === "early") {
      // Early tier: legitimacy matters more
      // Scale legitimacy (0-100) to bonus (0-10)
      tierBonus = legitimacyScore / 10;
    } else {
      // Established tier: age sweetspot bonus
      // Prefer 5-10 day old tokens
      const idealAge = 7;
      const ageDeviation = Math.abs(ageDays - idealAge);
      tierBonus = Math.max(0, 5 - ageDeviation);
    }

    const total = priceScore + recentScore + consistencyScore + liqScore +
                  volumeScore + volAccelScore + buyScore + organicScore + volatilityPenalty + tierBonus;

    // Log score breakdown for top candidates (debugging)
    if (total > 50) {
      console.log(`[DexScore] ${tier} token score=${total.toFixed(1)}: ` +
        `price=${priceScore.toFixed(1)} recent=${recentScore.toFixed(1)} ` +
        `consist=${consistencyScore.toFixed(1)} liq=${liqScore.toFixed(1)} ` +
        `vol=${volumeScore.toFixed(1)} volAccel=${volAccelScore.toFixed(1)} ` +
        `buy=${buyScore.toFixed(1)} organic=${organicScore.toFixed(1)} ` +
        `volPenalty=${volatilityPenalty.toFixed(1)} tierBonus=${tierBonus.toFixed(1)}`);
    }

    return Math.max(0, total);
  }
}

export function createDexScreenerProvider(): DexScreenerProvider {
  return new DexScreenerProvider();
}
