import { useState, useEffect, useMemo } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import clsx from 'clsx'
import { Panel } from './components/Panel'
import { Metric, MetricInline } from './components/Metric'
import { StatusIndicator, StatusBar } from './components/StatusIndicator'
import { SettingsModal } from './components/SettingsModal'
import { SetupWizard } from './components/SetupWizard'
import { LineChart, Sparkline } from './components/LineChart'
import { NotificationBell } from './components/NotificationBell'
import { Tooltip, TooltipContent } from './components/Tooltip'
import type { Status, Config, LogEntry, Signal, Position, SignalResearch, PortfolioSnapshot, DexMomentumSignal, DexPosition, CrisisLevel } from './types'

const API_BASE = '/api'

function getApiToken(): string {
  return localStorage.getItem('mahoraga_api_token') || (window as unknown as { VITE_MAHORAGA_API_TOKEN?: string }).VITE_MAHORAGA_API_TOKEN || ''
}

function authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const token = getApiToken()
  const headers = new Headers(options.headers)
  if (token) {
    headers.set('Authorization', `Bearer ${token}`)
  }
  if (!headers.has('Content-Type') && options.body) {
    headers.set('Content-Type', 'application/json')
  }
  return fetch(url, { ...options, headers })
}

function formatCurrency(amount: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount)
}

function formatPercent(value: number): string {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function getAgentColor(agent: string): string {
  const colors: Record<string, string> = {
    'Analyst': 'text-hud-purple',
    'Executor': 'text-hud-cyan',
    'StockTwits': 'text-hud-success',
    'SignalResearch': 'text-hud-cyan',
    'PositionResearch': 'text-hud-purple',
    'Crypto': 'text-hud-warning',
    'System': 'text-hud-text-dim',
  }
  return colors[agent] || 'text-hud-text'
}

function isCryptoSymbol(symbol: string, cryptoSymbols: string[] = []): boolean {
  return cryptoSymbols.includes(symbol) || symbol.includes('/USD') || symbol.includes('BTC') || symbol.includes('ETH') || symbol.includes('SOL')
}

function getVerdictColor(verdict: string): string {
  if (verdict === 'BUY') return 'text-hud-success'
  if (verdict === 'SKIP') return 'text-hud-error'
  return 'text-hud-warning'
}

function getQualityColor(quality: string): string {
  if (quality === 'excellent') return 'text-hud-success'
  if (quality === 'good') return 'text-hud-primary'
  if (quality === 'fair') return 'text-hud-warning'
  return 'text-hud-error'
}

function getSentimentColor(score: number): string {
  if (score >= 0.3) return 'text-hud-success'
  if (score <= -0.2) return 'text-hud-error'
  return 'text-hud-warning'
}

function getCrisisLevelColor(level: CrisisLevel): string {
  switch (level) {
    case 0: return 'text-hud-success'
    case 1: return 'text-hud-warning'
    case 2: return 'text-orange-500'
    case 3: return 'text-hud-error'
    default: return 'text-hud-text-dim'
  }
}

function getCrisisLevelBg(level: CrisisLevel): string {
  switch (level) {
    case 0: return 'bg-hud-success/20'
    case 1: return 'bg-hud-warning/20'
    case 2: return 'bg-orange-500/20'
    case 3: return 'bg-hud-error/20'
    default: return 'bg-hud-panel'
  }
}

function getCrisisLevelLabel(level: CrisisLevel): string {
  switch (level) {
    case 0: return 'NORMAL'
    case 1: return 'ELEVATED'
    case 2: return 'HIGH ALERT'
    case 3: return 'FULL CRISIS'
    default: return 'UNKNOWN'
  }
}

// Generate mock portfolio history for demo (will be replaced by real data from API)
function generateMockPortfolioHistory(equity: number, points: number = 24): PortfolioSnapshot[] {
  const history: PortfolioSnapshot[] = []
  const now = Date.now()
  const interval = 3600000 // 1 hour in ms
  let value = equity * 0.95 // Start slightly lower
  
  for (let i = points; i >= 0; i--) {
    const change = (Math.random() - 0.45) * equity * 0.005 // Small random walk with slight upward bias
    value = Math.max(value + change, equity * 0.8)
    const pl = value - equity * 0.95
    history.push({
      timestamp: now - i * interval,
      equity: value,
      pl,
      pl_pct: (pl / (equity * 0.95)) * 100,
    })
  }
  // Ensure last point is current equity
  history[history.length - 1] = {
    timestamp: now,
    equity,
    pl: equity - history[0].equity,
    pl_pct: ((equity - history[0].equity) / history[0].equity) * 100,
  }
  return history
}

// Generate mock price history for positions
function generateMockPriceHistory(currentPrice: number, unrealizedPl: number, points: number = 20): number[] {
  const prices: number[] = []
  const isPositive = unrealizedPl >= 0
  const startPrice = currentPrice * (isPositive ? 0.95 : 1.05)
  
  for (let i = 0; i < points; i++) {
    const progress = i / (points - 1)
    const trend = startPrice + (currentPrice - startPrice) * progress
    const noise = trend * (Math.random() - 0.5) * 0.02
    prices.push(trend + noise)
  }
  prices[prices.length - 1] = currentPrice
  return prices
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [showSettings, setShowSettings] = useState(false)
  const [showSetup, setShowSetup] = useState(false)
  const [setupChecked, setSetupChecked] = useState(false)
  const [time, setTime] = useState(new Date())
  const [theme, setTheme] = useState<'dark' | 'light'>(() => {
    if (typeof window !== 'undefined') {
      return (localStorage.getItem('theme') as 'dark' | 'light') || 'dark'
    }
    return 'dark'
  })

  // Apply theme to document
  useEffect(() => {
    document.documentElement.classList.toggle('light', theme === 'light')
    localStorage.setItem('theme', theme)
  }, [theme])
  const [portfolioHistory, setPortfolioHistory] = useState<PortfolioSnapshot[]>([])

  // Fetch real SOL price (cached for 5 minutes)
  const [solPriceUsd, setSolPriceUsd] = useState<number>(200) // Fallback
  useEffect(() => {
    const fetchSolPrice = async () => {
      try {
        const res = await fetch(
          'https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112'
        )
        if (!res.ok) return
        const data = await res.json()
        const pairs = data.pairs || []
        // Find highest liquidity SOL pair
        const solPair = pairs
          .filter((p: { priceUsd?: string; liquidity?: { usd?: number } }) =>
            p.priceUsd && p.liquidity?.usd && p.liquidity.usd > 100000
          )
          .sort((a: { liquidity?: { usd?: number } }, b: { liquidity?: { usd?: number } }) =>
            (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
          )[0]
        if (solPair?.priceUsd) {
          const price = parseFloat(solPair.priceUsd)
          if (!isNaN(price) && price > 0) {
            setSolPriceUsd(price)
          }
        }
      } catch {
        // Keep fallback price
      }
    }
    fetchSolPrice()
    // Refresh every 5 minutes
    const interval = setInterval(fetchSolPrice, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    const checkSetup = async () => {
      try {
        const res = await authFetch(`${API_BASE}/setup/status`)
        const data = await res.json()
        if (data.ok && !data.data.configured) {
          setShowSetup(true)
        }
        setSetupChecked(true)
      } catch {
        setSetupChecked(true)
      }
    }
    checkSetup()
  }, [])

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await authFetch(`${API_BASE}/status`)
        const data = await res.json()
        if (data.ok) {
          setStatus(data.data)
          setError(null)
          
          // Generate mock portfolio history if we have account data but no history
          if (data.data.account && portfolioHistory.length === 0) {
            setPortfolioHistory(generateMockPortfolioHistory(data.data.account.equity))
          } else if (data.data.account) {
            // Append new data point on each fetch
            setPortfolioHistory(prev => {
              const now = Date.now()
              const newSnapshot: PortfolioSnapshot = {
                timestamp: now,
                equity: data.data.account.equity,
                pl: data.data.account.equity - (prev[0]?.equity || data.data.account.equity),
                pl_pct: prev[0] ? ((data.data.account.equity - prev[0].equity) / prev[0].equity) * 100 : 0,
              }
              // Keep last 48 points (4 hours at 5-second intervals, or display fewer if needed)
              const updated = [...prev, newSnapshot].slice(-48)
              return updated
            })
          }
        } else {
          setError(data.error || 'Failed to fetch status')
        }
      } catch (err) {
        setError('Connection failed - is the agent running?')
      }
    }

    if (setupChecked && !showSetup) {
      fetchStatus()
      const interval = setInterval(fetchStatus, 5000)
      const timeInterval = setInterval(() => setTime(new Date()), 1000)

      return () => {
        clearInterval(interval)
        clearInterval(timeInterval)
      }
    }
  }, [setupChecked, showSetup])

  const handleSaveConfig = async (config: Config) => {
    const res = await authFetch(`${API_BASE}/config`, {
      method: 'POST',
      body: JSON.stringify(config),
    })
    const data = await res.json()
    if (data.ok && status) {
      setStatus({ ...status, config: data.data })
    }
  }

  const handleDexClearCooldowns = async () => {
    try {
      const res = await authFetch(`${API_BASE}/dex/clear-cooldowns`, { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        alert(`Cleared ${data.clearedCount} cooldowns`)
      }
    } catch (err) {
      console.error('Failed to clear cooldowns:', err)
    }
  }

  const handleDexClearBreaker = async () => {
    try {
      const res = await authFetch(`${API_BASE}/dex/clear-breaker`, { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        alert(data.message)
      }
    } catch (err) {
      console.error('Failed to clear breaker:', err)
    }
  }

  const handleDexReset = async () => {
    if (!confirm('Reset DEX paper trading? This will clear all positions and reset balance to 1 SOL.')) return
    try {
      const res = await authFetch(`${API_BASE}/dex/reset`, { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        alert(`DEX reset. Paper balance: ${data.paperBalance} SOL`)
      }
    } catch (err) {
      console.error('Failed to reset DEX:', err)
    }
  }

  // Derived state (must stay above early returns per React hooks rules)
  const account = status?.account
  const positions = status?.positions || []
  const signals = status?.signals || []
  const logs = status?.logs || []
  const costs = status?.costs || { total_usd: 0, calls: 0, tokens_in: 0, tokens_out: 0 }
  const config = status?.config
  const isMarketOpen = status?.clock?.is_open ?? false

  const startingEquity = config?.starting_equity || 100000
  const totalPl = account ? account.equity - startingEquity : 0

  // DEX Paper Trading values (convert SOL to USD using real price)
  const dexPaperTrading = status?.dexPaperTrading
  const dexPositions = status?.dexPositions || []
  const dexPaperBalanceUsd = (dexPaperTrading?.paperBalance || 0) * solPriceUsd
  const dexRealizedPl = (dexPaperTrading?.realizedPnL || 0) * solPriceUsd
  const dexTotalValue = dexPaperBalanceUsd + dexPositions.reduce((sum, p) => sum + (p.currentValue || 0), 0)
  const dexStartingBalanceSol = config?.dex_starting_balance_sol || 1.0
  const dexStartingValue = dexStartingBalanceSol * solPriceUsd
  const dexTotalPl = dexTotalValue - dexStartingValue + dexRealizedPl

  // Combined totals
  const combinedEquity = (account?.equity || 0) + dexTotalValue
  const combinedPl = totalPl + dexTotalPl
  const combinedPlPct = startingEquity > 0 ? (combinedPl / (startingEquity + dexStartingValue)) * 100 : 0

  // Color palette for position lines (distinct colors for each stock)
  const positionColors = ['cyan', 'purple', 'yellow', 'blue', 'green'] as const

  // Generate mock price histories for positions (stable per session via useMemo)
  const positionPriceHistories = useMemo(() => {
    const histories: Record<string, number[]> = {}
    positions.forEach(pos => {
      histories[pos.symbol] = generateMockPriceHistory(pos.current_price, pos.unrealized_pl)
    })
    return histories
  }, [positions.map(p => p.symbol).join(',')])

  // DEX portfolio history from API
  const dexPortfolioHistory = status?.dexPortfolioHistory || []

  // Crisis Mode state
  const crisisState = status?.crisisState
  const lastCrisisCheck = status?.lastCrisisCheck || 0

  // Chart data - show absolute dollar values
  const portfolioChartData = useMemo(() => {
    return portfolioHistory.map(s => s.equity)
  }, [portfolioHistory])

  // DEX chart data - use real history, show in USD
  // Pad to match stocks array length so they align on the same x-axis
  const dexChartData = useMemo(() => {
    if (!config?.dex_enabled) return []

    const targetLength = portfolioHistory.length

    // Use real history if available
    if (dexPortfolioHistory.length >= 1) {
      const dexValues = dexPortfolioHistory.map(s => s.totalValueSol * solPriceUsd)

      // Pad the beginning with the first value to match stocks array length
      if (dexValues.length < targetLength) {
        const padding = Array(targetLength - dexValues.length).fill(dexValues[0])
        return [...padding, ...dexValues]
      }
      return dexValues.slice(-targetLength) // Keep only last N points if too many
    }

    // Fallback: show current value across the whole timeline
    if (targetLength >= 2) {
      return Array(targetLength).fill(dexTotalValue)
    }

    return []
  }, [dexPortfolioHistory, dexTotalValue, portfolioHistory.length, config?.dex_enabled, solPriceUsd])

  // Combined labels from stocks history (primary) or DEX history
  const portfolioChartLabels = useMemo(() => {
    if (portfolioHistory.length > 0) {
      return portfolioHistory.map(s =>
        new Date(s.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
      )
    }
    if (dexPortfolioHistory.length > 0) {
      return dexPortfolioHistory.map(s =>
        new Date(s.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
      )
    }
    return []
  }, [portfolioHistory, dexPortfolioHistory])

  // Normalize position price histories to % change for stacked comparison view
  const normalizedPositionSeries = useMemo(() => {
    return positions.map((pos, idx) => {
      const priceHistory = positionPriceHistories[pos.symbol] || []
      if (priceHistory.length < 2) return null
      const startPrice = priceHistory[0]
      // Convert to % change from start
      const normalizedData = priceHistory.map(price => ((price - startPrice) / startPrice) * 100)
      return {
        label: pos.symbol,
        data: normalizedData,
        variant: positionColors[idx % positionColors.length],
      }
    }).filter(Boolean) as { label: string; data: number[]; variant: typeof positionColors[number] }[]
  }, [positions, positionPriceHistories])

  // Early returns (after all hooks)
  if (showSetup) {
    return <SetupWizard onComplete={() => setShowSetup(false)} />
  }

  if (error && !status) {
    const isAuthError = error.includes('Unauthorized')
    return (
      <div className="min-h-screen bg-hud-bg flex items-center justify-center p-6">
        <Panel title={isAuthError ? "AUTHENTICATION REQUIRED" : "CONNECTION ERROR"} className="max-w-md w-full">
          <div className="text-center py-8">
            <div className="text-hud-error text-2xl mb-4">{isAuthError ? "NO TOKEN" : "OFFLINE"}</div>
            <p className="text-hud-text-dim text-sm mb-6">{error}</p>
            {isAuthError ? (
              <div className="space-y-4">
                <div className="text-left bg-hud-panel p-4 border border-hud-line">
                  <label className="hud-label block mb-2">API Token</label>
                  <input
                    type="password"
                    className="hud-input w-full mb-2"
                    placeholder="Enter MAHORAGA_API_TOKEN"
                    defaultValue={localStorage.getItem('mahoraga_api_token') || ''}
                    onChange={(e) => localStorage.setItem('mahoraga_api_token', e.target.value)}
                  />
                  <button 
                    onClick={() => window.location.reload()}
                    className="hud-button w-full"
                  >
                    Save & Reload
                  </button>
                </div>
                <p className="text-hud-text-dim text-xs">
                  Find your token in <code className="text-hud-primary">.dev.vars</code> (local) or Cloudflare secrets (deployed)
                </p>
              </div>
            ) : (
              <p className="text-hud-text-dim text-xs">
                Enable the agent: <code className="text-hud-primary">curl -H "Authorization: Bearer $TOKEN" localhost:8787/agent/enable</code>
              </p>
            )}
          </div>
        </Panel>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-hud-bg">
      <div className="max-w-[1920px] mx-auto p-4">
        <header className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 mb-4 pb-3 border-b border-hud-line">
          <div className="flex items-center gap-4 md:gap-6">
            <div className="flex items-baseline gap-2">
              <span className="text-xl md:text-2xl font-light tracking-tight text-hud-text-bright">
                MAHORAGA
              </span>
              <span className="hud-label">v2</span>
            </div>
            <StatusIndicator 
              status={isMarketOpen ? 'active' : 'inactive'} 
              label={isMarketOpen ? 'MARKET OPEN' : 'MARKET CLOSED'}
              pulse={isMarketOpen}
            />
          </div>
          <div className="flex items-center gap-3 md:gap-6 flex-wrap">
            <StatusBar
              items={[
                { label: 'LLM COST', value: `$${costs.total_usd.toFixed(4)}`, status: costs.total_usd > 1 ? 'warning' : 'active' },
                { label: 'API CALLS', value: costs.calls.toString() },
              ]}
            />
            <NotificationBell 
              overnightActivity={status?.overnightActivity}
              premarketPlan={status?.premarketPlan}
            />
            <button
              className="hud-label hover:text-hud-primary transition-colors"
              onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
              title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
            >
              {theme === 'dark' ? '☀' : '☾'}
            </button>
            <button
              className="hud-label hover:text-hud-primary transition-colors"
              onClick={() => setShowSettings(true)}
            >
              [CONFIG]
            </button>
            <span className="hud-value-sm font-mono">
              {time.toLocaleTimeString('en-US', { hour12: false })}
            </span>
          </div>
        </header>

        <div className="grid grid-cols-4 md:grid-cols-8 lg:grid-cols-12 gap-4">
          {/* Row 1: Account, Positions, LLM Costs */}
          <div className="col-span-4 md:col-span-4 lg:col-span-3">
            <Panel title="ACCOUNT" className="h-full">
              {account ? (
                <div className="space-y-3">
                  <Metric label="TOTAL EQUITY" value={formatCurrency(combinedEquity)} size="xl" />

                  {/* Breakdown by source */}
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div className="flex items-center gap-1">
                      <span className="w-2 h-2 rounded-full bg-hud-success"></span>
                      <span className="text-hud-text-dim">STOCKS:</span>
                      <span className="text-hud-text">{formatCurrency(account.equity)}</span>
                    </div>
                    {config?.dex_enabled && (
                      <div className="flex items-center gap-1">
                        <span className="w-2 h-2 rounded-full bg-hud-warning"></span>
                        <span className="text-hud-text-dim">DEX:</span>
                        <span className="text-hud-text">{formatCurrency(dexTotalValue)}</span>
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <Metric label="CASH" value={formatCurrency(account.cash)} size="md" />
                    <Metric label="BUYING POWER" value={formatCurrency(account.buying_power)} size="md" />
                  </div>

                  <div className="pt-2 border-t border-hud-line space-y-2">
                    <Metric
                      label="COMBINED P&L"
                      value={`${formatCurrency(combinedPl)} (${formatPercent(combinedPlPct)})`}
                      size="md"
                      color={combinedPl >= 0 ? 'success' : 'error'}
                    />
                    <div className="grid grid-cols-2 gap-2">
                      <MetricInline
                        label="STOCKS"
                        value={formatCurrency(totalPl)}
                        color={totalPl >= 0 ? 'success' : 'error'}
                      />
                      {config?.dex_enabled && (
                        <MetricInline
                          label="DEX"
                          value={formatCurrency(dexTotalPl)}
                          color={dexTotalPl >= 0 ? 'success' : 'error'}
                        />
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-hud-text-dim text-sm">Loading...</div>
              )}
            </Panel>
          </div>

          <div className="col-span-4 md:col-span-4 lg:col-span-5">
            <Panel title="POSITIONS" titleRight={`${positions.length}/${config?.max_positions || 5}`} className="h-full">
              {positions.length === 0 ? (
                <div className="text-hud-text-dim text-sm py-8 text-center">No open positions</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-hud-line/50">
                        <th className="hud-label text-left py-2 px-2">Symbol</th>
                        <th className="hud-label text-right py-2 px-2 hidden sm:table-cell">Qty</th>
                        <th className="hud-label text-right py-2 px-2 hidden md:table-cell">Value</th>
                        <th className="hud-label text-right py-2 px-2">P&L</th>
                        <th className="hud-label text-center py-2 px-2">Trend</th>
                      </tr>
                    </thead>
                    <tbody>
                      {positions.map((pos: Position) => {
                        const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100
                        const priceHistory = positionPriceHistories[pos.symbol] || []
                        const posEntry = status?.positionEntries?.[pos.symbol]
                        const staleness = status?.stalenessAnalysis?.[pos.symbol]
                        const holdTime = posEntry ? Math.floor((Date.now() - posEntry.entry_time) / 3600000) : null
                        
                        return (
                          <motion.tr 
                            key={pos.symbol}
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="border-b border-hud-line/20 hover:bg-hud-line/10"
                          >
                            <td className="hud-value-sm py-2 px-2">
                              <Tooltip
                                position="right"
                                content={
                                  <TooltipContent
                                    title={pos.symbol}
                                    items={[
                                      { label: 'Entry Price', value: posEntry ? formatCurrency(posEntry.entry_price) : 'N/A' },
                                      { label: 'Current Price', value: formatCurrency(pos.current_price) },
                                      { label: 'Hold Time', value: holdTime !== null ? `${holdTime}h` : 'N/A' },
                                      { label: 'Entry Sentiment', value: posEntry ? `${(posEntry.entry_sentiment * 100).toFixed(0)}%` : 'N/A' },
                                      ...(staleness ? [{ 
                                        label: 'Staleness', 
                                        value: `${(staleness.score * 100).toFixed(0)}%`,
                                        color: staleness.shouldExit ? 'text-hud-error' : 'text-hud-text'
                                      }] : []),
                                    ]}
                                    description={posEntry?.entry_reason}
                                  />
                                }
                              >
                                <span className="cursor-help border-b border-dotted border-hud-text-dim">
                                  {isCryptoSymbol(pos.symbol, config?.crypto_symbols) && (
                                    <span className="text-hud-warning mr-1">₿</span>
                                  )}
                                  {pos.symbol}
                                </span>
                              </Tooltip>
                            </td>
                            <td className="hud-value-sm text-right py-2 px-2 hidden sm:table-cell">{pos.qty}</td>
                            <td className="hud-value-sm text-right py-2 px-2 hidden md:table-cell">{formatCurrency(pos.market_value)}</td>
                            <td className={clsx(
                              'hud-value-sm text-right py-2 px-2',
                              pos.unrealized_pl >= 0 ? 'text-hud-success' : 'text-hud-error'
                            )}>
                              <div>{formatCurrency(pos.unrealized_pl)}</div>
                              <div className="text-xs opacity-70">{formatPercent(plPct)}</div>
                            </td>
                            <td className="py-2 px-2">
                              <div className="flex justify-center">
                                <Sparkline data={priceHistory} width={60} height={20} />
                              </div>
                            </td>
                          </motion.tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>
          </div>

          <div className="col-span-4 md:col-span-8 lg:col-span-4">
            <Panel title="LLM COSTS" className="h-full">
              <div className="grid grid-cols-2 gap-4">
                <Metric label="TOTAL SPENT" value={`$${costs.total_usd.toFixed(4)}`} size="lg" />
                <Metric label="API CALLS" value={costs.calls.toString()} size="lg" />
                <MetricInline label="TOKENS IN" value={costs.tokens_in.toLocaleString()} />
                <MetricInline label="TOKENS OUT" value={costs.tokens_out.toLocaleString()} />
                <MetricInline 
                  label="AVG COST/CALL" 
                  value={costs.calls > 0 ? `$${(costs.total_usd / costs.calls).toFixed(6)}` : '$0'} 
                />
                <MetricInline label="MODEL" value={config?.llm_model || 'gpt-4o-mini'} />
              </div>
            </Panel>
          </div>

          {/* Crisis Mode Panel - Black Swan Protection */}
          {config?.crisis_mode_enabled && (
            <div className="col-span-4 md:col-span-8 lg:col-span-12">
              <Panel
                title="CRISIS MONITOR"
                titleRight={crisisState ? `Last check: ${lastCrisisCheck ? new Date(lastCrisisCheck).toLocaleTimeString() : 'Never'}` : 'DISABLED'}
                className="h-auto"
              >
                {crisisState ? (
                  <div className="space-y-3">
                    {/* Crisis Level Banner */}
                    <div className={clsx(
                      'flex items-center justify-between p-3 rounded border',
                      getCrisisLevelBg(crisisState.level),
                      crisisState.level === 0 ? 'border-hud-success/30' :
                      crisisState.level === 1 ? 'border-hud-warning/30' :
                      crisisState.level === 2 ? 'border-orange-500/30' :
                      'border-hud-error/30'
                    )}>
                      <div className="flex items-center gap-3">
                        <span className={clsx(
                          'text-2xl font-mono font-bold',
                          getCrisisLevelColor(crisisState.level)
                        )}>
                          LEVEL {crisisState.level}
                        </span>
                        <span className={clsx(
                          'text-lg font-semibold',
                          getCrisisLevelColor(crisisState.level)
                        )}>
                          {getCrisisLevelLabel(crisisState.level)}
                        </span>
                      </div>
                      {crisisState.manualOverride && (
                        <span className="px-2 py-1 bg-hud-purple/20 text-hud-purple text-xs rounded">
                          MANUAL OVERRIDE
                        </span>
                      )}
                    </div>

                    {/* Indicators Grid - Row 1: Core Indicators */}
                    <div className="grid grid-cols-3 md:grid-cols-6 lg:grid-cols-8 gap-3">
                      {/* VIX */}
                      <div className="space-y-1 cursor-help" title="Fear Index - measures expected market volatility. High = panic selling likely.">
                        <span className="hud-label">VIX</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.vix !== null && crisisState.indicators.vix >= (config?.crisis_vix_elevated || 25)
                            ? crisisState.indicators.vix >= (config?.crisis_vix_critical || 45) ? 'text-hud-error' : 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.vix?.toFixed(1) ?? 'N/A'}
                        </div>
                      </div>
                      {/* Yield Curve */}
                      <div className="space-y-1 cursor-help" title="Treasury Yield Curve - negative (inverted) = recession signal. Banks lose money lending.">
                        <span className="hud-label">2Y/10Y</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.yieldCurve2Y10Y !== null && crisisState.indicators.yieldCurve2Y10Y <= (config?.crisis_yield_curve_inversion_critical || -0.5)
                            ? 'text-hud-error'
                            : crisisState.indicators.yieldCurve2Y10Y !== null && crisisState.indicators.yieldCurve2Y10Y <= (config?.crisis_yield_curve_inversion_warning || 0.25)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.yieldCurve2Y10Y !== null ? `${(crisisState.indicators.yieldCurve2Y10Y * 100).toFixed(0)}bp` : 'N/A'}
                        </div>
                      </div>
                      {/* TED Spread */}
                      <div className="space-y-1 cursor-help" title="TED Spread - gap between bank lending rates and T-bills. High = banks don't trust each other.">
                        <span className="hud-label">TED</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.tedSpread !== null && crisisState.indicators.tedSpread >= (config?.crisis_ted_spread_critical || 1.0)
                            ? 'text-hud-error'
                            : crisisState.indicators.tedSpread !== null && crisisState.indicators.tedSpread >= (config?.crisis_ted_spread_warning || 0.5)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.tedSpread?.toFixed(2) ?? 'N/A'}%
                        </div>
                      </div>
                      {/* DXY Dollar Index */}
                      <div className="space-y-1 cursor-help" title="Dollar Index - high = flight to safety, global risk-off. Everyone fleeing to USD.">
                        <span className="hud-label">DXY</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.dxy !== null && crisisState.indicators.dxy >= (config?.crisis_dxy_critical || 110)
                            ? 'text-hud-error'
                            : crisisState.indicators.dxy !== null && crisisState.indicators.dxy >= (config?.crisis_dxy_elevated || 105)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.dxy?.toFixed(1) ?? 'N/A'}
                        </div>
                      </div>
                      {/* USD/JPY */}
                      <div className="space-y-1 cursor-help" title="Yen Carry Trade - low/falling = carry trade unwinding, global deleveraging. Japan selling treasuries.">
                        <span className="hud-label">USD/JPY</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.usdJpy !== null && crisisState.indicators.usdJpy <= (config?.crisis_usdjpy_critical || 130)
                            ? 'text-hud-error'
                            : crisisState.indicators.usdJpy !== null && crisisState.indicators.usdJpy <= (config?.crisis_usdjpy_warning || 140)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.usdJpy?.toFixed(1) ?? 'N/A'}
                        </div>
                      </div>
                      {/* HY Spread */}
                      <div className="space-y-1 cursor-help" title="High Yield Spread - gap between junk bonds and treasuries. High = corporate default risk rising.">
                        <span className="hud-label">HY SPRD</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.highYieldSpread !== null && crisisState.indicators.highYieldSpread >= (config?.crisis_hy_spread_critical || 600)
                            ? 'text-hud-error'
                            : crisisState.indicators.highYieldSpread !== null && crisisState.indicators.highYieldSpread >= (config?.crisis_hy_spread_warning || 400)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.highYieldSpread?.toFixed(0) ?? 'N/A'}
                        </div>
                      </div>
                      {/* KRE Regional Banks 7D */}
                      <div className="space-y-1 cursor-help" title="Regional Bank ETF weekly change - sharp drops = banking sector stress, potential contagion.">
                        <span className="hud-label">KRE 7D</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.kreWeeklyChange !== null && crisisState.indicators.kreWeeklyChange <= (config?.crisis_kre_weekly_critical || -20)
                            ? 'text-hud-error'
                            : crisisState.indicators.kreWeeklyChange !== null && crisisState.indicators.kreWeeklyChange <= (config?.crisis_kre_weekly_warning || -10)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.kreWeeklyChange !== null ? `${crisisState.indicators.kreWeeklyChange >= 0 ? '+' : ''}${crisisState.indicators.kreWeeklyChange.toFixed(1)}%` : 'N/A'}
                        </div>
                      </div>
                      {/* Fed Balance Sheet Change */}
                      <div className="space-y-1 cursor-help" title="Fed Balance Sheet weekly change - rapid expansion = emergency intervention, something is breaking.">
                        <span className="hud-label">FED BS</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.fedBalanceSheetChange !== null && Math.abs(crisisState.indicators.fedBalanceSheetChange) >= (config?.crisis_fed_balance_change_critical || 5)
                            ? 'text-hud-error'
                            : crisisState.indicators.fedBalanceSheetChange !== null && Math.abs(crisisState.indicators.fedBalanceSheetChange) >= (config?.crisis_fed_balance_change_warning || 2)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.fedBalanceSheetChange !== null ? `${crisisState.indicators.fedBalanceSheetChange >= 0 ? '+' : ''}${crisisState.indicators.fedBalanceSheetChange.toFixed(1)}%` : 'N/A'}
                        </div>
                      </div>
                    </div>

                    {/* Indicators Grid - Row 2: Crypto, Metals, Breadth */}
                    <div className="grid grid-cols-3 md:grid-cols-6 lg:grid-cols-8 gap-3 mt-3">
                      {/* BTC Price */}
                      <div className="space-y-1 cursor-help" title="Bitcoin - risk asset barometer. Sharp drops signal risk-off, liquidity crunch.">
                        <span className="hud-label">BTC</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.btcWeeklyChange !== null && crisisState.indicators.btcWeeklyChange <= (config?.crisis_btc_weekly_drop_pct || -20)
                            ? 'text-hud-error'
                            : crisisState.indicators.btcWeeklyChange !== null && crisisState.indicators.btcWeeklyChange <= -10
                            ? 'text-orange-500'
                            : crisisState.indicators.btcWeeklyChange !== null && crisisState.indicators.btcWeeklyChange < 0
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.btcPrice ? `$${(crisisState.indicators.btcPrice / 1000).toFixed(1)}k` : 'N/A'}
                        </div>
                      </div>
                      {/* BTC 7D */}
                      <div className="space-y-1 cursor-help" title="Bitcoin weekly momentum - large drops often precede or accompany equity selloffs.">
                        <span className="hud-label">BTC 7D</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.btcWeeklyChange !== null && crisisState.indicators.btcWeeklyChange <= (config?.crisis_btc_weekly_drop_pct || -20)
                            ? 'text-hud-error'
                            : crisisState.indicators.btcWeeklyChange !== null && crisisState.indicators.btcWeeklyChange < 0
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.btcWeeklyChange !== null ? `${crisisState.indicators.btcWeeklyChange >= 0 ? '+' : ''}${crisisState.indicators.btcWeeklyChange.toFixed(1)}%` : 'N/A'}
                        </div>
                      </div>
                      {/* USDT Peg */}
                      <div className="space-y-1 cursor-help" title="Tether peg - below $0.985 = stablecoin/banking crisis, crypto contagion risk.">
                        <span className="hud-label">USDT</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.stablecoinPeg !== null && crisisState.indicators.stablecoinPeg < (config?.crisis_stablecoin_depeg_threshold || 0.985)
                            ? 'text-hud-error'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.stablecoinPeg !== null ? `$${crisisState.indicators.stablecoinPeg.toFixed(3)}` : 'N/A'}
                        </div>
                      </div>
                      {/* Gold/Silver Ratio */}
                      <div className="space-y-1 cursor-help" title="Gold/Silver Ratio - low (<60) = silver squeeze, monetary system distrust, inflation fears.">
                        <span className="hud-label">G/S</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.goldSilverRatio !== null && crisisState.indicators.goldSilverRatio < (config?.crisis_gold_silver_ratio_low || 60)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.goldSilverRatio?.toFixed(1) ?? 'N/A'}
                        </div>
                      </div>
                      {/* Silver 7D */}
                      <div className="space-y-1 cursor-help" title="Silver weekly momentum - rapid surge = flight to hard assets, monetary crisis expectations.">
                        <span className="hud-label">SLV 7D</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.silverWeeklyChange !== null && crisisState.indicators.silverWeeklyChange >= (config?.crisis_silver_weekly_critical || 20)
                            ? 'text-hud-error'
                            : crisisState.indicators.silverWeeklyChange !== null && crisisState.indicators.silverWeeklyChange >= (config?.crisis_silver_weekly_warning || 10)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.silverWeeklyChange !== null ? `${crisisState.indicators.silverWeeklyChange >= 0 ? '+' : ''}${crisisState.indicators.silverWeeklyChange.toFixed(1)}%` : 'N/A'}
                        </div>
                      </div>
                      {/* Stocks Above 200MA */}
                      <div className="space-y-1 cursor-help" title="Market Breadth - % of S&P 500 above 200-day MA. Low = broad market weakness, not just a few stocks.">
                        <span className="hud-label">200MA</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.indicators.stocksAbove200MA !== null && crisisState.indicators.stocksAbove200MA < (config?.crisis_stocks_above_200ma_critical || 20)
                            ? 'text-hud-error'
                            : crisisState.indicators.stocksAbove200MA !== null && crisisState.indicators.stocksAbove200MA < (config?.crisis_stocks_above_200ma_warning || 30)
                            ? 'text-hud-warning'
                            : 'text-hud-success'
                        )}>
                          {crisisState.indicators.stocksAbove200MA?.toFixed(0) ?? 'N/A'}%
                        </div>
                      </div>
                      {/* KRE Price */}
                      <div className="space-y-1 cursor-help" title="Regional Bank ETF price - tracks small/mid bank health. First to show banking stress.">
                        <span className="hud-label">KRE</span>
                        <div className="font-mono text-lg text-hud-text">
                          {crisisState.indicators.kre ? `$${crisisState.indicators.kre.toFixed(1)}` : 'N/A'}
                        </div>
                      </div>
                      {/* Trigger Count */}
                      <div className="space-y-1 cursor-help" title="Active crisis triggers - number of indicators currently breaching warning/critical thresholds.">
                        <span className="hud-label">ALERTS</span>
                        <div className={clsx(
                          'font-mono text-lg',
                          crisisState.triggeredIndicators.length >= 3 ? 'text-hud-error' :
                          crisisState.triggeredIndicators.length >= 1 ? 'text-hud-warning' : 'text-hud-success'
                        )}>
                          {crisisState.triggeredIndicators.length}
                        </div>
                      </div>
                    </div>

                    {/* Triggered Indicators */}
                    {crisisState.triggeredIndicators.length > 0 && (
                      <div className="mt-2 p-2 bg-hud-error/10 border border-hud-error/30 rounded">
                        <span className="hud-label text-hud-error">TRIGGERED:</span>
                        <div className="mt-1 text-xs text-hud-error space-y-0.5">
                          {crisisState.triggeredIndicators.map((trigger, idx) => (
                            <div key={idx}>⚠ {trigger}</div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="text-hud-text-dim text-sm">Crisis monitoring initializing...</div>
                )}
              </Panel>
            </div>
          )}

          {/* Row 2: Portfolio Performance Chart */}
          <div className="col-span-4 md:col-span-8 lg:col-span-8">
            <Panel title="PORTFOLIO PERFORMANCE" titleRight="24H" className="h-[320px]">
              {portfolioChartData.length > 1 || dexChartData.length > 1 ? (
                <div className="h-full w-full">
                  <LineChart
                    series={[
                      ...(portfolioChartData.length > 0 ? [{ label: 'Stocks', data: portfolioChartData, variant: totalPl >= 0 ? 'green' as const : 'red' as const }] : []),
                      ...(dexChartData.length > 0 ? [{ label: 'DEX', data: dexChartData, variant: 'yellow' as const }] : [])
                    ]}
                    labels={portfolioChartLabels}
                    showArea={false}
                    showGrid={true}
                    showDots={false}
                    formatValue={(v) => v >= 1000 ? `$${(v / 1000).toFixed(1)}k` : `$${v.toFixed(0)}`}
                  />
                </div>
              ) : (
                <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
                  Collecting performance data...
                </div>
              )}
            </Panel>
          </div>

          <div className="col-span-4 md:col-span-8 lg:col-span-4">
            <Panel title="POSITION PERFORMANCE" titleRight="% CHANGE" className="h-[320px]">
              {positions.length === 0 ? (
                <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
                  No positions to display
                </div>
              ) : normalizedPositionSeries.length > 0 ? (
                <div className="h-full flex flex-col">
                  {/* Legend */}
                  <div className="flex flex-wrap gap-3 mb-2 pb-2 border-b border-hud-line/30 shrink-0">
                    {positions.slice(0, 5).map((pos: Position, idx: number) => {
                      const isPositive = pos.unrealized_pl >= 0
                      const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100
                      const color = positionColors[idx % positionColors.length]
                      return (
                        <div key={pos.symbol} className="flex items-center gap-1.5">
                          <div 
                            className="w-2 h-2 rounded-full" 
                            style={{ backgroundColor: `var(--color-hud-${color})` }}
                          />
                          <span className="hud-value-sm">{pos.symbol}</span>
                          <span className={clsx('hud-label', isPositive ? 'text-hud-success' : 'text-hud-error')}>
                            {formatPercent(plPct)}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                  {/* Stacked chart */}
                  <div className="flex-1 min-h-0 w-full">
                    <LineChart
                      series={normalizedPositionSeries.slice(0, 5)}
                      showArea={false}
                      showGrid={true}
                      showDots={false}
                      animated={false}
                      formatValue={(v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`}
                    />
                  </div>
                </div>
              ) : (
                <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
                  Loading position data...
                </div>
              )}
            </Panel>
          </div>

          {/* DEX Momentum Panel - Row 2.5 */}
          {config?.dex_enabled && ((status?.dexSignals?.length || 0) > 0 || (status?.dexPositions?.length || 0) > 0) && (
            <div className="col-span-4 md:col-span-8 lg:col-span-12">
              <Panel
                title="DEX MOMENTUM (SOLANA)"
                titleRight={
                  <div className="flex items-center gap-4">
                    {status?.dexPaperTrading && (
                      <span className="text-hud-warning text-xs">
                        PAPER: {status.dexPaperTrading.paperBalance?.toFixed(2)} SOL
                        {status.dexPaperTrading.realizedPnL !== 0 && (
                          <span className={status.dexPaperTrading.realizedPnL >= 0 ? 'text-hud-success' : 'text-hud-error'}>
                            {' '}({status.dexPaperTrading.realizedPnL >= 0 ? '+' : ''}{status.dexPaperTrading.realizedPnL.toFixed(3)} SOL)
                          </span>
                        )}
                      </span>
                    )}
                    <span>{status?.dexSignals?.length || 0} signals</span>
                  </div>
                }
              >
                {/* Paper Positions */}
                {(status?.dexPositions?.length || 0) > 0 && (
                  <div className="mb-4">
                    <div className="hud-label text-xs mb-2">PAPER POSITIONS</div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                      {status?.dexPositions?.map((pos: DexPosition) => (
                        <div
                          key={pos.tokenAddress}
                          className="p-2 rounded bg-hud-bg-lighter border border-hud-line/30"
                        >
                          <div className="flex justify-between items-center">
                            <span className="font-semibold text-sm">{pos.symbol}</span>
                            <span className={clsx(
                              'text-sm font-bold',
                              (pos.unrealizedPlPct || 0) >= 0 ? 'text-hud-success' : 'text-hud-error'
                            )}>
                              {(pos.unrealizedPlPct || 0) >= 0 ? '+' : ''}{(pos.unrealizedPlPct || 0).toFixed(1)}%
                            </span>
                          </div>
                          <div className="text-xs text-hud-text-dim mt-1">
                            ${(pos.currentValue || 0).toFixed(2)} • {(pos.holdingHours || 0).toFixed(1)}h
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* DEX Controls */}
                <div className="flex gap-2 mb-4">
                  <button
                    onClick={handleDexClearCooldowns}
                    className="px-3 py-1 text-xs bg-hud-line/30 hover:bg-hud-line/50 border border-hud-line/50 rounded text-hud-text-dim hover:text-hud-text transition-colors"
                  >
                    Clear Cooldowns
                  </button>
                  <button
                    onClick={handleDexClearBreaker}
                    className="px-3 py-1 text-xs bg-hud-line/30 hover:bg-hud-line/50 border border-hud-line/50 rounded text-hud-text-dim hover:text-hud-text transition-colors"
                  >
                    Clear Circuit Breaker
                  </button>
                  <button
                    onClick={handleDexReset}
                    className="px-3 py-1 text-xs bg-hud-error/20 hover:bg-hud-error/30 border border-hud-error/50 rounded text-hud-error/70 hover:text-hud-error transition-colors"
                  >
                    Reset DEX
                  </button>
                </div>

                {/* Signals Table */}
                <div className="hud-label text-xs mb-2">MOMENTUM SIGNALS</div>
                <div className={clsx("overflow-x-auto", (status?.dexSignals?.length || 0) > 10 && "max-h-[400px] overflow-y-auto")}>
                  <table className="w-full">
                    <thead className="sticky top-0 bg-hud-bg">
                      <tr className="border-b border-hud-line/50">
                        <th className="hud-label text-left py-2 px-2">Token</th>
                        <th className="hud-label text-right py-2 px-2">Price</th>
                        <th className="hud-label text-right py-2 px-2">24h</th>
                        <th className="hud-label text-right py-2 px-2">1h</th>
                        <th className="hud-label text-right py-2 px-2">Liquidity</th>
                        <th className="hud-label text-right py-2 px-2">Volume</th>
                        <th className="hud-label text-right py-2 px-2">Score</th>
                        <th className="hud-label text-right py-2 px-2">Age</th>
                      </tr>
                    </thead>
                    <tbody>
                      {status?.dexSignals?.map((sig: DexMomentumSignal) => (
                        <motion.tr
                          key={sig.tokenAddress}
                          initial={{ opacity: 0 }}
                          animate={{ opacity: 1 }}
                          className="border-b border-hud-line/20 hover:bg-hud-line/10 cursor-pointer"
                          onClick={() => window.open(sig.url, '_blank')}
                        >
                          <td className="hud-value-sm py-2 px-2">
                            <span className="text-hud-warning mr-1">◎</span>
                            <span className="font-semibold">{sig.symbol}</span>
                            {sig.tier && (
                              <span className={clsx(
                                'text-[8px] ml-1 px-1 rounded uppercase',
                                sig.tier === 'microspray' && 'bg-hud-purple/30 text-hud-purple',
                                sig.tier === 'breakout' && 'bg-hud-error/30 text-hud-error',
                                sig.tier === 'lottery' && 'bg-hud-success/30 text-hud-success',
                                sig.tier === 'early' && 'bg-hud-cyan/30 text-hud-cyan',
                                sig.tier === 'established' && 'bg-hud-primary/30 text-hud-primary',
                              )}>{sig.tier}</span>
                            )}
                          </td>
                          <td className="hud-value-sm text-right py-2 px-2 text-hud-text-dim">
                            ${sig.priceUsd < 0.01 ? sig.priceUsd.toFixed(6) : sig.priceUsd.toFixed(4)}
                          </td>
                          <td className={clsx(
                            'hud-value-sm text-right py-2 px-2 font-semibold',
                            sig.priceChange24h >= 0 ? 'text-hud-success' : 'text-hud-error'
                          )}>
                            {formatPercent(sig.priceChange24h)}
                          </td>
                          <td className={clsx(
                            'hud-value-sm text-right py-2 px-2',
                            sig.priceChange1h >= 0 ? 'text-hud-success' : 'text-hud-error'
                          )}>
                            {formatPercent(sig.priceChange1h)}
                          </td>
                          <td className="hud-value-sm text-right py-2 px-2">
                            ${(sig.liquidity / 1000).toFixed(0)}k
                          </td>
                          <td className="hud-value-sm text-right py-2 px-2 hidden md:table-cell">
                            ${(sig.volume24h / 1000).toFixed(0)}k
                          </td>
                          <td className="hud-value-sm text-right py-2 px-2">
                            <span className={clsx(
                              sig.momentumScore >= 70 ? 'text-hud-success' :
                              sig.momentumScore >= 50 ? 'text-hud-warning' : 'text-hud-text-dim'
                            )}>
                              {sig.momentumScore.toFixed(0)}
                            </span>
                          </td>
                          <td className="hud-value-sm text-right py-2 px-2 text-hud-text-dim">
                            {sig.ageDays.toFixed(1)}d
                          </td>
                        </motion.tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="text-xs text-hud-text-dim mt-2 text-center">
                  Click row to view on DexScreener
                </div>

                {/* Trade History */}
                {(status?.dexPaperTrading?.recentTrades?.length || 0) > 0 && (
                  <div className="mt-6 pt-4 border-t border-hud-line">
                    <div className="hud-label text-xs mb-2">
                      TRADE HISTORY
                      <span className="text-hud-text-dim ml-2">
                        ({status?.dexPaperTrading?.winningTrades || 0}W / {status?.dexPaperTrading?.losingTrades || 0}L)
                      </span>
                    </div>
                    <div className={clsx("overflow-x-auto", (status?.dexPaperTrading?.recentTrades?.length || 0) > 8 && "max-h-[300px] overflow-y-auto")}>
                      <table className="w-full">
                        <thead className="sticky top-0 bg-hud-bg">
                          <tr className="border-b border-hud-line/50">
                            <th className="hud-label text-left py-2 px-2">Token</th>
                            <th className="hud-label text-right py-2 px-2">P&L</th>
                            <th className="hud-label text-right py-2 px-2">Exit Reason</th>
                            <th className="hud-label text-right py-2 px-2">Hold Time</th>
                            <th className="hud-label text-right py-2 px-2">Time</th>
                          </tr>
                        </thead>
                        <tbody>
                          {status?.dexPaperTrading?.recentTrades?.slice().reverse().map((trade: {
                            symbol: string;
                            tokenAddress: string;
                            entryPrice: number;
                            exitPrice: number;
                            entrySol: number;
                            entryTime: number;
                            exitTime: number;
                            pnlPct: number;
                            pnlSol: number;
                            exitReason: string;
                          }, idx: number) => {
                            const holdTimeHours = (trade.exitTime - trade.entryTime) / (1000 * 60 * 60);
                            const exitTimeAgo = (Date.now() - trade.exitTime) / (1000 * 60);
                            const exitReasonLabel = {
                              'take_profit': '🎯 Take Profit',
                              'stop_loss': '🛑 Stop Loss',
                              'lost_momentum': '📉 Lost Momentum',
                              'trailing_stop': '📊 Trailing Stop',
                              'manual': '✋ Manual',
                            }[trade.exitReason] || trade.exitReason;
                            const exitReasonColor = {
                              'take_profit': 'text-hud-success',
                              'stop_loss': 'text-hud-error',
                              'lost_momentum': 'text-hud-warning',
                              'trailing_stop': 'text-hud-cyan',
                              'manual': 'text-hud-text-dim',
                            }[trade.exitReason] || 'text-hud-text-dim';

                            return (
                              <tr
                                key={`${trade.tokenAddress}-${trade.exitTime}-${idx}`}
                                className="border-b border-hud-line/20 hover:bg-hud-line/10 cursor-pointer"
                                onClick={() => window.open(`https://dexscreener.com/solana/${trade.tokenAddress}`, '_blank')}
                                title={`Entry: $${trade.entryPrice.toFixed(8)} → Exit: $${trade.exitPrice.toFixed(8)}\nSize: ${trade.entrySol.toFixed(4)} SOL\nP&L: ${trade.pnlSol >= 0 ? '+' : ''}${trade.pnlSol.toFixed(4)} SOL`}
                              >
                                <td className="hud-value-sm py-2 px-2">
                                  <span className="text-hud-warning mr-1">◎</span>
                                  <span className="font-semibold">{trade.symbol}</span>
                                </td>
                                <td className={clsx(
                                  'hud-value-sm text-right py-2 px-2 font-semibold',
                                  trade.pnlPct >= 0 ? 'text-hud-success' : 'text-hud-error'
                                )}>
                                  {trade.pnlPct >= 0 ? '+' : ''}{trade.pnlPct.toFixed(1)}%
                                  <span className="text-[10px] text-hud-text-dim ml-1">
                                    ({trade.pnlSol >= 0 ? '+' : ''}{trade.pnlSol.toFixed(3)})
                                  </span>
                                </td>
                                <td className={clsx('hud-value-sm text-right py-2 px-2', exitReasonColor)}>
                                  {exitReasonLabel}
                                </td>
                                <td className="hud-value-sm text-right py-2 px-2 text-hud-text-dim">
                                  {holdTimeHours < 1 ? `${(holdTimeHours * 60).toFixed(0)}m` : `${holdTimeHours.toFixed(1)}h`}
                                </td>
                                <td className="hud-value-sm text-right py-2 px-2 text-hud-text-dim">
                                  {exitTimeAgo < 60 ? `${exitTimeAgo.toFixed(0)}m ago` : `${(exitTimeAgo / 60).toFixed(1)}h ago`}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="text-xs text-hud-text-dim mt-2 text-center">
                      Click row to view token on DexScreener • Hover for entry/exit details
                    </div>
                  </div>
                )}
              </Panel>
            </div>
          )}

          {/* Row 3: Signals, Activity, Research */}
          <div className="col-span-4 md:col-span-4 lg:col-span-4">
            <Panel title="ACTIVE SIGNALS" titleRight={signals.length.toString()} className="h-80">
              <div className="overflow-y-auto h-full space-y-1">
                {signals.length === 0 ? (
                  <div className="text-hud-text-dim text-sm py-4 text-center">Gathering signals...</div>
                ) : (
                  signals.slice(0, 20).map((sig: Signal, i: number) => (
                    <Tooltip
                      key={`${sig.symbol}-${sig.source}-${i}`}
                      position="right"
                      content={
                        <TooltipContent
                          title={`${sig.symbol} - ${sig.source.toUpperCase()}`}
                          items={[
                            { label: 'Sentiment', value: `${(sig.sentiment * 100).toFixed(0)}%`, color: getSentimentColor(sig.sentiment) },
                            { label: 'Volume', value: sig.volume },
                            ...(sig.bullish !== undefined ? [{ label: 'Bullish', value: sig.bullish, color: 'text-hud-success' }] : []),
                            ...(sig.bearish !== undefined ? [{ label: 'Bearish', value: sig.bearish, color: 'text-hud-error' }] : []),
                            ...(sig.score !== undefined ? [{ label: 'Score', value: sig.score }] : []),
                            ...(sig.upvotes !== undefined ? [{ label: 'Upvotes', value: sig.upvotes }] : []),
                            ...(sig.momentum !== undefined ? [{ label: 'Momentum', value: `${sig.momentum >= 0 ? '+' : ''}${sig.momentum.toFixed(2)}%` }] : []),
                            ...(sig.price !== undefined ? [{ label: 'Price', value: formatCurrency(sig.price) }] : []),
                          ]}
                          description={sig.reason}
                        />
                      }
                    >
                      <motion.div 
                        initial={{ opacity: 0, x: -10 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: i * 0.02 }}
                        className={clsx(
                          "flex items-center justify-between py-1 px-2 border-b border-hud-line/10 hover:bg-hud-line/10 cursor-help",
                          sig.isCrypto && "bg-hud-warning/5"
                        )}
                      >
                        <div className="flex items-center gap-2">
                          {sig.isCrypto && <span className="text-hud-warning text-xs">₿</span>}
                          <span className="hud-value-sm">{sig.symbol}</span>
                          <span className={clsx('hud-label', sig.isCrypto ? 'text-hud-warning' : '')}>{sig.source.toUpperCase()}</span>
                        </div>
                        <div className="flex items-center gap-3">
                          {sig.isCrypto && sig.momentum !== undefined ? (
                            <span className={clsx('hud-label hidden sm:inline', sig.momentum >= 0 ? 'text-hud-success' : 'text-hud-error')}>
                              {sig.momentum >= 0 ? '+' : ''}{sig.momentum.toFixed(1)}%
                            </span>
                          ) : (
                            <span className="hud-label hidden sm:inline">VOL {sig.volume}</span>
                          )}
                          <span className={clsx('hud-value-sm', getSentimentColor(sig.sentiment))}>
                            {(sig.sentiment * 100).toFixed(0)}%
                          </span>
                        </div>
                      </motion.div>
                    </Tooltip>
                  ))
                )}
              </div>
            </Panel>
          </div>

          <div className="col-span-4 md:col-span-4 lg:col-span-4">
            <Panel title="ACTIVITY FEED" titleRight="LIVE" className="h-80">
              <div className="overflow-y-auto h-full font-mono text-xs space-y-1">
                {logs.length === 0 ? (
                  <div className="text-hud-text-dim py-4 text-center">Waiting for activity...</div>
                ) : (
                  logs.slice(-50).reverse().map((log: LogEntry, i: number) => (
                    <motion.div 
                      key={`${log.timestamp}-${i}`}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      className="flex items-start gap-2 py-1 border-b border-hud-line/10"
                    >
                      <span className="text-hud-text-dim shrink-0 hidden sm:inline w-[52px]">
                        {new Date(log.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                      </span>
                      <span className={clsx('shrink-0 w-[72px] text-right', getAgentColor(log.agent))}>
                        {log.agent}
                      </span>
                      <span className="text-hud-text flex-1 text-right wrap-break-word">
                        {log.action}
                        {log.symbol && <span className="text-hud-primary ml-1">({log.symbol})</span>}
                      </span>
                    </motion.div>
                  ))
                )}

              </div>
            </Panel>
          </div>

          <div className="col-span-4 md:col-span-8 lg:col-span-4">
            <Panel title="SIGNAL RESEARCH" titleRight={Object.keys(status?.signalResearch || {}).length.toString()} className="h-80">
              <div className="overflow-y-auto h-full space-y-2">
                {Object.entries(status?.signalResearch || {}).length === 0 ? (
                  <div className="text-hud-text-dim text-sm py-4 text-center">Researching candidates...</div>
                ) : (
                  Object.entries(status?.signalResearch || {})
                    .sort(([, a], [, b]) => b.timestamp - a.timestamp)
                    .map(([symbol, research]: [string, SignalResearch]) => (
                    <Tooltip
                      key={symbol}
                      position="left"
                      content={
                        <div className="space-y-2 min-w-[200px]">
                          <div className="hud-label text-hud-primary border-b border-hud-line/50 pb-1">
                            {symbol} DETAILS
                          </div>
                          <div className="space-y-1">
                            <div className="flex justify-between">
                              <span className="text-hud-text-dim">Confidence</span>
                              <span className="text-hud-text-bright">{(research.confidence * 100).toFixed(0)}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-hud-text-dim">Sentiment</span>
                              <span className={getSentimentColor(research.sentiment)}>
                                {(research.sentiment * 100).toFixed(0)}%
                              </span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-hud-text-dim">Analyzed</span>
                              <span className="text-hud-text">
                                {new Date(research.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                              </span>
                            </div>
                          </div>
                          {research.catalysts.length > 0 && (
                            <div className="pt-1 border-t border-hud-line/30">
                              <span className="text-[9px] text-hud-text-dim">CATALYSTS:</span>
                              <ul className="mt-1 space-y-0.5">
                                {research.catalysts.map((c, i) => (
                                  <li key={i} className="text-[10px] text-hud-success">+ {c}</li>
                                ))}
                              </ul>
                            </div>
                          )}
                          {research.red_flags.length > 0 && (
                            <div className="pt-1 border-t border-hud-line/30">
                              <span className="text-[9px] text-hud-text-dim">RED FLAGS:</span>
                              <ul className="mt-1 space-y-0.5">
                                {research.red_flags.map((f, i) => (
                                  <li key={i} className="text-[10px] text-hud-error">- {f}</li>
                                ))}
                              </ul>
                            </div>
                          )}
                        </div>
                      }
                    >
                      <motion.div 
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className="p-2 border border-hud-line/30 rounded hover:border-hud-line/60 cursor-help transition-colors"
                      >
                        <div className="flex justify-between items-center mb-1">
                          <span className="hud-value-sm">{symbol}</span>
                          <div className="flex items-center gap-2">
                            <span className={clsx('hud-label', getQualityColor(research.entry_quality))}>
                              {research.entry_quality.toUpperCase()}
                            </span>
                            <span className={clsx('hud-value-sm font-bold', getVerdictColor(research.verdict))}>
                              {research.verdict}
                            </span>
                          </div>
                        </div>
                        <p className="text-xs text-hud-text-dim leading-tight mb-1">{research.reasoning}</p>
                        {research.red_flags.length > 0 && (
                          <div className="flex flex-wrap gap-1">
                            {research.red_flags.slice(0, 2).map((flag, i) => (
                              <span key={i} className="text-xs text-hud-error bg-hud-error/10 px-1 rounded">
                                {flag.slice(0, 30)}...
                              </span>
                            ))}
                          </div>
                        )}
                      </motion.div>
                    </Tooltip>
                  ))
                )}
              </div>
            </Panel>
          </div>
        </div>

        <footer className="mt-4 pt-3 border-t border-hud-line flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3">
          <div className="flex flex-wrap gap-4 md:gap-6">
            {config && (
              <>
                <MetricInline label="MAX POS" value={`$${config.max_position_value}`} />
                <MetricInline label="MIN SENT" value={`${(config.min_sentiment_score * 100).toFixed(0)}%`} />
                <MetricInline label="TAKE PROFIT" value={`${config.take_profit_pct}%`} />
                <MetricInline label="STOP LOSS" value={`${config.stop_loss_pct}%`} />
                <span className="hidden lg:inline text-hud-line">|</span>
                <MetricInline 
                  label="OPTIONS" 
                  value={config.options_enabled ? 'ON' : 'OFF'} 
                  valueClassName={config.options_enabled ? 'text-hud-purple' : 'text-hud-text-dim'}
                />
                {config.options_enabled && (
                  <>
                    <MetricInline label="OPT Δ" value={config.options_target_delta?.toFixed(2) || '0.35'} />
                    <MetricInline label="OPT DTE" value={`${config.options_min_dte || 7}-${config.options_max_dte || 45}`} />
                  </>
                )}
                <span className="hidden lg:inline text-hud-line">|</span>
                <MetricInline 
                  label="CRYPTO" 
                  value={config.crypto_enabled ? '24/7' : 'OFF'} 
                  valueClassName={config.crypto_enabled ? 'text-hud-warning' : 'text-hud-text-dim'}
                />
                {config.crypto_enabled && (
                  <MetricInline label="SYMBOLS" value={(config.crypto_symbols || ['BTC', 'ETH', 'SOL']).map(s => s.split('/')[0]).join('/')} />
                )}
              </>
            )}
          </div>
          <div className="flex items-center gap-4">
            <span className="hud-label hidden md:inline">AUTONOMOUS TRADING SYSTEM</span>
            <span className="hud-value-sm">PAPER MODE</span>
          </div>
        </footer>
      </div>

      <AnimatePresence>
        {showSettings && config && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <SettingsModal 
              config={config} 
              onSave={handleSaveConfig} 
              onClose={() => setShowSettings(false)} 
            />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
