import { motion } from 'motion/react'
import { useState, useRef } from 'react'

type ChartVariant = 'cyan' | 'blue' | 'green' | 'yellow' | 'red' | 'purple' | 'primary'

interface LineChartSeries {
  label: string
  data: number[]
  variant?: ChartVariant
}

interface ChartMarker {
  index: number
  label: string
  color?: string
}

interface LineChartProps {
  series: LineChartSeries[]
  labels?: string[]
  variant?: ChartVariant
  height?: number
  showDots?: boolean
  showGrid?: boolean
  showArea?: boolean
  animated?: boolean
  formatValue?: (value: number) => string
  markers?: ChartMarker[]
  onHover?: (index: number | null, value: number | null) => void
}

const variantColors: Record<ChartVariant, { stroke: string; fill: string }> = {
  cyan: { stroke: 'var(--color-hud-cyan)', fill: 'var(--color-hud-cyan)' },
  blue: { stroke: 'var(--color-hud-blue)', fill: 'var(--color-hud-blue)' },
  green: { stroke: 'var(--color-hud-green)', fill: 'var(--color-hud-green)' },
  yellow: { stroke: 'var(--color-hud-yellow)', fill: 'var(--color-hud-yellow)' },
  red: { stroke: 'var(--color-hud-red)', fill: 'var(--color-hud-red)' },
  purple: { stroke: 'var(--color-hud-purple)', fill: 'var(--color-hud-purple)' },
  primary: { stroke: 'var(--color-hud-primary)', fill: 'var(--color-hud-primary)' },
}

export function LineChart({
  series,
  labels,
  variant = 'cyan',
  height,
  showDots = false,
  showGrid = true,
  showArea = true,
  animated = true,
  formatValue,
  markers,
}: LineChartProps) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  const viewBoxWidth = 800
  const viewBoxHeight = height || 200
  const padding = { top: 16, right: 16, bottom: 24, left: 48 }
  const chartWidth = viewBoxWidth - padding.left - padding.right
  const chartHeight = viewBoxHeight - padding.top - padding.bottom

  const allValues = series.flatMap((s) => s.data)
  const dataMin = Math.min(...allValues)
  const dataMax = Math.max(...allValues)
  const range = dataMax - dataMin || 1
  const minValue = dataMin - range * 0.05
  const maxValue = dataMax + range * 0.05
  const valueRange = maxValue - minValue || 1

  const maxPoints = Math.max(...series.map((s) => s.data.length), 1)

  const getX = (index: number) => padding.left + (index / (maxPoints - 1 || 1)) * chartWidth
  const getY = (value: number) => padding.top + chartHeight - ((value - minValue) / valueRange) * chartHeight
  const getIndexFromX = (x: number) => Math.round(((x - padding.left) / chartWidth) * (maxPoints - 1))

  const gridLines = 4
  const gridValues = Array.from({ length: gridLines }, (_, i) => minValue + (valueRange / (gridLines - 1)) * i)

  const formatLabel = formatValue || ((v: number) => {
    if (Math.abs(v) >= 1000) return `${(v / 1000).toFixed(1)}k`
    return v.toFixed(0)
  })

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!svgRef.current) return
    const rect = svgRef.current.getBoundingClientRect()
    const scaleX = viewBoxWidth / rect.width
    const x = (e.clientX - rect.left) * scaleX
    const index = getIndexFromX(x)
    if (index >= 0 && index < maxPoints) {
      setHoverIndex(index)
    } else {
      setHoverIndex(null)
    }
  }

  const handleMouseLeave = () => setHoverIndex(null)

  const hoverValues = hoverIndex !== null
    ? series.map(s => ({ label: s.label, value: s.data[hoverIndex], variant: s.variant })).filter(v => v.value !== undefined)
    : []
  const hoverLabel = hoverIndex !== null && labels ? labels[hoverIndex] : null

  return (
    <svg
      ref={svgRef}
      width="100%"
      height="100%"
      viewBox={`0 0 ${viewBoxWidth} ${viewBoxHeight}`}
      preserveAspectRatio="xMidYMid meet"
      className="block"
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      {showGrid && (
        <g>
          {gridValues.map((value, i) => (
            <g key={i}>
              <line
                x1={padding.left}
                y1={getY(value)}
                x2={viewBoxWidth - padding.right}
                y2={getY(value)}
                stroke="currentColor"
                className="text-hud-border"
                strokeWidth={0.5}
                opacity={0.3}
              />
              <text
                x={padding.left - 6}
                y={getY(value)}
                textAnchor="end"
                dominantBaseline="middle"
                fill="currentColor"
                className="text-hud-text-dim"
                fontSize={9}
              >
                {formatLabel(value)}
              </text>
            </g>
          ))}
        </g>
      )}

      {labels && (
        <g>
          {labels.filter((_, i) => i % Math.ceil(labels.length / 6) === 0).map((label, i) => {
            const actualIndex = i * Math.ceil(labels.length / 6)
            return (
              <text
                key={i}
                x={getX(actualIndex)}
                y={viewBoxHeight - 6}
                textAnchor="middle"
                fill="currentColor"
                className="text-hud-text-dim"
                fontSize={9}
              >
                {label}
              </text>
            )
          })}
        </g>
      )}

      {markers && markers.map((marker, i) => (
        <g key={`marker-${i}`}>
          <line
            x1={getX(marker.index)}
            y1={padding.top}
            x2={getX(marker.index)}
            y2={padding.top + chartHeight}
            stroke={marker.color || 'var(--color-hud-text-dim)'}
            strokeWidth={1}
            strokeDasharray="4,4"
            opacity={0.4}
          />
          <text
            x={getX(marker.index)}
            y={padding.top - 4}
            textAnchor="middle"
            fill={marker.color || 'var(--color-hud-text-dim)'}
            fontSize={8}
          >
            {marker.label}
          </text>
        </g>
      ))}

      {series.map((s, seriesIndex) => {
        const colors = variantColors[s.variant ?? variant]
        const points = s.data.map((value, i) => ({ x: getX(i), y: getY(value) }))
        if (points.length === 0) return null

        const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ')
        const areaD = `${pathD} L ${points[points.length - 1]?.x ?? 0} ${padding.top + chartHeight} L ${points[0]?.x ?? 0} ${padding.top + chartHeight} Z`

        return (
          <g key={seriesIndex}>
            {showArea && (
              <defs>
                <linearGradient id={`area-gradient-${seriesIndex}`} x1="0%" y1="0%" x2="0%" y2="100%">
                  <stop offset="0%" stopColor={colors.fill} stopOpacity={0.2} />
                  <stop offset="100%" stopColor={colors.fill} stopOpacity={0} />
                </linearGradient>
              </defs>
            )}

            {showArea && (
              <motion.path
                d={areaD}
                fill={`url(#area-gradient-${seriesIndex})`}
                initial={animated ? { opacity: 0 } : undefined}
                animate={{ opacity: 1 }}
                transition={{ duration: 0.8 }}
              />
            )}

            <motion.path
              d={pathD}
              fill="none"
              stroke={colors.stroke}
              strokeWidth={1.5}
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity={0.8}
              initial={animated ? { pathLength: 0 } : undefined}
              animate={{ pathLength: 1 }}
              transition={{ duration: 1.2, ease: [0.25, 0.46, 0.45, 0.94] }}
            />

            {showDots &&
              points.map((p, i) => (
                <motion.circle
                  key={i}
                  cx={p.x}
                  cy={p.y}
                  r={2}
                  fill={colors.fill}
                  opacity={0.8}
                  initial={animated ? { scale: 0 } : undefined}
                  animate={{ scale: 1 }}
                  transition={{ delay: i * 0.03, duration: 0.2 }}
                />
              ))}
          </g>
        )
      })}

      {hoverIndex !== null && hoverValues.length > 0 && (
        <g>
          <line
            x1={getX(hoverIndex)}
            y1={padding.top}
            x2={getX(hoverIndex)}
            y2={padding.top + chartHeight}
            stroke="var(--color-hud-text-dim)"
            strokeWidth={1}
            opacity={0.6}
          />
          {/* Dots for each series at hover point */}
          {hoverValues.map((hv, i) => (
            <circle
              key={i}
              cx={getX(hoverIndex)}
              cy={getY(hv.value)}
              r={4}
              fill="var(--color-hud-bg)"
              stroke={variantColors[hv.variant ?? variant].stroke}
              strokeWidth={2}
            />
          ))}
          {/* Tooltip box */}
          <g transform={`translate(${Math.min(getX(hoverIndex) + 8, viewBoxWidth - 100)}, ${Math.max(padding.top, 10)})`}>
            <rect
              x={0}
              y={0}
              width={92}
              height={16 + hoverValues.length * 14 + (hoverLabel ? 14 : 0)}
              fill="var(--color-hud-bg)"
              stroke="var(--color-hud-line)"
              strokeWidth={1}
              rx={2}
            />
            {hoverLabel && (
              <text x={8} y={12} fill="var(--color-hud-text-dim)" fontSize={9}>
                {hoverLabel}
              </text>
            )}
            {hoverValues.map((hv, i) => (
              <g key={i}>
                <circle
                  cx={8}
                  cy={(hoverLabel ? 24 : 12) + i * 14 - 3}
                  r={3}
                  fill={variantColors[hv.variant ?? variant].stroke}
                />
                <text
                  x={16}
                  y={(hoverLabel ? 24 : 12) + i * 14}
                  fill="var(--color-hud-text)"
                  fontSize={9}
                >
                  {hv.label}: {formatLabel(hv.value)}
                </text>
              </g>
            ))}
          </g>
        </g>
      )}
    </svg>
  )
}

// Mini sparkline chart for inline use
interface SparklineProps {
  data: number[]
  width?: number
  height?: number
  variant?: ChartVariant
  showChange?: boolean
}

export function Sparkline({
  data,
  width = 80,
  height = 24,
}: SparklineProps) {
  if (data.length < 2) return null

  const padding = 2
  const chartWidth = width - padding * 2
  const chartHeight = height - padding * 2

  const minValue = Math.min(...data)
  const maxValue = Math.max(...data)
  const valueRange = maxValue - minValue || 1

  const points = data.map((value, i) => ({
    x: padding + (i / (data.length - 1)) * chartWidth,
    y: padding + chartHeight - ((value - minValue) / valueRange) * chartHeight,
  }))

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ')
  const isPositive = data[data.length - 1] >= data[0]

  return (
    <svg width={width} height={height}>
      <path
        d={pathD}
        fill="none"
        stroke={isPositive ? variantColors.green.stroke : variantColors.red.stroke}
        strokeWidth={1}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}
