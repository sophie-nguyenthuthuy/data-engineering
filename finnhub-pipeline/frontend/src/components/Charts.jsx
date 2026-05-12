import React, { useMemo } from 'react'
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid,
  BarChart, Bar,
} from 'recharts'

const GRID = '#1f2a42'
const AXIS = '#6b7590'
const LINE = '#5cd0a3'
const VWAP = '#64a8ff'
const BAR  = '#64a8ff'

export function PriceChart({ trades, aggs }) {
  // Downsample heavily — Recharts gets laggy past a few hundred points.
  const data = useMemo(() => {
    const step = Math.max(1, Math.floor(trades.length / 300))
    const sampled = []
    for (let i = 0; i < trades.length; i += step) sampled.push(trades[i])
    if (trades.length && sampled[sampled.length - 1] !== trades[trades.length - 1]) {
      sampled.push(trades[trades.length - 1])
    }
    return sampled
  }, [trades])

  const vwapSeries = useMemo(() => aggs.map(a => ({ t: a.t, vwap: a.vwap })), [aggs])
  const merged = useMemo(() => {
    const byT = new Map()
    for (const d of data)        byT.set(d.t, { t: d.t, price: d.price })
    for (const d of vwapSeries)  byT.set(d.t, { ...(byT.get(d.t) || { t: d.t }), vwap: d.vwap })
    return [...byT.values()].sort((a, b) => a.t - b.t)
  }, [data, vwapSeries])

  const [min, max] = useMemo(() => {
    const prices = data.map(d => d.price)
    if (!prices.length) return [0, 1]
    const lo = Math.min(...prices)
    const hi = Math.max(...prices)
    const pad = (hi - lo) * 0.1 || hi * 0.001 || 1
    return [lo - pad, hi + pad]
  }, [data])

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={merged} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
        <CartesianGrid stroke={GRID} strokeDasharray="3 3" />
        <XAxis
          dataKey="t"
          type="number"
          domain={['dataMin', 'dataMax']}
          tickFormatter={formatTime}
          stroke={AXIS}
          tick={{ fontSize: 11 }}
          minTickGap={60}
        />
        <YAxis
          domain={[min, max]}
          stroke={AXIS}
          tick={{ fontSize: 11 }}
          width={72}
          tickFormatter={v => v.toLocaleString(undefined, { maximumFractionDigits: 4 })}
        />
        <Tooltip content={<ChartTooltip />} />
        <Line type="monotone" dataKey="price" stroke={LINE} strokeWidth={1.6} dot={false} isAnimationActive={false} connectNulls />
        <Line type="stepAfter" dataKey="vwap" stroke={VWAP} strokeWidth={1.2} strokeDasharray="4 3" dot={false} isAnimationActive={false} connectNulls />
      </LineChart>
    </ResponsiveContainer>
  )
}

export function VolumeChart({ aggs }) {
  const data = aggs.slice(-60)
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
        <CartesianGrid stroke={GRID} strokeDasharray="3 3" vertical={false} />
        <XAxis
          dataKey="t"
          type="number"
          domain={['dataMin', 'dataMax']}
          tickFormatter={formatTime}
          stroke={AXIS}
          tick={{ fontSize: 11 }}
          minTickGap={60}
        />
        <YAxis stroke={AXIS} tick={{ fontSize: 11 }} width={72} />
        <Tooltip content={<ChartTooltip kind="bar" />} />
        <Bar dataKey="total_volume" fill={BAR} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  )
}

function formatTime(t) {
  const d = new Date(t)
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function ChartTooltip({ active, payload, label, kind }) {
  if (!active || !payload?.length) return null
  const p = payload[0].payload
  return (
    <div style={{
      background: '#121826', border: '1px solid #222c42', borderRadius: 6,
      padding: '6px 10px', fontSize: 12, color: '#e5e7eb',
    }}>
      <div style={{ color: '#8b93a7', fontSize: 11 }}>{formatTime(label)}</div>
      {kind === 'bar' ? (
        <>
          <div>volume: {fmt(p.total_volume)}</div>
          <div>trades: {p.trade_count}</div>
          <div>vwap: {fmt(p.vwap, 4)}</div>
        </>
      ) : (
        <>
          {p.price != null && <div>price: {fmt(p.price, 4)}</div>}
          {p.vwap != null  && <div>vwap: {fmt(p.vwap, 4)}</div>}
        </>
      )}
    </div>
  )
}

function fmt(n, d = 2) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
}
