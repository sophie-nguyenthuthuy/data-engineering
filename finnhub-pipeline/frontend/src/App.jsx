import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useLiveStream } from './useLiveStream.js'
import { PriceChart, VolumeChart } from './components/Charts.jsx'
import { TradesTable } from './components/TradesTable.jsx'

const API_URL = import.meta.env.VITE_API_URL || ''
const DEFAULT_SYMBOLS = ['BINANCE:BTCUSDT', 'BINANCE:ETHUSDT', 'AAPL', 'TSLA', 'NVDA']

export default function App() {
  const [symbols, setSymbols] = useState(DEFAULT_SYMBOLS)
  const [selected, setSelected] = useState(DEFAULT_SYMBOLS[0])

  // Discover which symbols are actually flowing through the pipeline.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const r = await fetch(`${API_URL}/symbols`)
        if (!r.ok) return
        const list = await r.json()
        if (!cancelled && list.length) {
          setSymbols(list)
          if (!list.includes(selected)) setSelected(list[0])
        }
      } catch {}
    })()
    const id = setInterval(() => {
      fetch(`${API_URL}/symbols`).then(r => r.ok && r.json()).then(list => {
        if (!cancelled && Array.isArray(list) && list.length) {
          setSymbols(prev => (prev.length === list.length && prev.every((s,i) => s === list[i])) ? prev : list)
        }
      }).catch(() => {})
    }, 15000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  const { connected, tradesBySymbol, aggsBySymbol, latestAggBySymbol } = useLiveStream(API_URL)

  const trades = tradesBySymbol[selected] || []
  const aggs   = aggsBySymbol[selected]   || []
  const latestAgg = latestAggBySymbol[selected]

  const kpis = useMemo(() => {
    const last = trades[trades.length - 1]
    const first = trades[0]
    const lastPrice = last?.price ?? latestAgg?.avg_price ?? null
    const firstPrice = first?.price ?? null
    const change = (lastPrice != null && firstPrice != null) ? (lastPrice - firstPrice) : null
    const changePct = (change != null && firstPrice) ? (change / firstPrice) * 100 : null

    const totalVol = aggs.reduce((s, a) => s + (a.total_volume || 0), 0)
    const totalTrades = aggs.reduce((s, a) => s + (a.trade_count || 0), 0)
    return {
      price: lastPrice,
      change, changePct,
      totalVol,
      totalTrades,
      vwap: latestAgg?.vwap ?? null,
    }
  }, [trades, aggs, latestAgg])

  return (
    <div className="app">
      <header className="header">
        <h1>📈 Finnhub Live Pipeline</h1>
        <span className={`dot ${connected ? 'live' : 'down'}`} />
        <span className="status">{connected ? 'streaming' : 'reconnecting…'}</span>
        <div className="spacer" />
        <div className="symbol-bar">
          {symbols.map(s => (
            <button
              key={s}
              className={`symbol-chip ${s === selected ? 'active' : ''}`}
              onClick={() => setSelected(s)}
            >
              {s}
            </button>
          ))}
        </div>
      </header>

      <main className="main">
        <section className="panel">
          <div className="kpis">
            <div className="kpi">
              <div className="label">Last</div>
              <div className="value">{fmt(kpis.price, 4)}</div>
            </div>
            <div className="kpi">
              <div className="label">Δ</div>
              <div className={`value ${kpis.change > 0 ? 'up' : kpis.change < 0 ? 'down' : ''}`}>
                {kpis.change == null ? '—' : `${kpis.change > 0 ? '+' : ''}${fmt(kpis.change, 4)} (${fmt(kpis.changePct, 2)}%)`}
              </div>
            </div>
            <div className="kpi">
              <div className="label">VWAP (5s)</div>
              <div className="value">{fmt(kpis.vwap, 4)}</div>
            </div>
            <div className="kpi">
              <div className="label">Volume (window)</div>
              <div className="value">{fmt(kpis.totalVol, 2)}</div>
            </div>
          </div>
          <div className="charts">
            <div className="chart-wrap">
              {trades.length === 0
                ? <div className="empty">waiting for ticks…</div>
                : <PriceChart trades={trades} aggs={aggs} />}
            </div>
            <div className="chart-wrap">
              {aggs.length === 0
                ? <div className="empty">aggregates land every 5s…</div>
                : <VolumeChart aggs={aggs} />}
            </div>
          </div>
        </section>

        <aside className="panel trades">
          <h2>Recent Trades · {selected}</h2>
          <TradesTable trades={trades.slice(-60).reverse()} />
        </aside>
      </main>
    </div>
  )
}

function fmt(n, d = 2) {
  if (n == null || Number.isNaN(n)) return '—'
  return Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
}
