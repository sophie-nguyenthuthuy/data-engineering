import { useEffect, useRef, useState } from 'react'

const MAX_TRADES = 400    // per symbol, trimmed as new ticks arrive
const MAX_AGGS = 120      // ~10 min of 5s bars

/**
 * Opens a single WebSocket to /ws and buckets messages by symbol.
 * Exposes three maps plus a connected flag. State updates are batched
 * via requestAnimationFrame so the UI doesn't re-render on every tick.
 */
export function useLiveStream(apiUrl) {
  const [connected, setConnected] = useState(false)
  const [tradesBySymbol, setTradesBySymbol] = useState({})
  const [aggsBySymbol, setAggsBySymbol] = useState({})
  const [latestAggBySymbol, setLatestAggBySymbol] = useState({})

  const bufTrades = useRef({})
  const bufAggs   = useRef({})
  const bufLatest = useRef({})
  const scheduled = useRef(false)

  useEffect(() => {
    let ws
    let retries = 0
    let stopped = false

    const wsUrl = (() => {
      const base = apiUrl || window.location.origin
      const u = new URL(base, window.location.origin)
      u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:'
      u.pathname = '/ws'
      return u.toString()
    })()

    const flush = () => {
      scheduled.current = false
      if (Object.keys(bufTrades.current).length) {
        const buf = bufTrades.current; bufTrades.current = {}
        setTradesBySymbol(prev => {
          const next = { ...prev }
          for (const [sym, items] of Object.entries(buf)) {
            const arr = (next[sym] || []).concat(items)
            if (arr.length > MAX_TRADES) arr.splice(0, arr.length - MAX_TRADES)
            next[sym] = arr
          }
          return next
        })
      }
      if (Object.keys(bufAggs.current).length) {
        const buf = bufAggs.current; bufAggs.current = {}
        setAggsBySymbol(prev => {
          const next = { ...prev }
          for (const [sym, items] of Object.entries(buf)) {
            const arr = (next[sym] || []).concat(items)
            if (arr.length > MAX_AGGS) arr.splice(0, arr.length - MAX_AGGS)
            next[sym] = arr
          }
          return next
        })
      }
      if (Object.keys(bufLatest.current).length) {
        const buf = bufLatest.current; bufLatest.current = {}
        setLatestAggBySymbol(prev => ({ ...prev, ...buf }))
      }
    }

    const schedule = () => {
      if (!scheduled.current) {
        scheduled.current = true
        requestAnimationFrame(flush)
      }
    }

    const connect = () => {
      if (stopped) return
      ws = new WebSocket(wsUrl)
      ws.binaryType = 'arraybuffer'

      ws.onopen = () => {
        setConnected(true)
        retries = 0
      }

      ws.onmessage = (ev) => {
        let text
        if (typeof ev.data === 'string') text = ev.data
        else text = new TextDecoder().decode(ev.data)
        let msg
        try { msg = JSON.parse(text) } catch { return }
        if (msg.type === 'trade') {
          const t = msg.data
          if (!t?.symbol) return
          ;(bufTrades.current[t.symbol] ||= []).push({
            t: t.ts_ms,
            price: t.price,
            volume: t.volume,
          })
        } else if (msg.type === 'agg') {
          const a = msg.data
          if (!a?.symbol) return
          const bar = {
            t: new Date(a.window_start).getTime(),
            window_start: a.window_start,
            window_end: a.window_end,
            trade_count: a.trade_count,
            avg_price: a.avg_price,
            min_price: a.min_price,
            max_price: a.max_price,
            total_volume: a.total_volume,
            vwap: a.vwap,
          }
          ;(bufAggs.current[a.symbol] ||= []).push(bar)
          bufLatest.current[a.symbol] = bar
        }
        schedule()
      }

      ws.onclose = () => {
        setConnected(false)
        if (stopped) return
        const delay = Math.min(500 * 2 ** retries, 5000)
        retries += 1
        setTimeout(connect, delay)
      }
      ws.onerror = () => ws.close()
    }

    connect()
    return () => {
      stopped = true
      if (ws && ws.readyState <= 1) ws.close()
    }
  }, [apiUrl])

  return { connected, tradesBySymbol, aggsBySymbol, latestAggBySymbol }
}
