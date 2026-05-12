import React, { useMemo } from 'react'

export function TradesTable({ trades }) {
  // Direction relative to the previous trade (reversed input: newest first).
  const rows = useMemo(() => {
    const out = []
    for (let i = 0; i < trades.length; i++) {
      const t = trades[i]
      const next = trades[i + 1]
      const dir = !next ? 0 : Math.sign(t.price - next.price)
      out.push({ ...t, dir })
    }
    return out
  }, [trades])

  if (!rows.length) return <div className="empty">no trades yet</div>

  return (
    <table>
      <thead>
        <tr>
          <th style={{ textAlign: 'left' }}>time</th>
          <th>price</th>
          <th>vol</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={`${r.t}-${i}`} className={r.dir > 0 ? 'up' : r.dir < 0 ? 'down' : ''}>
            <td className="sym">{formatTime(r.t)}</td>
            <td className="px">{fmt(r.price, 4)}</td>
            <td>{fmt(r.volume, 2)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function formatTime(t) {
  const d = new Date(t)
  return d.toLocaleTimeString(undefined, { hour12: false })
}
function fmt(n, d = 2) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
}
