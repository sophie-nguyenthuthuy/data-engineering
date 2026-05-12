interface Props {
  consumed: number;
  total: number;
  size?: "sm" | "md" | "lg";
}

const colors = {
  low: "#22c55e",
  mid: "#f59e0b",
  high: "#ef4444",
};

export default function BudgetGauge({ consumed, total, size = "md" }: Props) {
  const pct = total > 0 ? Math.min(100, (consumed / total) * 100) : 0;
  const color = pct >= 100 ? colors.high : pct >= 75 ? colors.mid : colors.low;
  const h = size === "sm" ? "h-1.5" : size === "lg" ? "h-4" : "h-2.5";

  return (
    <div className="w-full">
      <div className={`w-full bg-gray-200 rounded-full ${h} overflow-hidden`}>
        <div
          className={`${h} rounded-full transition-all duration-500`}
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
      <div className="flex justify-between mt-1 text-xs text-gray-500">
        <span>ε {consumed.toFixed(3)} used</span>
        <span>{pct.toFixed(1)}%</span>
        <span>ε {total} total</span>
      </div>
    </div>
  );
}
