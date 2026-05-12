"""ASCII table reporter for benchmark results."""
from __future__ import annotations
from .runner import BenchResult


def render_table(results: list[BenchResult], title: str = "", description: str = "") -> str:
    if not results:
        return "(no results)"

    param_name = results[0].param_name
    col_w = max(len(str(r.param_value)) for r in results)
    col_w = max(col_w, len(param_name), 12)

    header = (
        f"  {param_name:<{col_w}} │ "
        f"{'volcano':>10} │ "
        f"{'push':>10} │ "
        f"{'adaptive':>10} │ "
        f"{'push ↑':>7} │ "
        f"{'adpt ↑':>7} │ "
        f"{'winner':<8} │ "
        f"{'rows':>8}"
    )
    sep = "  " + "─" * (col_w) + "─┼─" + "─" * 10 + "─┼─" + "─" * 10 + "─┼─" + "─" * 10 + "─┼─" + "─" * 7 + "─┼─" + "─" * 7 + "─┼─" + "─" * 8 + "─┼─" + "─" * 8

    lines = []
    if title:
        lines.append("")
        lines.append(f"  {'═' * (len(header) - 2)}")
        lines.append(f"  {title}")
        if description:
            lines.append(f"  {description}")
        lines.append(f"  {'═' * (len(header) - 2)}")
    lines.append(header)
    lines.append(sep)

    for r in results:
        winner_marker = {"volcano": "volcano", "push": "push ✓", "adaptive": "adaptive ✓"}[r.winner]
        lines.append(
            f"  {str(r.param_value):<{col_w}} │ "
            f"{r.volcano_ms:>9.2f}ms │ "
            f"{r.push_ms:>9.2f}ms │ "
            f"{r.adaptive_ms:>9.2f}ms │ "
            f"{r.push_speedup:>6.2f}× │ "
            f"{r.adaptive_speedup:>6.2f}× │ "
            f"{winner_marker:<8} │ "
            f"{r.result_rows:>8,}"
        )

    lines.append(sep)

    # Summary line
    winners = [r.winner for r in results]
    push_wins = winners.count("push")
    adpt_wins = winners.count("adaptive")
    volc_wins = winners.count("volcano")
    avg_push_speedup = sum(r.push_speedup for r in results) / len(results)
    avg_adpt_speedup = sum(r.adaptive_speedup for r in results) / len(results)
    lines.append(
        f"  Summary: push wins {push_wins}/{len(results)} "
        f"({avg_push_speedup:.2f}× avg), "
        f"adaptive wins {adpt_wins}/{len(results)} "
        f"({avg_adpt_speedup:.2f}× avg), "
        f"volcano wins {volc_wins}/{len(results)}"
    )

    return "\n".join(lines)


def render_all(scenario_results: dict[str, list[BenchResult]]) -> str:
    from .scenarios import ALL_SCENARIOS
    desc_map = {s.name: s.description for s in ALL_SCENARIOS}

    parts = []
    for name, results in scenario_results.items():
        parts.append(render_table(results, title=name, description=desc_map.get(name, "")))
    return "\n".join(parts) + "\n"
