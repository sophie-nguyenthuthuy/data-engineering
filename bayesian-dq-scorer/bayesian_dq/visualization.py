"""
Uncertainty-aware visualizations for Bayesian DQ posteriors.

All public methods return (fig, axes) so callers can save or show them.
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from .models import BatchResult, DQDimension, PosteriorState

# Lazy import matplotlib so the module is importable without a display.
def _mpl():
    import matplotlib.pyplot as plt
    return plt


_DIM_COLORS = {
    DQDimension.COMPLETENESS: "#4C72B0",
    DQDimension.FRESHNESS:    "#DD8452",
    DQDimension.UNIQUENESS:   "#55A868",
}

_DIM_LABELS = {
    DQDimension.COMPLETENESS: "Completeness",
    DQDimension.FRESHNESS:    "Freshness",
    DQDimension.UNIQUENESS:   "Uniqueness",
}


class DQVisualizer:
    """
    Visualization toolkit for BayesianDQScorer outputs.

    Parameters
    ----------
    scorers:
        Dict mapping DQDimension -> dimension scorer instance (has .pdf_curve,
        .credible_interval, .health_threshold).
    figsize_base:
        Base (width, height) per subplot.
    """

    def __init__(self, scorers: dict, figsize_base: tuple[float, float] = (5.5, 4.0)):
        self.scorers = scorers
        self.figsize_base = figsize_base

    # ------------------------------------------------------------------
    # Posterior density plots
    # ------------------------------------------------------------------

    def plot_posteriors(
        self,
        dimensions: Optional[list[DQDimension]] = None,
        title: str = "Posterior Distributions — Data Quality",
    ):
        """
        Side-by-side Beta posterior PDFs for each dimension.
        Shaded region = P(healthy); vertical dashed = health threshold.
        """
        plt = _mpl()
        dims = dimensions or list(self.scorers)
        n = len(dims)
        fig, axes = plt.subplots(
            1, n,
            figsize=(self.figsize_base[0] * n, self.figsize_base[1]),
            constrained_layout=True,
        )
        if n == 1:
            axes = [axes]

        for ax, dim in zip(axes, dims):
            scorer = self.scorers[dim]
            state = scorer.state
            x, y = scorer.pdf_curve(n_points=500)
            color = _DIM_COLORS[dim]
            threshold = scorer.health_threshold

            ax.plot(x, y, color=color, lw=2.0, label="Posterior PDF")
            ax.fill_between(
                x, y,
                where=(x >= threshold),
                alpha=0.25,
                color=color,
                label=f"P(healthy) = {scorer.p_healthy():.3f}",
            )
            ax.axvline(threshold, color="crimson", ls="--", lw=1.4,
                       label=f"Threshold = {threshold}")

            lo, hi = scorer.credible_interval(0.95)
            ax.axvspan(lo, hi, alpha=0.07, color=color, label=f"95% CI [{lo:.2f}, {hi:.2f}]")

            ax.set_title(f"{_DIM_LABELS[dim]}\nμ={state.mean:.3f}  σ={state.std:.3f}  n_batches={state.batch_count}")
            ax.set_xlabel("Quality Rate")
            ax.set_ylabel("Density")
            ax.set_xlim(0, 1)
            ax.legend(fontsize=8, loc="upper left")
            ax.grid(alpha=0.3)

        fig.suptitle(title, fontsize=13, fontweight="bold")
        return fig, axes

    # ------------------------------------------------------------------
    # P(healthy) time series
    # ------------------------------------------------------------------

    def plot_p_healthy_over_time(
        self,
        results: list[BatchResult],
        dimensions: Optional[list[DQDimension]] = None,
        alert_threshold: float = 0.20,
        title: str = "P(healthy) Over Time",
    ):
        """
        Line chart of P(healthy) per batch, with alert band shaded.
        """
        plt = _mpl()
        dims = dimensions or list(_DIM_COLORS)
        fig, ax = plt.subplots(figsize=(max(8, len(results) * 0.4 + 2), 4.5), constrained_layout=True)

        batch_ids = [r.batch_id for r in results]
        x = np.arange(len(batch_ids))

        for dim in dims:
            color = _DIM_COLORS[dim]
            ys = [r.p_healthy.get(dim, float("nan")) for r in results]
            ax.plot(x, ys, color=color, lw=2, marker="o", ms=5, label=_DIM_LABELS[dim])

        ax.axhline(alert_threshold, color="crimson", ls="--", lw=1.3,
                   label=f"Alert threshold = {alert_threshold}")
        ax.fill_between(x, 0, alert_threshold, alpha=0.06, color="crimson")

        # Mark alert-firing batches
        for i, r in enumerate(results):
            if r.alerts_fired:
                ax.axvline(i, color="crimson", alpha=0.4, lw=1.0)

        ax.set_xticks(x)
        ax.set_xticklabels(batch_ids, rotation=45, ha="right", fontsize=8)
        ax.set_ylim(-0.02, 1.05)
        ax.set_ylabel("P(healthy)")
        ax.set_xlabel("Batch")
        ax.set_title(title, fontweight="bold")
        ax.legend(fontsize=9)
        ax.grid(alpha=0.3)
        return fig, ax

    # ------------------------------------------------------------------
    # Posterior mean + uncertainty band time series
    # ------------------------------------------------------------------

    def plot_posterior_mean_over_time(
        self,
        results: list[BatchResult],
        dimensions: Optional[list[DQDimension]] = None,
        title: str = "Posterior Mean ± Uncertainty Over Batches",
    ):
        """
        Rolling posterior mean with ±1σ and 95% CI bands.
        """
        plt = _mpl()
        dims = dimensions or list(_DIM_COLORS)
        n = len(dims)
        fig, axes = plt.subplots(
            n, 1,
            figsize=(max(8, len(results) * 0.4 + 2), 3.5 * n),
            constrained_layout=True,
            sharex=True,
        )
        if n == 1:
            axes = [axes]

        batch_ids = [r.batch_id for r in results]
        x = np.arange(len(batch_ids))

        for ax, dim in zip(axes, dims):
            color = _DIM_COLORS[dim]
            means = [r.posteriors[dim].mean for r in results if dim in r.posteriors]
            stds  = [r.posteriors[dim].std  for r in results if dim in r.posteriors]
            xi    = x[:len(means)]

            means = np.array(means)
            stds  = np.array(stds)

            ax.plot(xi, means, color=color, lw=2, marker="o", ms=4, label="Posterior mean")
            ax.fill_between(xi, means - stds, means + stds,
                            alpha=0.30, color=color, label="±1σ")
            ax.fill_between(xi, means - 2 * stds, means + 2 * stds,
                            alpha=0.12, color=color, label="±2σ  (≈95% CI)")

            threshold = self.scorers[dim].health_threshold
            ax.axhline(threshold, color="crimson", ls="--", lw=1.2,
                       label=f"Health threshold = {threshold}")
            ax.set_ylabel(_DIM_LABELS[dim])
            ax.set_ylim(0, 1.05)
            ax.legend(fontsize=8, loc="lower right")
            ax.grid(alpha=0.3)

        axes[-1].set_xticks(x)
        axes[-1].set_xticklabels(batch_ids, rotation=45, ha="right", fontsize=8)
        axes[-1].set_xlabel("Batch")
        fig.suptitle(title, fontsize=13, fontweight="bold")
        return fig, axes

    # ------------------------------------------------------------------
    # Dashboard: all three panels combined
    # ------------------------------------------------------------------

    def dashboard(
        self,
        results: list[BatchResult],
        alert_threshold: float = 0.20,
        output_path: Optional[str] = None,
    ):
        """
        Three-panel dashboard: posteriors | P(healthy) series | mean+CI series.
        Saves to output_path if provided, otherwise shows interactively.
        """
        plt = _mpl()
        fig_post, _ = self.plot_posteriors()
        fig_ph, _   = self.plot_p_healthy_over_time(results, alert_threshold=alert_threshold)
        fig_mu, _   = self.plot_posterior_mean_over_time(results)

        if output_path:
            from matplotlib.backends.backend_pdf import PdfPages
            with PdfPages(output_path) as pdf:
                pdf.savefig(fig_post)
                pdf.savefig(fig_ph)
                pdf.savefig(fig_mu)
            plt.close("all")
            return output_path
        else:
            plt.show()
            return (fig_post, fig_ph, fig_mu)
