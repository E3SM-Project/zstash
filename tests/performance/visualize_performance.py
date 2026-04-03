#!/usr/bin/env python3
"""
visualize_performance.py  –  Plot zstash performance profiling results.

Usage:
    python visualize_performance.py

Edit the constants at the top of this file to point at the CSV(s) to plot.

The CSV is produced by performance_profile.sh and has columns:
    test_label, create_subdir, update_subdir, hpss_label, operation, elapsed_seconds

Visualization strategy
----------------------
Four dimensions:
  1. Operation  : create | update | extract_seq | extract_par
  2. Directory  : build/ (many small) | run/ (medium) | init/ (few large)
  3. HPSS mode  : none | hpss | globus
  4. Parallelism: already encoded in operation (extract_seq vs extract_par)

Figure 1 – Performance overview:
  Layout: 2×2 grid of subplots, one per operation.
  Within each subplot:
    - X-axis groups  = directory processed (create_subdir or update_subdir)
                       for create/update; or (create_subdir, update_subdir)
                       archive config for extract_seq/extract_par.
    - Bars           = HPSS mode (none / hpss / globus), colour-coded
    - Each test config contributes one bar per (directory, hpss_mode) cell;
      if multiple configs share the same directory for an operation, their
      runtimes are shown as individual dots and the bar shows the mean.
  An additional 5th subplot compares extract_seq vs extract_par side-by-side
  to make the parallelism speed-up immediately visible.

Figure 2 – Baseline comparison (current branch vs main):
  Produced only when BASELINE_RESULTS_CSV is set to a valid path.
  Same 2×2 + comparison layout, but each cell shows two bars
  (current = solid, baseline = hatched) with a ratio annotation
  (current/baseline) above each pair. Ratio > 1 = regression (slower),
  ratio < 1 = improvement (faster).
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# ← EDIT THESE for each new run
# ---------------------------------------------------------------------------

# The results to show in Fig. 1
RESULTS_CSV: str = (
    "/pscratch/sd/f/forsyth/zstash_performance/performance_20260402/results.csv"
)

# The results to compare against in Fig. 2.
# Set to None to skip Fig. 2.
BASELINE_RESULTS_CSV: Optional[str] = None

# Output path for the saved figures.
# Set to None to display interactively instead of saving.
OUTPUT_PATH: Optional[str] = (
    "/global/cfs/cdirs/e3sm/www/forsyth/zstash_performance/performance__20260402_pr427.png"
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HPSS_ORDER = ["none", "hpss", "globus"]
HPSS_COLORS = {"none": "#4C72B0", "hpss": "#DD8452", "globus": "#55A868"}
HPSS_LABELS = {"none": "No HPSS", "hpss": "Direct HPSS", "globus": "Globus"}

OP_ORDER = ["create", "update", "extract_seq", "extract_par"]
OP_TITLES = {
    "create": "zstash create",
    "update": "zstash update",
    "extract_seq": "zstash extract  (sequential, 1 worker)",
    "extract_par": "zstash extract  (parallel, 2 workers)",
}

# Map an operation to the column that holds the "relevant directory".
# Extract is intentionally absent: it operates on the combined create+update
# archive, so both subdirs are needed and it is handled separately.
OP_DIR_COL = {
    "create": "create_subdir",
    "update": "update_subdir",
}

BAR_WIDTH = 0.22
DOT_ALPHA = 0.55
DOT_SIZE = 40


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df["elapsed_seconds"] = pd.to_numeric(df["elapsed_seconds"], errors="coerce")
    # Normalise subdir names: strip trailing slashes for display
    for col in ("create_subdir", "update_subdir"):
        df[col] = df[col].str.strip().str.rstrip("/")
    df["hpss_label"] = df["hpss_label"].str.strip()
    df["operation"] = df["operation"].str.strip()
    return df


def dir_sort_key(name: str) -> int:
    """Sort dirs in a consistent order: build, run, init."""
    order = {"build": 0, "run": 1, "init": 2}
    return order.get(name.lower(), 99)


def _add_dir_annotation(ax, dirs, x_positions):
    """
    Add a small file-count hint below each directory group label.

    Parameters
    ----------
    ax          : the Axes to annotate
    dirs        : list of directory names in display order
    x_positions : list of x-axis data coordinates for each dir group centre.
                  These are passed in explicitly so the function works for both
                  Fig. 1 (groups at 0, 1, 2, …) and Fig. 2 (wider group_span).
    """
    hints = {
        "build": "many small files\n(~7k files, 1.2 GiB)",
        "run": "mixed\n(~111 files, 11 GiB)",
        "init": "few large files\n(14 files, 6.9 GiB)",
    }
    for x_centre, d in zip(x_positions, dirs):
        if d in hints:
            ax.annotate(
                hints[d],
                xy=(x_centre, 0),
                xycoords=("data", "axes fraction"),
                xytext=(0, -46),
                textcoords="offset points",
                ha="center",
                va="top",
                fontsize=6.5,
                color="#555555",
                annotation_clip=False,
            )


def plot_operation(ax, df_op: pd.DataFrame, operation: str, dirs: list[str]):
    """Draw grouped bars for one operation subplot."""
    dir_col = OP_DIR_COL[operation]
    n_dirs = len(dirs)
    n_hpss = len(HPSS_ORDER)

    x_base = np.arange(n_dirs)
    offsets = np.linspace(-(n_hpss - 1) / 2, (n_hpss - 1) / 2, n_hpss) * BAR_WIDTH

    for h_idx, hpss in enumerate(HPSS_ORDER):
        df_h = df_op[df_op["hpss_label"] == hpss]
        means, all_vals, xs = [], [], []

        for d_idx, d in enumerate(dirs):
            vals = df_h[df_h[dir_col] == d]["elapsed_seconds"].dropna().values
            mean = vals.mean() if len(vals) > 0 else 0.0
            means.append(mean)
            all_vals.append(vals)
            xs.append(x_base[d_idx] + offsets[h_idx])

        color = HPSS_COLORS[hpss]
        ax.bar(
            xs,
            means,
            width=BAR_WIDTH,
            color=color,
            alpha=0.85,
            label=HPSS_LABELS[hpss],
            zorder=2,
        )
        # Overlay individual data points so scatter is visible
        for x_pos, vals in zip(xs, all_vals):
            if len(vals) > 1:
                jitter = np.random.uniform(
                    -BAR_WIDTH * 0.25, BAR_WIDTH * 0.25, size=len(vals)
                )
                ax.scatter(
                    x_pos + jitter,
                    vals,
                    color="white",
                    edgecolors=color,
                    s=DOT_SIZE,
                    zorder=3,
                    alpha=DOT_ALPHA,
                    linewidths=1.2,
                )

    ax.set_title(OP_TITLES[operation], fontsize=10, fontweight="bold", pad=6)
    ax.set_xticks(x_base)
    ax.set_xticklabels([d + "/" for d in dirs], fontsize=9)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel("Directory processed", fontsize=8, labelpad=14)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    # Fig. 1: group centres are at integer positions 0, 1, 2, …
    _add_dir_annotation(ax, dirs, list(x_base))

    # Value labels on bars
    for rect in ax.patches:
        h = rect.get_height()
        if h > 0:
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                h * 1.01,
                f"{h:.0f}s",
                ha="center",
                va="bottom",
                fontsize=6,
                color="#333333",
            )


def _extract_configs(df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Return the sorted list of (create_subdir, update_subdir) pairs that
    actually appear in the extract rows of *df*.  These represent the
    combined archives that were extracted from.
    """
    mask = df["operation"].isin(["extract_seq", "extract_par"])
    pairs = (
        df[mask][["create_subdir", "update_subdir"]]
        .drop_duplicates()
        .apply(tuple, axis=1)
        .tolist()
    )
    # Sort by create_subdir first, then update_subdir
    return sorted(pairs, key=lambda p: (dir_sort_key(p[0]), dir_sort_key(p[1])))


def _extract_tick_label(create_sub: str, update_sub: str) -> str:
    """Short two-line tick label for a (create, update) archive config."""
    return f"create: {create_sub}/\nupdate: {update_sub}/"


def _plot_extract_single_op(ax, df: pd.DataFrame, operation: str):
    """
    Draw grouped bars for one extract operation (extract_seq or extract_par).
    X-axis groups are the combined (create_subdir, update_subdir) archive
    configs, since extract operates on the archive built by both operations.
    """
    configs = _extract_configs(df)
    n_configs = len(configs)
    n_hpss = len(HPSS_ORDER)

    x_base = np.arange(n_configs, dtype=float)
    offsets = np.linspace(-(n_hpss - 1) / 2, (n_hpss - 1) / 2, n_hpss) * BAR_WIDTH

    for h_idx, hpss in enumerate(HPSS_ORDER):
        means, all_vals, xs = [], [], []
        for c_idx, (create_sub, update_sub) in enumerate(configs):
            vals = (
                df[
                    (df["operation"] == operation)
                    & (df["hpss_label"] == hpss)
                    & (df["create_subdir"] == create_sub)
                    & (df["update_subdir"] == update_sub)
                ]["elapsed_seconds"]
                .dropna()
                .values
            )
            mean = vals.mean() if len(vals) > 0 else 0.0
            means.append(mean)
            all_vals.append(vals)
            xs.append(x_base[c_idx] + offsets[h_idx])

        color = HPSS_COLORS[hpss]
        ax.bar(
            xs,
            means,
            width=BAR_WIDTH,
            color=color,
            alpha=0.85,
            label=HPSS_LABELS[hpss],
            zorder=2,
        )
        for x_pos, vals in zip(xs, all_vals):
            if len(vals) > 1:
                jitter = np.random.uniform(
                    -BAR_WIDTH * 0.25, BAR_WIDTH * 0.25, size=len(vals)
                )
                ax.scatter(
                    x_pos + jitter,
                    vals,
                    color="white",
                    edgecolors=color,
                    s=DOT_SIZE,
                    zorder=3,
                    alpha=DOT_ALPHA,
                    linewidths=1.2,
                )

    ax.set_title(OP_TITLES[operation], fontsize=10, fontweight="bold", pad=6)
    ax.set_xticks(x_base)
    ax.set_xticklabels([_extract_tick_label(c, u) for c, u in configs], fontsize=7)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel("Archive contents (create → update)", fontsize=8, labelpad=6)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)

    for rect in ax.patches:
        h = rect.get_height()
        if h > 0:
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                h * 1.01,
                f"{h:.0f}s",
                ha="center",
                va="bottom",
                fontsize=6,
                color="#333333",
            )


def plot_extract_comparison(ax, df: pd.DataFrame):
    """
    Extra subplot: sequential vs parallel extract, grouped by
    (archive config, hpss).  Each archive config is the *combined*
    create+update directory pair, since extract operates on the full
    archive assembled by both operations.
    Uses a hatch pattern to distinguish seq/par within each hpss colour.
    """
    configs = _extract_configs(df)
    n_configs = len(configs)
    ops = ["extract_seq", "extract_par"]
    hatches = {"extract_seq": "", "extract_par": "////"}
    n_bars = len(HPSS_ORDER) * len(ops)

    group_width = n_bars * BAR_WIDTH + 0.15  # total width per config group
    x_base = np.arange(n_configs) * group_width

    for c_idx, (create_sub, update_sub) in enumerate(configs):
        for h_idx, hpss in enumerate(HPSS_ORDER):
            for op_idx, op in enumerate(ops):
                df_cell = df[
                    (df["operation"] == op)
                    & (df["hpss_label"] == hpss)
                    & (df["create_subdir"] == create_sub)
                    & (df["update_subdir"] == update_sub)
                ]
                vals = df_cell["elapsed_seconds"].dropna().values
                mean = vals.mean() if len(vals) > 0 else 0.0
                bar_x = x_base[c_idx] + (h_idx * len(ops) + op_idx) * BAR_WIDTH
                ax.bar(
                    bar_x,
                    mean,
                    width=BAR_WIDTH,
                    color=HPSS_COLORS[hpss],
                    hatch=hatches[op],
                    alpha=0.85,
                    zorder=2,
                )

    tick_positions = x_base + (n_bars / 2 - 0.5) * BAR_WIDTH
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([_extract_tick_label(c, u) for c, u in configs], fontsize=7.5)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel(
        "Archive contents (create subdir → update subdir)", fontsize=8, labelpad=14
    )
    ax.set_title(
        "Extract: Sequential vs Parallel (speed-up comparison)\n"
        "Each group = archive built from create subdir + update subdir",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)

    # Custom legend: colour = hpss, hatch = seq/par
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h]) for h in HPSS_ORDER
    ]
    seq_patch = mpatches.Patch(
        facecolor="grey", hatch="", label="Sequential (1 worker)"
    )
    par_patch = mpatches.Patch(
        facecolor="grey", hatch="////", label="Parallel (2 workers)"
    )
    ax.legend(
        handles=hpss_patches + [seq_patch, par_patch],
        fontsize=7,
        loc="upper right",
        ncol=2,
    )


# ---------------------------------------------------------------------------
# Baseline comparison figure
# ---------------------------------------------------------------------------

# Ratio colouring thresholds
RATIO_REGRESSION = 1.10  # >= 10% slower  → red
RATIO_IMPROVEMENT = 0.90  # <= 10% faster  → green
RATIO_NEUTRAL_COLOR = "#333333"
RATIO_REGRESSION_COLOR = "#CC3311"
RATIO_IMPROVEMENT_COLOR = "#228833"


def _ratio_color(ratio: float) -> str:
    if ratio >= RATIO_REGRESSION:
        return RATIO_REGRESSION_COLOR
    if ratio <= RATIO_IMPROVEMENT:
        return RATIO_IMPROVEMENT_COLOR
    return RATIO_NEUTRAL_COLOR


def plot_comparison_operation(
    ax,
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
    operation: str,
    dirs: list[str],
):
    """
    For one operation, draw paired bars (current vs baseline) per
    (directory, hpss_mode) cell, with a ratio annotation above each pair.
    """
    dir_col = OP_DIR_COL[operation]
    n_dirs = len(dirs)
    n_hpss = len(HPSS_ORDER)

    # Each hpss group occupies 2 bars (current + baseline) + a small gap
    pair_width = BAR_WIDTH
    gap = BAR_WIDTH * 0.3
    group_span = n_hpss * (2 * pair_width + gap) + 0.2
    x_base = np.arange(n_dirs) * group_span

    for h_idx, hpss in enumerate(HPSS_ORDER):
        color = HPSS_COLORS[hpss]
        pair_offset = h_idx * (2 * pair_width + gap)

        for d_idx, d in enumerate(dirs):
            x_left = x_base[d_idx] + pair_offset  # current bar
            x_right = x_base[d_idx] + pair_offset + pair_width  # baseline bar

            def mean_for(df, _op=operation, _h=hpss, _d=d):
                v = (
                    df[
                        (df["operation"] == _op)
                        & (df["hpss_label"] == _h)
                        & (df[dir_col] == _d)
                    ]["elapsed_seconds"]
                    .dropna()
                    .values
                )
                return v.mean() if len(v) > 0 else 0.0

            cur_mean = mean_for(df_cur)
            bas_mean = mean_for(df_bas)

            # Current bar (solid)
            ax.bar(
                x_left,
                cur_mean,
                width=pair_width,
                color=color,
                alpha=0.85,
                zorder=2,
                label=HPSS_LABELS[hpss] if d_idx == 0 else "",
            )
            # Baseline bar (hatched, lighter)
            ax.bar(
                x_right,
                bas_mean,
                width=pair_width,
                color=color,
                alpha=0.40,
                hatch="////",
                zorder=2,
                edgecolor=color,
            )

            # Ratio annotation
            if bas_mean > 0 and cur_mean > 0:
                ratio = cur_mean / bas_mean
                top = max(cur_mean, bas_mean)
                rat_color = _ratio_color(ratio)
                arrow = (
                    "▲"
                    if ratio >= RATIO_REGRESSION
                    else ("▼" if ratio <= RATIO_IMPROVEMENT else "")
                )
                ax.text(
                    (x_left + x_right) / 2,
                    top * 1.03,
                    f"{arrow}{ratio:.2f}×",
                    ha="center",
                    va="bottom",
                    fontsize=6.5,
                    fontweight="bold",
                    color=rat_color,
                    zorder=4,
                )

    ax.set_title(OP_TITLES[operation], fontsize=10, fontweight="bold", pad=6)
    # Tick at the centre of each directory's group of bars
    group_centre_offset = (n_hpss * (2 * pair_width + gap) - gap) / 2
    x_ticks = x_base + group_centre_offset
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([d + "/" for d in dirs], fontsize=9)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel("Directory processed", fontsize=8, labelpad=14)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    # Pass actual tick x-positions so annotations align with tick labels
    _add_dir_annotation(ax, dirs, list(x_ticks))


def _plot_comparison_extract_single_op(
    ax,
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
    operation: str,
):
    """
    Fig. 2 version of a single extract-op subplot (extract_seq or extract_par).
    X-axis = combined (create, update) archive config; bars = current vs baseline
    paired within each HPSS group.
    """
    configs = _extract_configs(df_cur)
    n_configs = len(configs)
    n_hpss = len(HPSS_ORDER)

    pair_width = BAR_WIDTH
    gap = BAR_WIDTH * 0.3
    group_span = n_hpss * (2 * pair_width + gap) + 0.2
    x_base = np.arange(n_configs) * group_span

    for h_idx, hpss in enumerate(HPSS_ORDER):
        color = HPSS_COLORS[hpss]
        pair_offset = h_idx * (2 * pair_width + gap)
        for c_idx, (create_sub, update_sub) in enumerate(configs):
            x_left = x_base[c_idx] + pair_offset
            x_right = x_left + pair_width

            def mean_for(df, _op=operation, _h=hpss, _cs=create_sub, _us=update_sub):
                v = (
                    df[
                        (df["operation"] == _op)
                        & (df["hpss_label"] == _h)
                        & (df["create_subdir"] == _cs)
                        & (df["update_subdir"] == _us)
                    ]["elapsed_seconds"]
                    .dropna()
                    .values
                )
                return v.mean() if len(v) > 0 else 0.0

            cur_mean = mean_for(df_cur)
            bas_mean = mean_for(df_bas)

            ax.bar(
                x_left,
                cur_mean,
                width=pair_width,
                color=color,
                alpha=0.85,
                zorder=2,
                label=HPSS_LABELS[hpss] if c_idx == 0 else "",
            )
            ax.bar(
                x_right,
                bas_mean,
                width=pair_width,
                color=color,
                alpha=0.40,
                hatch="////",
                zorder=2,
                edgecolor=color,
            )

            if bas_mean > 0 and cur_mean > 0:
                ratio = cur_mean / bas_mean
                top = max(cur_mean, bas_mean)
                arrow = (
                    "▲"
                    if ratio >= RATIO_REGRESSION
                    else ("▼" if ratio <= RATIO_IMPROVEMENT else "")
                )
                ax.text(
                    (x_left + x_right) / 2,
                    top * 1.03,
                    f"{arrow}{ratio:.2f}×",
                    ha="center",
                    va="bottom",
                    fontsize=6.5,
                    fontweight="bold",
                    color=_ratio_color(ratio),
                    zorder=4,
                )

    group_centre_offset = (n_hpss * (2 * pair_width + gap) - gap) / 2
    x_ticks = x_base + group_centre_offset
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([_extract_tick_label(c, u) for c, u in configs], fontsize=7)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel("Archive contents (create → update)", fontsize=8, labelpad=6)
    ax.set_title(OP_TITLES[operation], fontsize=10, fontweight="bold", pad=6)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)


def plot_comparison_extract(
    ax,
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
):
    """
    Seq vs par extract comparison with current/baseline pairing, grouped by
    the combined (create_subdir, update_subdir) archive config.

    Bar order within each HPSS × op cell (innermost grouping):
        [current/seq] [baseline/seq] ‹op_gap› [current/par] [baseline/par]
    """
    configs = _extract_configs(df_cur)
    n_configs = len(configs)
    ops = ["extract_seq", "extract_par"]
    op_hatches = {"extract_seq": "", "extract_par": "xxxx"}

    pair_width = BAR_WIDTH
    inner_gap = BAR_WIDTH * 0.15  # gap between current/baseline within a pair
    op_gap = BAR_WIDTH * 0.55  # larger gap between seq-pair and par-pair
    hpss_gap = BAR_WIDTH * 0.30  # gap between HPSS groups

    pair_span = 2 * pair_width + inner_gap
    hpss_group_span = 2 * pair_span + op_gap

    group_span = len(HPSS_ORDER) * (hpss_group_span + hpss_gap) + 0.3
    x_base = np.arange(n_configs) * group_span

    for c_idx, (create_sub, update_sub) in enumerate(configs):
        for h_idx, hpss in enumerate(HPSS_ORDER):
            color = HPSS_COLORS[hpss]
            hpss_origin = x_base[c_idx] + h_idx * (hpss_group_span + hpss_gap)
            for op_idx, op in enumerate(ops):
                hatch = op_hatches[op]
                op_origin = hpss_origin + op_idx * (pair_span + op_gap)
                x_cur = op_origin
                x_bas = op_origin + pair_width + inner_gap

                def mean_for(df, _op=op, _h=hpss, _cs=create_sub, _us=update_sub):
                    v = (
                        df[
                            (df["operation"] == _op)
                            & (df["hpss_label"] == _h)
                            & (df["create_subdir"] == _cs)
                            & (df["update_subdir"] == _us)
                        ]["elapsed_seconds"]
                        .dropna()
                        .values
                    )
                    return v.mean() if len(v) > 0 else 0.0

                cur_mean = mean_for(df_cur)
                bas_mean = mean_for(df_bas)

                ax.bar(
                    x_cur,
                    cur_mean,
                    width=pair_width,
                    color=color,
                    hatch=hatch,
                    alpha=0.85,
                    zorder=2,
                )
                ax.bar(
                    x_bas,
                    bas_mean,
                    width=pair_width,
                    color=color,
                    hatch=hatch,
                    alpha=0.35,
                    zorder=2,
                    edgecolor=color,
                )

                if bas_mean > 0 and cur_mean > 0:
                    ratio = cur_mean / bas_mean
                    top = max(cur_mean, bas_mean)
                    arrow = (
                        "▲"
                        if ratio >= RATIO_REGRESSION
                        else ("▼" if ratio <= RATIO_IMPROVEMENT else "")
                    )
                    ax.text(
                        (x_cur + x_bas) / 2 + pair_width / 2,
                        top * 1.03,
                        f"{arrow}{ratio:.2f}×",
                        ha="center",
                        va="bottom",
                        fontsize=5.5,
                        fontweight="bold",
                        color=_ratio_color(ratio),
                        zorder=4,
                    )

    group_total_bar_span = len(HPSS_ORDER) * (hpss_group_span + hpss_gap) - hpss_gap
    x_ticks = x_base + group_total_bar_span / 2
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([_extract_tick_label(c, u) for c, u in configs], fontsize=7.5)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel(
        "Archive contents (create subdir → update subdir)", fontsize=8, labelpad=14
    )
    ax.set_title(
        "Extract: Sequential vs Parallel — current vs baseline\n"
        "Each group = archive built from create subdir + update subdir",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)

    # Legend: colour=hpss, hatch=seq/par, alpha=current/baseline
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h]) for h in HPSS_ORDER
    ]
    seq_patch = mpatches.Patch(
        facecolor="grey", hatch="", label="Sequential (1 worker)"
    )
    par_patch = mpatches.Patch(
        facecolor="grey", hatch="xxxx", label="Parallel (2 workers)"
    )
    cur_patch = mpatches.Patch(facecolor="grey", alpha=0.85, label="Current branch")
    bas_patch = mpatches.Patch(
        facecolor="grey", alpha=0.35, label="Baseline (main)", hatch="////"
    )
    ax.legend(
        handles=hpss_patches + [seq_patch, par_patch, cur_patch, bas_patch],
        fontsize=6.5,
        loc="upper right",
        ncol=3,
    )


def build_comparison_figure(
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
    all_dirs: list[str],
    cur_label: str,
    bas_label: str,
) -> plt.Figure:
    """Build and return the full baseline-comparison figure."""
    fig = plt.figure(figsize=(16, 17))
    fig.suptitle(
        f"zstash Performance: Current vs Baseline\n"
        f"current = {cur_label}   |   baseline (main) = {bas_label}\n"
        f"Ratio = current / baseline  —  "
        f"▲ {RATIO_REGRESSION_COLOR_LABEL} ≥{RATIO_REGRESSION:.0%} slower  "
        f"▼ {RATIO_IMPROVEMENT_COLOR_LABEL} ≤{RATIO_IMPROVEMENT:.0%} faster  "
        f"= within ±10%",
        fontsize=11,
        fontweight="bold",
        y=0.98,
    )

    gs = fig.add_gridspec(
        3, 2, hspace=0.58, wspace=0.35, top=0.92, bottom=0.07, left=0.07, right=0.97
    )

    axes = {
        "create": fig.add_subplot(gs[0, 0]),
        "update": fig.add_subplot(gs[0, 1]),
        "extract_seq": fig.add_subplot(gs[1, 0]),
        "extract_par": fig.add_subplot(gs[1, 1]),
    }
    ax_cmp = fig.add_subplot(gs[2, :])

    for op in OP_ORDER:
        if op in OP_DIR_COL:
            plot_comparison_operation(axes[op], df_cur, df_bas, op, all_dirs)
        else:
            _plot_comparison_extract_single_op(axes[op], df_cur, df_bas, op)

    # Shared legend for solid=current, hatched=baseline
    cur_patch = mpatches.Patch(facecolor="grey", alpha=0.85, label="Current branch")
    bas_patch = mpatches.Patch(
        facecolor="grey", alpha=0.40, hatch="////", label="Baseline (main)"
    )
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h]) for h in HPSS_ORDER
    ]
    axes["create"].legend(
        handles=[cur_patch, bas_patch] + hpss_patches,
        fontsize=7,
        loc="upper right",
    )

    plot_comparison_extract(ax_cmp, df_cur, df_bas)
    return fig


# String labels used in the suptitle (avoids referencing undefined vars earlier)
RATIO_REGRESSION_COLOR_LABEL = "red"
RATIO_IMPROVEMENT_COLOR_LABEL = "green"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Visualise zstash performance results."
    )
    parser.add_argument(
        "--dpi", type=int, default=150, help="Output DPI (default: 150)"
    )
    args = parser.parse_args()

    df = load_data(RESULTS_CSV)

    if df.empty:
        print("ERROR: CSV is empty or could not be parsed.", file=sys.stderr)
        sys.exit(1)

    # Determine the sorted list of directories that actually appear in the data
    all_dirs = sorted(
        set(df["create_subdir"].dropna()) | set(df["update_subdir"].dropna()),
        key=dir_sort_key,
    )

    # -----------------------------------------------------------------------
    # Figure layout: 3 rows × 2 cols
    #   Row 0: create  |  update
    #   Row 1: extract_seq  |  extract_par
    #   Row 2: extract seq-vs-par comparison (spans both columns)
    # -----------------------------------------------------------------------
    fig = plt.figure(figsize=(15, 16))
    fig.suptitle(
        "zstash Performance Profiling\n"
        "(bars = mean over test configs; dots = individual runs)",
        fontsize=13,
        fontweight="bold",
        y=0.98,
    )

    gs = fig.add_gridspec(
        3, 2, hspace=0.55, wspace=0.35, top=0.93, bottom=0.07, left=0.07, right=0.97
    )

    axes = {
        "create": fig.add_subplot(gs[0, 0]),
        "update": fig.add_subplot(gs[0, 1]),
        "extract_seq": fig.add_subplot(gs[1, 0]),
        "extract_par": fig.add_subplot(gs[1, 1]),
    }
    ax_cmp = fig.add_subplot(gs[2, :])

    # -----------------------------------------------------------------------
    # Draw the four single-operation subplots
    # -----------------------------------------------------------------------
    legend_handles = None
    for op in OP_ORDER:
        ax = axes[op]
        if op in OP_DIR_COL:
            df_op = df[df["operation"] == op]
            plot_operation(ax, df_op, op, all_dirs)
        else:
            # extract_seq / extract_par each get a dedicated single-op view
            # that still uses the combined archive config as the x-axis.
            _plot_extract_single_op(ax, df, op)

        if legend_handles is None:
            legend_handles = [
                mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h])
                for h in HPSS_ORDER
            ]
            ax.legend(handles=legend_handles, fontsize=7, loc="upper right")

    # -----------------------------------------------------------------------
    # Draw the sequential vs parallel comparison subplot
    # -----------------------------------------------------------------------
    plot_extract_comparison(ax_cmp, df)

    # -----------------------------------------------------------------------
    # Baseline comparison figure (Figure 2)
    # -----------------------------------------------------------------------
    fig_cmp = None
    if BASELINE_RESULTS_CSV is not None:
        bas_path = Path(BASELINE_RESULTS_CSV)
        if not bas_path.exists():
            print(
                f"WARNING: BASELINE_RESULTS_CSV not found: {bas_path}", file=sys.stderr
            )
            print("Skipping baseline comparison figure.", file=sys.stderr)
        else:
            df_bas = load_data(str(bas_path))
            # Derive a short label from the CSV path for titles,
            # e.g. ".../performance_20260101/results.csv" → "performance_20260101"
            bas_label = bas_path.parent.name
            cur_label = Path(RESULTS_CSV).parent.name
            fig_cmp = build_comparison_figure(
                df, df_bas, all_dirs, cur_label, bas_label
            )

    # -----------------------------------------------------------------------
    # Save or show
    # -----------------------------------------------------------------------
    def save_or_show(figure, out_path_str, label):
        if out_path_str:
            out_path = Path(out_path_str)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            figure.savefig(out_path, dpi=args.dpi, bbox_inches="tight")
            print(f"{label} saved to: {out_path}")
            os.chmod(out_path_str, 0o755)
            web_path = str(out_path).replace(
                "/global/cfs/cdirs/e3sm/www/",
                "https://portal.nersc.gov/cfs/e3sm/",
            )
            print(f"  Accessible at: {web_path}")
        else:
            plt.show()

    if OUTPUT_PATH:
        save_or_show(fig, OUTPUT_PATH, "Figure 1 (overview)")
        if fig_cmp is not None:
            p = Path(OUTPUT_PATH)
            cmp_output = str(p.with_stem(p.stem + "_vs_baseline"))
            save_or_show(fig_cmp, cmp_output, "Figure 2 (baseline comparison)")
    else:
        plt.show()


if __name__ == "__main__":
    np.random.seed(42)  # reproducible jitter
    main()
