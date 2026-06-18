#!/usr/bin/env python3
"""
visualize_performance.py  –  Plot zstash performance profiling results.

Usage:
    python visualize_performance.py [--cfg path/to/perf.cfg] [--dpi 150]

Pass --cfg <file> (default: perf.cfg next to this script) instead of editing
hard-coded constants.  The cfg file uses the same key=value format as
generate_performance_data.bash and can be shared between both scripts.

The CSV is produced by generate_performance_data.bash and has columns:
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
  Produced only when baseline_results_csv is set to a valid path in the cfg.
  Same 2×2 + comparison layout, but each cell shows two bars
  (current = solid, baseline = hatched) with a ratio annotation
  (current/baseline) above each pair. Ratio > 1 = regression (slower),
  ratio < 1 = improvement (faster).

Figure 3 – Full record archive for create & update
  (all historical CSVs in performance_archive_dir):
  Produced only when performance_archive_dir is set in the cfg and contains
  *results*.csv files with YYYYMMDD in their names.
  Layout: 2×2 grid (create | update) × (time-series | box plot).
  Time-series: x = record date, y = runtime, color = hpss mode,
               line style = subdir (solid=build, dashed=run, dotted=init).
  Box plots: vertical box-and-whisker for each (subdir, hpss) combination,
             with individual data-point dots overlaid.

Figure 4 – Full record archive for extract_seq & extract_par:
  Produced only when performance_archive_dir is set and contains extract data.
  Layout: 2×2 grid (extract_seq | extract_par) × (time-series | box plot).
  X-axis groups for box plots: (create_subdir, update_subdir) archive config pairs.
  Same color/line-style encoding as Figure 3.

Outlier removal
---------------
All plotting functions apply IQR-based outlier filtering before computing
means or drawing boxes/lines.  Values outside
  [Q1 - 1.5 * IQR,  Q3 + 1.5 * IQR]
are dropped silently.  This prevents a single aberrant run from dominating
axis scales while preserving legitimate spread.
"""

import argparse
import configparser
import datetime
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import matplotlib.dates
import matplotlib.lines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Cfg-file helpers
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).parent


def _load_cfg(cfg_path: Path) -> dict:
    """
    Parse a key=value cfg file (same format used by generate_performance_data.bash).
    Returns a plain dict.  Section headers are not required; if present they are
    ignored so the same file can be shared between the bash script and this one.
    """
    # configparser needs at least one section header; inject a fake one.
    text = "[run]\n" + cfg_path.read_text()
    cp = configparser.ConfigParser(
        inline_comment_prefixes=("#",),
        strict=False,
    )
    cp.read_string(text)
    return dict(cp["run"])


def _cfg_optional(cfg: dict, key: str) -> Optional[str]:
    """Return the value for *key*, or None if missing / blank."""
    v = cfg.get(key, "").strip()
    return v if v else None


def _cfg_require(cfg: dict, key: str, cfg_path: Path) -> str:
    v = _cfg_optional(cfg, key)
    if v is None:
        print(
            f"ERROR: required key '{key}' is missing from {cfg_path}",
            file=sys.stderr,
        )
        sys.exit(1)
    return v


# ---------------------------------------------------------------------------
# Config  (styling – not user-configurable)
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
# Figure 3/4 – per-subdir line styles (encode which directory is plotted)
# ---------------------------------------------------------------------------
# build/ = many small files  →  solid
# run/   = mixed             →  dashed
# init/  = few large files   →  dotted
SUBDIR_LINESTYLES: dict[str, str] = {
    "build": "solid",
    "run": "dashed",
    "init": "dotted",
}
SUBDIR_ORDER = ["build", "run", "init"]


# ---------------------------------------------------------------------------
# Outlier removal
# ---------------------------------------------------------------------------


def remove_outliers_iqr(vals: np.ndarray, k: float = 1.5) -> np.ndarray:
    """
    Return a copy of *vals* with IQR-based outliers removed.

    Values outside [Q1 - k*IQR,  Q3 + k*IQR] are dropped.
    Returns the original array unchanged when it has fewer than 4 elements
    (too few to estimate quartiles reliably).
    """
    if len(vals) < 4:
        return vals
    q1, q3 = np.percentile(vals, [25, 75])
    iqr = q3 - q1
    lo = q1 - k * iqr
    hi = q3 + k * iqr
    return vals[(vals >= lo) & (vals <= hi)]


def _filter_df_outliers(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Apply IQR outlier removal to elapsed_seconds within each group defined
    by *group_cols*.  Returns a new DataFrame with outlier rows dropped.
    Duplicate values that survive the IQR filter are all retained; only values
    that fall outside the fence are removed.
    """
    keep = []
    for _, grp in df.groupby(group_cols, dropna=False):
        vals = grp["elapsed_seconds"].dropna().values
        clean = remove_outliers_iqr(vals)
        clean_counts = Counter(clean.tolist())
        used: Counter = Counter()
        row_mask = []
        for v in grp["elapsed_seconds"]:
            if pd.isna(v):
                row_mask.append(False)
                continue
            if used[v] < clean_counts[v]:
                row_mask.append(True)
                used[v] += 1
            else:
                row_mask.append(False)
        keep.append(grp[row_mask])
    if not keep:
        return df.iloc[0:0]
    return pd.concat(keep, ignore_index=True)


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


def plot_operation(ax, df_op: pd.DataFrame, operation: str, dirs: list):
    """Draw grouped bars for one operation subplot (outliers removed)."""
    dir_col = OP_DIR_COL[operation]
    df_op = _filter_df_outliers(df_op.copy(), [dir_col, "hpss_label"])

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


def _extract_configs(df: pd.DataFrame) -> list:
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
    return sorted(pairs, key=lambda p: (dir_sort_key(p[0]), dir_sort_key(p[1])))


def _extract_tick_label(create_sub: str, update_sub: str) -> str:
    """Short two-line tick label for a (create, update) archive config."""
    return f"create: {create_sub}/\nupdate: {update_sub}/"


def _plot_extract_single_op(ax, df: pd.DataFrame, operation: str):
    """
    Draw grouped bars for one extract operation (extract_seq or extract_par).
    Outliers removed per (create_subdir, update_subdir, hpss_label) group.
    """
    df_op = df[df["operation"] == operation].copy()
    df_op = _filter_df_outliers(df_op, ["create_subdir", "update_subdir", "hpss_label"])

    configs = _extract_configs(df)
    n_configs = len(configs)
    n_hpss = len(HPSS_ORDER)

    x_base = np.arange(n_configs, dtype=float)
    offsets = np.linspace(-(n_hpss - 1) / 2, (n_hpss - 1) / 2, n_hpss) * BAR_WIDTH

    for h_idx, hpss in enumerate(HPSS_ORDER):
        means, all_vals, xs = [], [], []
        for c_idx, (create_sub, update_sub) in enumerate(configs):
            vals = (
                df_op[
                    (df_op["hpss_label"] == hpss)
                    & (df_op["create_subdir"] == create_sub)
                    & (df_op["update_subdir"] == update_sub)
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
    Extra subplot: sequential vs parallel extract, grouped by (archive config, hpss).
    Outliers removed per (operation, create_subdir, update_subdir, hpss_label).
    """
    df_ext = df[df["operation"].isin(["extract_seq", "extract_par"])].copy()
    df_ext = _filter_df_outliers(
        df_ext, ["operation", "create_subdir", "update_subdir", "hpss_label"]
    )

    configs = _extract_configs(df)
    n_configs = len(configs)
    ops = ["extract_seq", "extract_par"]
    hatches = {"extract_seq": "", "extract_par": "////"}
    n_bars = len(HPSS_ORDER) * len(ops)

    group_width = n_bars * BAR_WIDTH + 0.15
    x_base = np.arange(n_configs) * group_width

    for c_idx, (create_sub, update_sub) in enumerate(configs):
        for h_idx, hpss in enumerate(HPSS_ORDER):
            for op_idx, op in enumerate(ops):
                df_cell = df_ext[
                    (df_ext["operation"] == op)
                    & (df_ext["hpss_label"] == hpss)
                    & (df_ext["create_subdir"] == create_sub)
                    & (df_ext["update_subdir"] == update_sub)
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

RATIO_REGRESSION = 1.10
RATIO_IMPROVEMENT = 0.90
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
    dirs: list,
):
    """Paired bars (current vs baseline) per (directory, hpss) cell. Outliers removed."""
    dir_col = OP_DIR_COL[operation]
    df_cur = _filter_df_outliers(
        df_cur[df_cur["operation"] == operation].copy(), [dir_col, "hpss_label"]
    )
    df_bas = _filter_df_outliers(
        df_bas[df_bas["operation"] == operation].copy(), [dir_col, "hpss_label"]
    )

    n_dirs = len(dirs)
    n_hpss = len(HPSS_ORDER)

    pair_width = BAR_WIDTH
    gap = BAR_WIDTH * 0.3
    group_span = n_hpss * (2 * pair_width + gap) + 0.2
    x_base = np.arange(n_dirs) * group_span

    for h_idx, hpss in enumerate(HPSS_ORDER):
        color = HPSS_COLORS[hpss]
        pair_offset = h_idx * (2 * pair_width + gap)

        for d_idx, d in enumerate(dirs):
            x_left = x_base[d_idx] + pair_offset
            x_right = x_base[d_idx] + pair_offset + pair_width

            def mean_for(df, _h=hpss, _d=d):
                v = (
                    df[(df["hpss_label"] == _h) & (df[dir_col] == _d)][
                        "elapsed_seconds"
                    ]
                    .dropna()
                    .values
                )
                return v.mean() if len(v) > 0 else 0.0

            cur_mean = mean_for(df_cur)
            bas_mean = mean_for(df_bas)

            ax.bar(
                x_left,
                bas_mean,
                width=pair_width,
                color=color,
                alpha=0.40,
                hatch="////",
                zorder=2,
                edgecolor=color,
            )
            ax.bar(
                x_right,
                cur_mean,
                width=pair_width,
                color=color,
                alpha=0.85,
                zorder=2,
                label=HPSS_LABELS[hpss] if d_idx == 0 else "",
            )

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
    group_centre_offset = (n_hpss * (2 * pair_width + gap) - gap) / 2
    x_ticks = x_base + group_centre_offset
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([d + "/" for d in dirs], fontsize=9)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.set_xlabel("Directory processed", fontsize=8, labelpad=14)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    _add_dir_annotation(ax, dirs, list(x_ticks))


def _plot_comparison_extract_single_op(
    ax,
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
    operation: str,
):
    """Fig. 2 extract subplot: current vs baseline, outliers removed."""
    df_cur = _filter_df_outliers(
        df_cur[df_cur["operation"] == operation].copy(),
        ["create_subdir", "update_subdir", "hpss_label"],
    )
    df_bas = _filter_df_outliers(
        df_bas[df_bas["operation"] == operation].copy(),
        ["create_subdir", "update_subdir", "hpss_label"],
    )

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

            def mean_for(df, _h=hpss, _cs=create_sub, _us=update_sub):
                v = (
                    df[
                        (df["hpss_label"] == _h)
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
                bas_mean,
                width=pair_width,
                color=color,
                alpha=0.40,
                hatch="////",
                zorder=2,
                edgecolor=color,
            )
            ax.bar(
                x_right,
                cur_mean,
                width=pair_width,
                color=color,
                alpha=0.85,
                zorder=2,
                label=HPSS_LABELS[hpss] if c_idx == 0 else "",
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


def plot_comparison_extract(ax, df_cur: pd.DataFrame, df_bas: pd.DataFrame):
    """Seq vs par extract, current vs baseline. Outliers removed per group."""
    df_cur = _filter_df_outliers(
        df_cur[df_cur["operation"].isin(["extract_seq", "extract_par"])].copy(),
        ["operation", "create_subdir", "update_subdir", "hpss_label"],
    )
    df_bas = _filter_df_outliers(
        df_bas[df_bas["operation"].isin(["extract_seq", "extract_par"])].copy(),
        ["operation", "create_subdir", "update_subdir", "hpss_label"],
    )

    configs = _extract_configs(df_cur)
    n_configs = len(configs)
    ops = ["extract_seq", "extract_par"]
    op_hatches = {"extract_seq": "", "extract_par": "xxxx"}

    pair_width = BAR_WIDTH
    inner_gap = BAR_WIDTH * 0.15
    op_gap = BAR_WIDTH * 0.55
    hpss_gap = BAR_WIDTH * 0.30

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
                x_bas = op_origin
                x_cur = op_origin + pair_width + inner_gap

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

                bas_hatch = hatch + "////"
                ax.bar(
                    x_bas,
                    bas_mean,
                    width=pair_width,
                    color=color,
                    hatch=bas_hatch,
                    alpha=0.35,
                    zorder=2,
                    edgecolor=color,
                )
                ax.bar(
                    x_cur,
                    cur_mean,
                    width=pair_width,
                    color=color,
                    hatch=hatch,
                    alpha=0.85,
                    zorder=2,
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
                        (x_cur + x_bas) / 2,
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

    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h]) for h in HPSS_ORDER
    ]
    seq_patch = mpatches.Patch(
        facecolor="grey", hatch="", alpha=0.85, label="Sequential, current"
    )
    seq_bas_patch = mpatches.Patch(
        facecolor="grey", hatch="////", alpha=0.35, label="Sequential, baseline"
    )
    par_patch = mpatches.Patch(
        facecolor="grey", hatch="xxxx", alpha=0.85, label="Parallel, current"
    )
    par_bas_patch = mpatches.Patch(
        facecolor="grey", hatch="xxxx////", alpha=0.35, label="Parallel, baseline"
    )
    ax.legend(
        handles=hpss_patches + [seq_patch, seq_bas_patch, par_patch, par_bas_patch],
        fontsize=6.5,
        loc="upper right",
        ncol=3,
    )


def build_comparison_figure(
    df_cur: pd.DataFrame,
    df_bas: pd.DataFrame,
    all_dirs: list,
    cur_label: str,
    bas_label: str,
) -> plt.Figure:
    """Build and return the full baseline-comparison figure (Figure 2)."""
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

    cur_patch = mpatches.Patch(facecolor="grey", alpha=0.85, label="Current branch")
    bas_patch = mpatches.Patch(
        facecolor="grey", alpha=0.40, hatch="////", label="Baseline (main)"
    )
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h]) for h in HPSS_ORDER
    ]
    axes["create"].legend(
        handles=[cur_patch, bas_patch] + hpss_patches, fontsize=7, loc="upper right"
    )

    plot_comparison_extract(ax_cmp, df_cur, df_bas)
    return fig


RATIO_REGRESSION_COLOR_LABEL = "red"
RATIO_IMPROVEMENT_COLOR_LABEL = "green"


# ---------------------------------------------------------------------------
# Archive data loading
# ---------------------------------------------------------------------------


def _archive_date_from_path(csv_path: Path) -> Optional[datetime.date]:
    """Parse YYYYMMDD from a CSV filename; return None if not found."""
    m = re.search(r"(\d{8})", csv_path.stem)
    if not m:
        return None
    try:
        s = m.group(1)
        return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:]))
    except ValueError:
        return None


def load_archive_data(archive_dir: str) -> pd.DataFrame:
    """
    Load and concatenate every ``*results*.csv`` in *archive_dir*.
    Adds a ``record_date`` column (pandas Timestamp) from the filename.
    """
    _empty = pd.DataFrame(
        columns=[
            "test_label",
            "create_subdir",
            "update_subdir",
            "hpss_label",
            "operation",
            "elapsed_seconds",
            "record_date",
        ]
    )
    archive_path = Path(archive_dir)
    if not archive_path.is_dir():
        print(
            f"WARNING: performance_archive_dir not found: {archive_path}",
            file=sys.stderr,
        )
        return _empty

    csv_files = sorted(archive_path.glob("*results*.csv"))
    if not csv_files:
        print(
            f"WARNING: no *results*.csv files found in {archive_path}", file=sys.stderr
        )
        return _empty

    frames = []
    for p in csv_files:
        record_date = _archive_date_from_path(p)
        if record_date is None:
            print(
                f"WARNING: cannot parse date from {p.name!r}, skipping.",
                file=sys.stderr,
            )
            continue
        try:
            df_i = load_data(str(p))
        except Exception as exc:
            print(f"WARNING: failed to load {p}: {exc}", file=sys.stderr)
            continue
        df_i["record_date"] = record_date
        frames.append(df_i)

    if not frames:
        return _empty

    df_all = pd.concat(frames, ignore_index=True)
    df_all["record_date"] = pd.to_datetime(df_all["record_date"])
    return df_all


# ---------------------------------------------------------------------------
# Figure 3 – archive: create & update
# ---------------------------------------------------------------------------


def plot_archive_timeseries(ax, df_arch: pd.DataFrame, operation: str) -> None:
    """
    Time-series for create/update over the full archive.
    Outliers removed within each (date, subdir, hpss) group before aggregating.
    Color = hpss mode; line style = subdir.
    """
    dir_col = OP_DIR_COL[operation]
    df_op = df_arch[df_arch["operation"] == operation].copy()
    df_op = _filter_df_outliers(df_op, ["record_date", dir_col, "hpss_label"])

    for hpss in HPSS_ORDER:
        color = HPSS_COLORS[hpss]
        for subdir in SUBDIR_ORDER:
            ls = SUBDIR_LINESTYLES.get(subdir, "solid")
            mask = (df_op["hpss_label"] == hpss) & (df_op[dir_col] == subdir)
            df_line = (
                df_op[mask]
                .groupby("record_date")["elapsed_seconds"]
                .mean()
                .reset_index()
                .sort_values("record_date")
            )
            if df_line.empty:
                continue
            ax.plot(
                df_line["record_date"],
                df_line["elapsed_seconds"],
                color=color,
                linestyle=ls,
                linewidth=1.6,
                marker="o",
                markersize=4,
                label=f"{HPSS_LABELS[hpss]} – {subdir}/",
                zorder=3,
            )

    ax.set_title(
        f"zstash {operation}  –  runtime over time",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.set_xlabel("Record date", fontsize=8)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)

    color_handles = [
        matplotlib.lines.Line2D(
            [], [], color=HPSS_COLORS[h], linewidth=2, label=HPSS_LABELS[h]
        )
        for h in HPSS_ORDER
    ]
    style_handles = [
        matplotlib.lines.Line2D(
            [],
            [],
            color="grey",
            linewidth=2,
            linestyle=SUBDIR_LINESTYLES[s],
            label=f"{s}/",
        )
        for s in SUBDIR_ORDER
    ]
    ax.legend(
        handles=color_handles + style_handles,
        fontsize=6.5,
        loc="upper left",
        ncol=2,
        framealpha=0.8,
    )


def plot_archive_boxplot(ax, df_arch: pd.DataFrame, operation: str) -> None:
    """
    Box-and-whisker for create/update across the full archive.
    Outliers removed per (subdir, hpss) group before drawing.
    """
    dir_col = OP_DIR_COL[operation]
    df_op = df_arch[df_arch["operation"] == operation].copy()
    df_op = _filter_df_outliers(df_op, [dir_col, "hpss_label"])

    n_hpss = len(HPSS_ORDER)
    group_width = n_hpss * BAR_WIDTH + 0.10
    x_base = np.arange(len(SUBDIR_ORDER)) * group_width
    offsets = np.linspace(0, (n_hpss - 1) * BAR_WIDTH, n_hpss)

    tick_positions, tick_labels = [], []

    for s_idx, subdir in enumerate(SUBDIR_ORDER):
        tick_positions.append(x_base[s_idx] + offsets.mean())
        tick_labels.append(f"{subdir}/")

        for h_idx, hpss in enumerate(HPSS_ORDER):
            mask = (df_op["hpss_label"] == hpss) & (df_op[dir_col] == subdir)
            vals = df_op[mask]["elapsed_seconds"].dropna().values
            x_pos = x_base[s_idx] + offsets[h_idx]
            if len(vals) == 0:
                continue
            color = HPSS_COLORS[hpss]
            ax.boxplot(
                vals,
                positions=[x_pos],
                widths=BAR_WIDTH * 0.85,
                patch_artist=True,
                vert=True,
                manage_ticks=False,
                zorder=2,
                boxprops=dict(facecolor=color, alpha=0.55, linewidth=0.8),
                medianprops=dict(color="black", linewidth=1.5),
                whiskerprops=dict(linewidth=0.8),
                capprops=dict(linewidth=0.8),
                flierprops=dict(marker="", linestyle="none"),
            )
            jitter = np.random.uniform(
                -BAR_WIDTH * 0.2, BAR_WIDTH * 0.2, size=len(vals)
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

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, fontsize=9)
    ax.set_title(
        f"zstash {operation}  –  runtime distribution (all records)",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.set_xlabel("Directory processed", fontsize=8)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], alpha=0.75, label=HPSS_LABELS[h])
        for h in HPSS_ORDER
    ]
    ax.legend(handles=hpss_patches, fontsize=7, loc="upper right")


def build_archive_figure(df_arch: pd.DataFrame) -> plt.Figure:
    """
    Figure 3 – full archive overview for create & update.

    Layout (2 rows × 2 cols):
      [0,0] create  time-series  |  [0,1] update  time-series
      [1,0] create  box plot     |  [1,1] update  box plot
    """
    fig = plt.figure(figsize=(15, 12))
    fig.suptitle(
        "zstash Performance – Full Record Archive  (create & update)\n"
        "Time series: color = HPSS mode  ·  line style = directory "
        "(solid = build/, dashed = run/, dotted = init/)\n"
        "Box plots: every recorded runtime per (directory, HPSS) combination\n"
        "Outliers removed via IQR method before plotting",
        fontsize=11,
        fontweight="bold",
        y=0.995,
    )
    gs = fig.add_gridspec(
        2, 2, hspace=0.48, wspace=0.30, top=0.90, bottom=0.08, left=0.07, right=0.97
    )
    for col_idx, op in enumerate(["create", "update"]):
        plot_archive_timeseries(fig.add_subplot(gs[0, col_idx]), df_arch, op)
        plot_archive_boxplot(fig.add_subplot(gs[1, col_idx]), df_arch, op)
    return fig


# ---------------------------------------------------------------------------
# Figure 4 – archive: extract_seq & extract_par
# ---------------------------------------------------------------------------


def plot_extract_archive_timeseries(ax, df_arch: pd.DataFrame, operation: str) -> None:
    """
    Time-series for an extract operation over the full archive.

    Since extract has no single directory column, lines are keyed by the
    combined (create_subdir, update_subdir) archive config pair.
    Color = hpss mode; line style = create_subdir.
    Outliers removed within each (date, create_subdir, update_subdir, hpss) group.
    """
    df_op = df_arch[df_arch["operation"] == operation].copy()
    df_op = _filter_df_outliers(
        df_op, ["record_date", "create_subdir", "update_subdir", "hpss_label"]
    )

    all_pairs = sorted(
        df_op[["create_subdir", "update_subdir"]]
        .drop_duplicates()
        .apply(tuple, axis=1)
        .tolist(),
        key=lambda p: (dir_sort_key(p[0]), dir_sort_key(p[1])),
    )

    markers = ["o", "s", "^", "D", "v", "P", "X", "*", "h"]

    for hpss in HPSS_ORDER:
        color = HPSS_COLORS[hpss]
        for p_idx, (create_sub, update_sub) in enumerate(all_pairs):
            ls = SUBDIR_LINESTYLES.get(create_sub, "solid")
            marker = markers[p_idx % len(markers)]
            mask = (
                (df_op["hpss_label"] == hpss)
                & (df_op["create_subdir"] == create_sub)
                & (df_op["update_subdir"] == update_sub)
            )
            df_line = (
                df_op[mask]
                .groupby("record_date")["elapsed_seconds"]
                .mean()
                .reset_index()
                .sort_values("record_date")
            )
            if df_line.empty:
                continue
            ax.plot(
                df_line["record_date"],
                df_line["elapsed_seconds"],
                color=color,
                linestyle=ls,
                linewidth=1.6,
                marker=marker,
                markersize=4,
                label=f"{HPSS_LABELS[hpss]} – create:{create_sub}/ update:{update_sub}/",
                zorder=3,
            )

    ax.set_title(
        f"{OP_TITLES[operation]}  –  runtime over time",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.set_xlabel("Record date", fontsize=8)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)

    color_handles = [
        matplotlib.lines.Line2D(
            [], [], color=HPSS_COLORS[h], linewidth=2, label=HPSS_LABELS[h]
        )
        for h in HPSS_ORDER
    ]
    style_handles = [
        matplotlib.lines.Line2D(
            [],
            [],
            color="grey",
            linewidth=2,
            linestyle=SUBDIR_LINESTYLES.get(s, "solid"),
            label=f"create: {s}/",
        )
        for s in SUBDIR_ORDER
        if any(p[0] == s for p in all_pairs)
    ]
    ax.legend(
        handles=color_handles + style_handles,
        fontsize=6.5,
        loc="upper left",
        ncol=2,
        framealpha=0.8,
    )


def plot_extract_archive_boxplot(ax, df_arch: pd.DataFrame, operation: str) -> None:
    """
    Box-and-whisker for an extract operation across the full archive.

    X-axis groups = (create_subdir, update_subdir) archive config pairs
    (matching the x-axis used in Figures 1 and 2).
    Within each group the three HPSS modes sit side by side.
    Outliers removed per (archive config pair, hpss_label) group.
    """
    df_op = df_arch[df_arch["operation"] == operation].copy()
    df_op = _filter_df_outliers(df_op, ["create_subdir", "update_subdir", "hpss_label"])

    all_pairs = sorted(
        df_op[["create_subdir", "update_subdir"]]
        .drop_duplicates()
        .apply(tuple, axis=1)
        .tolist(),
        key=lambda p: (dir_sort_key(p[0]), dir_sort_key(p[1])),
    )

    n_hpss = len(HPSS_ORDER)
    group_width = n_hpss * BAR_WIDTH + 0.10
    x_base = np.arange(len(all_pairs)) * group_width
    offsets = np.linspace(0, (n_hpss - 1) * BAR_WIDTH, n_hpss)

    tick_positions, tick_labels = [], []

    for p_idx, (create_sub, update_sub) in enumerate(all_pairs):
        tick_positions.append(x_base[p_idx] + offsets.mean())
        tick_labels.append(_extract_tick_label(create_sub, update_sub))

        for h_idx, hpss in enumerate(HPSS_ORDER):
            mask = (
                (df_op["hpss_label"] == hpss)
                & (df_op["create_subdir"] == create_sub)
                & (df_op["update_subdir"] == update_sub)
            )
            vals = df_op[mask]["elapsed_seconds"].dropna().values
            x_pos = x_base[p_idx] + offsets[h_idx]
            if len(vals) == 0:
                continue
            color = HPSS_COLORS[hpss]
            ax.boxplot(
                vals,
                positions=[x_pos],
                widths=BAR_WIDTH * 0.85,
                patch_artist=True,
                vert=True,
                manage_ticks=False,
                zorder=2,
                boxprops=dict(facecolor=color, alpha=0.55, linewidth=0.8),
                medianprops=dict(color="black", linewidth=1.5),
                whiskerprops=dict(linewidth=0.8),
                capprops=dict(linewidth=0.8),
                flierprops=dict(marker="", linestyle="none"),
            )
            jitter = np.random.uniform(
                -BAR_WIDTH * 0.2, BAR_WIDTH * 0.2, size=len(vals)
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

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, fontsize=7)
    ax.set_title(
        f"{OP_TITLES[operation]}  –  runtime distribution (all records)",
        fontsize=10,
        fontweight="bold",
        pad=6,
    )
    ax.set_xlabel("Archive contents (create → update)", fontsize=8)
    ax.set_ylabel("Wall-clock time (s)", fontsize=8)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    hpss_patches = [
        mpatches.Patch(color=HPSS_COLORS[h], alpha=0.75, label=HPSS_LABELS[h])
        for h in HPSS_ORDER
    ]
    ax.legend(handles=hpss_patches, fontsize=7, loc="upper right")


def build_extract_archive_figure(df_arch: pd.DataFrame) -> plt.Figure:
    """
    Figure 4 – full archive overview for extract_seq & extract_par.

    Layout (2 rows × 2 cols):
      [0,0] extract_seq  time-series  |  [0,1] extract_par  time-series
      [1,0] extract_seq  box plot     |  [1,1] extract_par  box plot

    Only produced when the archive contains extract operation rows.
    """
    fig = plt.figure(figsize=(15, 12))
    fig.suptitle(
        "zstash Performance – Full Record Archive  (extract_seq & extract_par)\n"
        "Time series: color = HPSS mode  ·  line style = create_subdir "
        "(solid = build/, dashed = run/, dotted = init/)\n"
        "Box plots: every recorded runtime per (archive config, HPSS) combination\n"
        "Outliers removed via IQR method before plotting",
        fontsize=11,
        fontweight="bold",
        y=0.995,
    )
    gs = fig.add_gridspec(
        2, 2, hspace=0.55, wspace=0.32, top=0.90, bottom=0.10, left=0.07, right=0.97
    )
    for col_idx, op in enumerate(["extract_seq", "extract_par"]):
        plot_extract_archive_timeseries(fig.add_subplot(gs[0, col_idx]), df_arch, op)
        plot_extract_archive_boxplot(fig.add_subplot(gs[1, col_idx]), df_arch, op)
    return fig


# ---------------------------------------------------------------------------
# Main – helpers
# ---------------------------------------------------------------------------


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Visualise zstash performance results."
    )
    parser.add_argument(
        "--cfg",
        default=str(_SCRIPT_DIR / "perf.cfg"),
        help="Path to the key=value config file (default: perf.cfg next to this script).",
    )
    parser.add_argument(
        "--dpi", type=int, default=150, help="Output DPI (default: 150)"
    )
    return parser.parse_args()


def _load_results(results_csv: str) -> pd.DataFrame:
    """Load and validate the primary results CSV; exit on error."""
    results_path = Path(results_csv)
    if not results_path.is_file():
        print(f"ERROR: results_csv not found: {results_csv!r}", file=sys.stderr)
        sys.exit(1)
    df = load_data(str(results_path))
    if df.empty:
        print(
            f"ERROR: results_csv empty or unparseable: {results_csv!r}", file=sys.stderr
        )
        sys.exit(1)
    return df


def build_overview_figure(df: pd.DataFrame, all_dirs: list) -> plt.Figure:
    """
    Figure 1 – performance overview.

    Layout: 3 rows × 2 cols
      Row 0: create  |  update
      Row 1: extract_seq  |  extract_par
      Row 2: extract seq-vs-par comparison (spans both columns)
    """
    fig = plt.figure(figsize=(15, 16))
    fig.suptitle(
        "zstash Performance Profiling\n"
        "(bars = mean over test configs; dots = individual runs; "
        "outliers removed via IQR)",
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

    legend_handles = None
    for op in OP_ORDER:
        ax = axes[op]
        if op in OP_DIR_COL:
            plot_operation(ax, df[df["operation"] == op], op, all_dirs)
        else:
            _plot_extract_single_op(ax, df, op)
        if legend_handles is None:
            legend_handles = [
                mpatches.Patch(color=HPSS_COLORS[h], label=HPSS_LABELS[h])
                for h in HPSS_ORDER
            ]
            ax.legend(handles=legend_handles, fontsize=7, loc="upper right")

    plot_extract_comparison(ax_cmp, df)
    return fig


def _try_build_comparison_figure(
    df: pd.DataFrame,
    all_dirs: list,
    results_csv: str,
    baseline_results_csv: Optional[str],
) -> Optional[plt.Figure]:
    """Figure 2 – baseline comparison. Returns None when not applicable."""
    if not baseline_results_csv:
        return None
    bas_path = Path(baseline_results_csv)
    if not bas_path.exists():
        print(f"WARNING: baseline_results_csv not found: {bas_path}", file=sys.stderr)
        print("Skipping baseline comparison figure.", file=sys.stderr)
        return None
    df_bas = load_data(str(bas_path))
    bas_label = bas_path.parent.name
    cur_label = Path(results_csv).parent.name
    return build_comparison_figure(df, df_bas, all_dirs, cur_label, bas_label)


def _try_build_archive_figures(
    archive_dir: Optional[str],
) -> tuple:
    """
    Figures 3 & 4 – full record archive.

    Returns a (fig_arch, fig_arch_extract) tuple; either element may be None
    when the corresponding data is unavailable.
    """
    if not archive_dir:
        return None, None
    df_arch = load_archive_data(archive_dir)
    if df_arch.empty:
        print(
            "WARNING: no archive data found; skipping Figures 3 & 4.", file=sys.stderr
        )
        return None, None
    fig_arch = build_archive_figure(df_arch)
    has_extract = df_arch["operation"].isin(["extract_seq", "extract_par"]).any()
    if has_extract:
        fig_arch_extract = build_extract_archive_figure(df_arch)
    else:
        print("INFO: no extract data in archive; skipping Figure 4.", file=sys.stderr)
        fig_arch_extract = None
    return fig_arch, fig_arch_extract


def _save_figure(figure: plt.Figure, out_path_str: str, label: str, dpi: int) -> None:
    """Save *figure* to *out_path_str* and print the destination."""
    out_path = Path(out_path_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(out_path, dpi=dpi, bbox_inches="tight")
    print(f"{label} saved to: {out_path}")
    try:
        os.chmod(out_path, 0o644)
    except OSError:
        pass
    web_path = str(out_path).replace(
        "/global/cfs/cdirs/e3sm/www/",
        "https://portal.nersc.gov/cfs/e3sm/",
    )
    print(f"  Accessible at: {web_path}")


def _save_all_figures(
    fig: plt.Figure,
    fig_cmp: Optional[plt.Figure],
    fig_arch: Optional[plt.Figure],
    fig_arch_extract: Optional[plt.Figure],
    output_path: str,
    dpi: int,
) -> None:
    """Save every non-None figure to a path derived from *output_path*."""
    p = Path(output_path)
    _save_figure(fig, output_path, "Figure 1 (overview)", dpi)
    if fig_cmp is not None:
        _save_figure(
            fig_cmp,
            str(p.with_stem(p.stem + "_vs_baseline")),
            "Figure 2 (baseline comparison)",
            dpi,
        )
    if fig_arch is not None:
        _save_figure(
            fig_arch,
            str(p.with_stem(p.stem + "_archive")),
            "Figure 3 (full archive: create & update)",
            dpi,
        )
    if fig_arch_extract is not None:
        _save_figure(
            fig_arch_extract,
            str(p.with_stem(p.stem + "_archive_extract")),
            "Figure 4 (full archive: extract_seq & extract_par)",
            dpi,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = _parse_args()

    cfg_path = Path(args.cfg)
    if not cfg_path.is_file():
        print(f"ERROR: config file not found: {cfg_path}", file=sys.stderr)
        print(
            f"Copy {_SCRIPT_DIR / 'perf.cfg'} and edit it for your run.",
            file=sys.stderr,
        )
        sys.exit(1)

    cfg = _load_cfg(cfg_path)
    results_csv: str = _cfg_require(cfg, "results_csv", cfg_path)
    baseline_results_csv: Optional[str] = _cfg_optional(cfg, "baseline_results_csv")
    output_path: Optional[str] = _cfg_optional(cfg, "output_path")
    archive_dir: Optional[str] = _cfg_optional(cfg, "performance_archive_dir")

    df = _load_results(results_csv)
    all_dirs = sorted(
        set(df["create_subdir"].dropna()) | set(df["update_subdir"].dropna()),
        key=dir_sort_key,
    )

    fig = build_overview_figure(df, all_dirs)
    fig_cmp = _try_build_comparison_figure(
        df, all_dirs, results_csv, baseline_results_csv
    )
    fig_arch, fig_arch_extract = _try_build_archive_figures(archive_dir)

    if output_path:
        _save_all_figures(
            fig, fig_cmp, fig_arch, fig_arch_extract, output_path, args.dpi
        )
    else:
        plt.show()


if __name__ == "__main__":
    np.random.seed(42)  # reproducible jitter
    main()
