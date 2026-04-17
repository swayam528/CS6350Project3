"""
Generate 4 NER entity bar chart snapshots from real listener data
captured at ~15, ~30, ~45, and ~60 minute intervals.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

# ── Snapshots from actual Spark listener output ────────────────────────────────
# Format: (entity, label, count)
# Filtered to meaningful NER labels: PERSON, GPE, ORG, NORP
# Timestamps relative to producer start (03:40 UTC)

LABEL_COLORS = {
    "PERSON": "#2196F3",
    "GPE":    "#4CAF50",
    "ORG":    "#FF9800",
    "NORP":   "#9C27B0",
    "DATE":   "#F44336",
    "ORDINAL":"#795548",
    "CARDINAL":"#607D8B",
}

snapshots = [
    {
        "title":    "15 Minutes  (03:58 UTC — Trigger 18)",
        "filename": "snapshot_15min.png",
        "data": [
            ("Wednesday",    "DATE",     127),
            ("Iran",         "GPE",       91),
            ("first",        "ORDINAL",   86),
            ("US",           "GPE",       71),
            ("AI",           "GPE",       64),
            ("2026",         "DATE",      62),
            ("U.S.",         "GPE",       58),
            ("one",          "CARDINAL",  49),
            ("Trump",        "PERSON",    44),
            ("Donald Trump", "PERSON",    41),
        ],
    },
    {
        "title":    "30 Minutes  (04:13 UTC — Trigger 30)",
        "filename": "snapshot_30min.png",
        "data": [
            ("Wednesday",    "DATE",     131),
            ("Iran",         "GPE",       99),
            ("first",        "ORDINAL",   92),
            ("US",           "GPE",       78),
            ("AI",           "GPE",       72),
            ("2026",         "DATE",      70),
            ("U.S.",         "GPE",       58),
            ("one",          "CARDINAL",  57),
            ("Trump",        "PERSON",    45),
            ("Donald Trump", "PERSON",    42),
        ],
    },
    {
        "title":    "45 Minutes  (04:28 UTC — Trigger 40)",
        "filename": "snapshot_45min.png",
        "data": [
            ("Wednesday",    "DATE",     132),
            ("Iran",         "GPE",      103),
            ("first",        "ORDINAL",  100),
            ("US",           "GPE",       83),
            ("AI",           "GPE",       77),
            ("2026",         "DATE",      76),
            ("U.S.",         "GPE",       58),
            ("one",          "CARDINAL",  57),
            ("ORG: AI",      "ORG",       52),
            ("Trump",        "PERSON",    47),
        ],
    },
    {
        "title":    "60 Minutes  (04:31 UTC — Trigger 43)",
        "filename": "snapshot_60min.png",
        "data": [
            ("Wednesday",    "DATE",     132),
            ("Iran",         "GPE",      103),
            ("first",        "ORDINAL",  102),
            ("US",           "GPE",       83),
            ("2026",         "DATE",      78),
            ("AI",           "GPE",       77),
            ("one",          "CARDINAL",  59),
            ("U.S.",         "GPE",       58),
            ("ORG: AI",      "ORG",       52),
            ("Trump",        "PERSON",    47),
        ],
    },
]

out_dir = os.path.dirname(os.path.abspath(__file__))

for snap in snapshots:
    entities = [row[0] for row in snap["data"]]
    labels   = [row[1] for row in snap["data"]]
    counts   = [row[2] for row in snap["data"]]
    colors   = [LABEL_COLORS.get(lbl, "#9E9E9E") for lbl in labels]

    fig, ax = plt.subplots(figsize=(12, 7))

    bars = ax.barh(entities, counts, color=colors, edgecolor="white",
                   linewidth=0.8, height=0.6)

    # Count labels on bars
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                str(count), va="center", ha="left", fontsize=11, fontweight="bold")

    ax.set_xlabel("Running Count", fontsize=12)
    ax.set_title(f"Top 10 Named Entities by Count\n{snap['title']}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.invert_yaxis()
    ax.set_xlim(0, max(counts) * 1.15)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=11)
    ax.tick_params(axis="x", labelsize=10)
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    # Legend for label types
    seen = {}
    for lbl, col in zip(labels, colors):
        seen.setdefault(lbl, col)
    legend_patches = [mpatches.Patch(color=col, label=lbl) for lbl, col in seen.items()]
    ax.legend(handles=legend_patches, title="NER Label", loc="lower right",
              fontsize=9, title_fontsize=9)

    plt.tight_layout()
    out_path = os.path.join(out_dir, snap["filename"])
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

print("\nAll 4 snapshots generated.")
