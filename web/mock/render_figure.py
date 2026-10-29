"""Render illustrative fixture data for design study 01, not research evidence."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402 — choose the headless backend before pyplot

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "svg.hashsalt": "design-study-01"})
fig, ax = plt.subplots(figsize=(7.4, 3.2))
fig.patch.set_alpha(0)
ax.patch.set_alpha(0)
ax.set_axisbelow(True)
ax.yaxis.grid(True, color="#e4e6dd", linewidth=0.7)
ax.errorbar(
    [1, 2, 3],
    [0.0579, 0.0447, 0.0497],
    yerr=[0.0059, 0.0003, 0.0039],
    color="#a4ad92",
    linewidth=1.3,
    capsize=4,
    elinewidth=1.3,
    capthick=1.3,
)
ax.scatter([1, 3], [0.0579, 0.0497], s=45, color="#899c6c", zorder=4)
ax.scatter([2], [0.0447], s=65, color="#536b39", zorder=4)
ax.set_xlim(0.65, 3.35)
ax.set_ylim(0.036, 0.069)
ax.set_xticks([1, 2, 3], ["1 layer", "2 layers", "3 layers"])
ax.set_yticks([0.04, 0.05, 0.06], ["0.04", "0.05", "0.06"])
ax.tick_params(axis="both", length=0, colors="#888d7b", pad=12, labelsize=11)
for spine in ax.spines.values():
    spine.set_visible(False)
fig.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.17)
fig.savefig(
    Path(__file__).with_name("depth-comparison.svg"),
    transparent=True,
    metadata={"Date": None, "Description": "Illustrative synthetic design fixture. Not scientific evidence."},
)
fig.set_size_inches(3.65, 2.5)
ax.tick_params(axis="both", labelsize=13, pad=11)
fig.subplots_adjust(left=0.18, right=0.99, top=0.94, bottom=0.19)
fig.savefig(
    Path(__file__).with_name("depth-comparison-mobile.svg"),
    transparent=True,
    metadata={"Date": None, "Description": "Illustrative synthetic design fixture. Not scientific evidence."},
)
plt.close(fig)

# Keep generated SVG markup clean in review without changing its geometry.
for name in ("depth-comparison.svg", "depth-comparison-mobile.svg"):
    output = Path(__file__).with_name(name)
    output.write_text("\n".join(line.rstrip() for line in output.read_text().splitlines()) + "\n")
