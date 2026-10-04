import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["font.family"] = "serif"

# items_per_second (M/s) from Google Benchmark, capacity 1048576, 73-byte messages
readers = [2, 3, 4, 5, 6, 7]
series = {
    "SPMC": [15.1738, 11.7508, 9.89596, 8.53342, 7.98988, 7.16219],
    "Aeron Broadcast": [13.8066, 9.52394, 8.23437, 6.88203, 6.08654, 5.52381],
}
colors = ["#ffb3ba", "#bae1ff"]
hatches = ["//", "||"]

x = np.arange(len(readers))
width = 0.38

fig, ax = plt.subplots(figsize=(9, 5.5))

for i, (name, values) in enumerate(series.items()):
    offset = (i - (len(series) - 1) / 2) * width
    bars = ax.bar(
        x + offset,
        values,
        width,
        label=name,
        color=colors[i],
        edgecolor="black",
        hatch=hatches[i],
        )
    ax.bar_label(bars, fmt="%.1f", padding=2, fontsize=9)

ax.set_title("Broadcast Throughput vs. Reader Count\n(Higher is Better)", fontsize=14)
ax.set_xlabel("Number of readers")
ax.set_ylabel("Throughput (M items/s)")
ax.set_xticks(x)
ax.set_xticklabels(readers)
ax.set_ylim(0, max(max(v) for v in series.values()) * 1.12)
ax.grid(axis="y", linestyle="--", alpha=0.5)
ax.set_axisbelow(True)
ax.legend()

plt.tight_layout()
plt.savefig("throughput_vs_readers.png", dpi=150)