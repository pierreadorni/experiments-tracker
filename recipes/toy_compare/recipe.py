"""Synthetic infrastructure demo, pure Python. No third-party compute environment needed."""

import argparse
import hashlib
import json
import math
import random
from pathlib import Path

PARAMETERS = {"seed": 1729, "n_train": 64, "n_test": 128, "noise_sd": 0.2}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def data():
    rng = random.Random(PARAMETERS["seed"])

    def points(n):
        result = []
        for _ in range(n):
            x = rng.uniform(-2, 2)
            result.append([x, 0.5 + 0.7 * x + 1.3 * x * x + rng.gauss(0, PARAMETERS["noise_sd"])])
        return result

    return {
        "parameters": PARAMETERS,
        "train": points(PARAMETERS["n_train"]),
        "test": points(PARAMETERS["n_test"]),
    }


def solve(matrix, vector):
    a = [row[:] + [value] for row, value in zip(matrix, vector, strict=True)]
    n = len(a)
    for i in range(n):
        pivot = max(range(i, n), key=lambda j: abs(a[j][i]))
        a[i], a[pivot] = a[pivot], a[i]
        scale = a[i][i]
        if abs(scale) < 1e-12:
            raise ValueError("singular fit")
        a[i] = [v / scale for v in a[i]]
        for j in range(n):
            if j != i:
                ratio = a[j][i]
                a[j] = [v - ratio * w for v, w in zip(a[j], a[i], strict=True)]
    return [row[-1] for row in a]


def predict(coefficients, x):
    return sum(c * x**i for i, c in enumerate(coefficients))


def save(path, value):
    tmp = path.with_suffix(path.suffix + ".pending")
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def train(root, variant):
    dataset = data()
    n = 2 if variant == "linear" else 3
    train_data = dataset["train"]
    matrix = [[sum(x ** (i + j) for x, _ in train_data) for j in range(n)] for i in range(n)]
    vector = [sum(y * x**i for x, y in train_data) for i in range(n)]
    save(
        root / f"{variant}.json",
        {
            "schema_version": 1,
            "variant": variant,
            "coefficients": solve(matrix, vector),
            "data_hash": digest(dataset),
            "test_hash": digest(dataset["test"]),
            "parameters_hash": digest(PARAMETERS),
            "parameters": PARAMETERS,
        },
    )


def evaluate(root):
    dataset = data()
    metrics = {}
    models = {}
    for variant in ("linear", "quadratic"):
        model = json.loads((root / f"{variant}.json").read_text())
        assert model["variant"] == variant and model["data_hash"] == digest(dataset)
        assert model["test_hash"] == digest(dataset["test"])
        models[variant] = model
        errors = [predict(model["coefficients"], x) - y for x, y in dataset["test"]]
        mse = sum(v * v for v in errors) / len(errors)
        metrics[variant] = {
            "mse": mse,
            "rmse": math.sqrt(mse),
            "n_test": len(errors),
            "n_parameters": len(model["coefficients"]),
        }
    save(
        root / "results.json",
        {
            "schema_version": 1,
            "synthetic_demo": True,
            "parameters": PARAMETERS,
            "parameters_hash": digest(PARAMETERS),
            "data_hash": digest(dataset),
            "test_hash": digest(dataset["test"]),
            "metrics": metrics,
            "metric_definition": "mean squared prediction error on the shared held-out noisy targets",
        },
    )
    save(
        root / "predictions.json",
        {
            "schema_version": 1,
            "test_hash": digest(dataset["test"]),
            "points": [
                {"x": x, "target": y, **{v: predict(m["coefficients"], x) for v, m in models.items()}}
                for x, y in sorted(dataset["test"])
            ],
        },
    )

    # SVG is a trusted, fixed code-generated scientific artifact with no scripts or remote references.
    def px(x):
        return 65 + (x + 2) / 4 * 670

    def py(y):
        return 365 - (y + 1) / 9 * 300

    svg = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 430" role="img" aria-label="Synthetic regression predictions">',
        '<rect width="800" height="430" fill="#fff"/>',
        '<text x="65" y="30" font-family="sans-serif" font-size="18">Synthetic demo · shared held-out observations</text>',
        '<path d="M65 60 V365 H735" fill="none" stroke="#555"/>',
        '<text x="375" y="420" font-family="sans-serif">Input x (arbitrary units)</text>',
        '<text x="12" y="220" font-family="sans-serif" transform="rotate(-90 12 220)">Target y (arbitrary units)</text>',
    ]
    for x, y in dataset["test"]:
        svg.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="2.5" fill="#64748b" opacity="0.6"/>')
    for variant, color in (("linear", "#d97706"), ("quadratic", "#0d9488")):
        coordinates = " ".join(
            f"{px(x):.2f},{py(predict(models[variant]['coefficients'], x)):.2f}"
            for x in [-2 + i * 0.04 for i in range(101)]
        )
        svg.append(f'<polyline points="{coordinates}" fill="none" stroke="{color}" stroke-width="3"/>')
    svg += [
        '<text x="530" y="52" font-family="sans-serif" fill="#d97706">Linear</text>',
        '<text x="625" y="52" font-family="sans-serif" fill="#0d9488">Quadratic</text>',
    ]
    for x in (-2, -1, 0, 1, 2):
        svg.append(f'<text x="{px(x) - 5}" y="385" font-family="sans-serif">{x}</text>')
    for y in (0, 2, 4, 6, 8):
        svg.append(f'<text x="40" y="{py(y) + 5}" font-family="sans-serif">{y}</text>')
    svg.append("</svg>")
    (root / "predictions.svg").write_text("\n".join(svg))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["train_linear", "train_quadratic", "evaluate_and_plot"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.stage.startswith("train_"):
        train(args.output, args.stage.removeprefix("train_"))
    else:
        evaluate(args.output)
