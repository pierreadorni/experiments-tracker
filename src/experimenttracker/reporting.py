"""Evidence-bound report composition and deterministic, presentation-only plots."""

import hashlib
import html
import json
import math
import os
import re
import shutil
import tempfile
from collections import defaultdict
from importlib.metadata import version as package_version
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from . import artifacts
from .models import ReportDocument

RENDERER_VERSION = "1"


def skill_text():
    return (Path(__file__).parent / "skills/research-report/SKILL.md").read_text()


def artifact_id(exp, name):
    return str(uuid5(NAMESPACE_URL, f"{exp['id']}:{exp['evidence_revision']}:{name}"))


class Evidence:
    def __init__(self, exp, root, records, *, metric_source_ids=None):
        self.exp, self.root = exp, root
        iteration = exp["contract"].get("execution_contract") == 3
        if iteration and root.parent.name == "records":
            self.root = root.parent.parent
        self.records = {r["id"]: r for r in records}
        self.used = {}
        result = next(
            (
                r
                for r in reversed(records)
                if r["name"] == "results.json"
                and r.get("evidence_revision", exp.get("evidence_revision")) == exp.get("evidence_revision")
            ),
            None,
        )
        if iteration and metric_source_ids is not None:
            metrics = [m for identity in metric_source_ids for m in self.json(identity)["metrics"]]
        elif iteration and not result:
            names = {s["result_file"] for s in exp["snapshot"]["plan"]["stages"] if s.get("result_file")}
            selected = [
                r
                for r in records
                if r["name"] in names
                and r.get("evidence_revision", exp.get("evidence_revision")) == exp.get("evidence_revision")
            ]
            metrics = [m for r in selected for m in self.json(r["id"])["metrics"]]
        else:
            if not result:
                result = next(r for r in records if r["name"] == "results.json")
            value = self.json(result["id"])
            metrics = value["metrics"]
        if isinstance(metrics, dict):
            metrics = [
                {"name": name, "variant": variant, "value": number, "unit": ""}
                for variant, row in metrics.items()
                for name, number in row.items()
            ]
        self.metric_source_ids = list(self.used)
        self.metrics = {}
        for m in metrics:
            key = m["name"], m["variant"]
            if key in self.metrics:
                raise ValueError("ambiguous metric reference")
            self.metrics[key] = m

    def read(self, identity):
        if identity not in self.records:
            raise ValueError("report cites unknown evidence")
        row = self.records[identity]
        path = artifacts.allowed_path(Path(row["path"]), self.root)
        data = path.read_bytes()
        if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
            raise ValueError("report source artifact changed")
        seen = identity in self.used
        self.used[identity] = {k: row[k] for k in ("id", "name", "sha256", "size")}
        derived = re.match(r"^(discussion-[a-f0-9-]{36}-)", row["name"])
        if derived and not seen:
            provenance_name = derived[1] + "provenance.json"
            provenance = next((r for r in self.records.values() if r["name"] == provenance_name), None)
            if not provenance:
                raise ValueError("derived artifact is missing its provenance")
            if row["name"] != provenance_name:
                manifest = json.loads(self.read(provenance["id"]))
                for source in manifest["source_artifacts"] + manifest["outputs"]:
                    registered = self.records.get(source["id"])
                    if not registered or any(registered[k] != source[k] for k in ("name", "sha256", "size")):
                        raise ValueError("derived artifact provenance differs from registered evidence")
                    self.read(source["id"])
        return data

    def json(self, identity):
        value = json.loads(self.read(identity))
        artifacts.ensure_finite_json(value)
        return value

    def metric(self, reference):
        row = self.metrics.get((reference.name, reference.variant))
        if not row or not artifacts.finite_number(row["value"]):
            raise ValueError("report refers to missing or non-finite metric")
        return row

    def value(self, reference, uncertainty=None):
        metric = self.metric(reference)
        error = self.metric(uncertainty) if uncertainty else None
        if error and (error["unit"] != metric["unit"] or error["value"] < 0):
            raise ValueError("uncertainty must be nonnegative and use the metric's unit")
        return metric["value"], error["value"] if error else None


def validate_document(evidence, document):
    doc = ReportDocument.model_validate(document)
    for block in doc.blocks:
        for identity in block.evidence_ids:
            evidence.read(identity)
        if block.kind == "figure":
            evidence.read(block.artifact_id)
            if not evidence.records[block.artifact_id]["media_type"].startswith("image/"):
                raise ValueError("figure must reference a registered image")
        elif block.kind == "table":
            for row in block.rows:
                for cell in row:
                    if cell.metric:
                        evidence.value(cell.metric, cell.uncertainty)
        elif block.kind == "metric_plot":
            units = set()
            for series in block.series:
                for point in series.points + series.individual_points:
                    evidence.value(point.metric, point.uncertainty)
                    units.add(evidence.metric(point.metric)["unit"])
            if len(units) != 1:
                raise ValueError("plot measurements must have matching units")
        elif block.kind == "curve_plot":
            for series in block.curves:
                curve_data(evidence, block, series)
    return doc


def curve_data(evidence, block, series):
    records = evidence.json(block.artifact_id).get(block.records_key)
    if not isinstance(records, list) or not 1 <= len(records) <= 50000:
        raise ValueError("curve source must be a bounded array of records")
    selected = [
        r for r in records if not series.match_field or r.get(series.match_field) == series.match_value
    ]
    if not selected:
        raise ValueError("curve series has no matching records")
    groups = defaultdict(list)
    targets = {}
    raw = []
    for row in selected:
        x, y = row.get(block.x_field), row.get(series.y_field)
        target = row.get(block.target_field) if block.target_field else None
        if (
            not artifacts.finite_number(x)
            or not artifacts.finite_number(y)
            or (block.target_field and not artifacts.finite_number(target))
        ):
            raise ValueError("curve records must contain finite numeric fields")
        raw.append((x, y))
        groups[x].append(y)
        if target is not None:
            if x in targets and targets[x] != target:
                raise ValueError("repeated x has inconsistent observed targets")
            targets[x] = target
    points = [(x, math.fsum(ys) / len(ys)) for x, ys in groups.items()] if block.average_repetitions else raw
    if block.order == "x":
        points.sort(key=lambda p: p[0])
    return points, sorted(targets.items())


def _plotter(root):
    # Cache only under owned experiment output; do not write global configuration.
    os.environ.setdefault("MPLCONFIGDIR", str(root / ".report-cache"))
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    return plt


def _style_axis(ax, block):
    ax.set_xlabel(block.x_label, labelpad=9)
    ax.set_ylabel(block.y_label, labelpad=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.16, linewidth=0.7)
    ax.set_axisbelow(True)


def _save(plt, fig, target):
    fig.savefig(target, format="svg", metadata={"Date": None}, bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)


def plot_metric(evidence, block, target):
    plt = _plotter(evidence.root)
    with plt.rc_context(
        {
            "font.size": 12,
            "axes.labelsize": 13,
            "svg.fonttype": "none",
            "svg.hashsalt": "experimenttracker-report-v1",
        }
    ):
        fig, ax = plt.subplots(figsize=(6.2, 3.8), layout="constrained")
        units = set()
        ticks = set()
        for i, series in enumerate(block.series):
            points = sorted(series.points, key=lambda p: p.x)
            values = [evidence.value(p.metric, p.uncertainty) for p in points]
            units.update(evidence.metric(p.metric)["unit"] for p in points + series.individual_points)
            ticks.update(p.x for p in points)
            color = ["#236b88", "#bd6546", "#52815c", "#855fa0", "#b58c25", "#5e6573"][i]
            ax.errorbar(
                [p.x for p in points],
                [v[0] for v in values],
                yerr=[v[1] or 0 for v in values],
                fmt="o-",
                color=color,
                capsize=5,
                linewidth=1.6,
                markersize=6,
                label=series.label,
            )
            if series.individual_points:
                # Deterministic horizontal offsets separate repeated observations; x ticks retain true levels.
                counts = defaultdict(int)
                totals = defaultdict(int)
                for p in series.individual_points:
                    totals[p.x] += 1
                xs = []
                span = (max(ticks) - min(ticks)) if len(ticks) > 1 else 1
                for p in series.individual_points:
                    n = totals[p.x]
                    xs.append(p.x + (counts[p.x] - (n - 1) / 2) * span * 0.016)
                    counts[p.x] += 1
                ax.scatter(
                    xs,
                    [evidence.value(p.metric)[0] for p in series.individual_points],
                    s=22,
                    facecolor="white",
                    edgecolor=color,
                    alpha=0.8,
                    zorder=3,
                    label="Individual repetitions"
                    if len(block.series) == 1
                    else series.label + " repetitions",
                )
        if len(units) != 1:
            raise ValueError("plot measurements must have matching units")
        if len(ticks) <= 12:
            ax.set_xticks(sorted(ticks))
        _style_axis(ax, block)
        if len(block.series) > 1 or any(s.individual_points for s in block.series):
            ax.legend(frameon=False, fontsize=10, loc="best")
        _save(plt, fig, target)


def plot_curve(evidence, block, series, target):
    points, observed = curve_data(evidence, block, series)
    plt = _plotter(evidence.root)
    with plt.rc_context(
        {
            "font.size": 12,
            "axes.labelsize": 13,
            "svg.fonttype": "none",
            "svg.hashsalt": "experimenttracker-report-v1",
        }
    ):
        fig, ax = plt.subplots(figsize=(6.2, 3.5), layout="constrained")
        if observed:
            ax.scatter(
                *zip(*observed, strict=True), s=14, color="#89969b", alpha=0.6, label="Held-out observations"
            )
        ax.plot(*zip(*points, strict=True), color="#236b88", linewidth=2, label=series.label)
        _style_axis(ax, block)
        ax.legend(frameon=False, fontsize=10, loc="best")
        _save(plt, fig, target)


CSS = """
:root{color-scheme:light}*{box-sizing:border-box}body{margin:0;background:#fff;color:#26343a;font:17px/1.7 Georgia,'Times New Roman',serif}
article{max-width:780px;margin:auto;padding:48px 28px 64px}h1,h2,.eyebrow{font-family:system-ui,sans-serif}h1{font-size:32px;line-height:1.22;letter-spacing:-.6px;margin:12px 0 20px}h2{font-size:22px;line-height:1.3;margin:36px 0 12px}
p{margin:16px 0}.standfirst{font-size:19px;color:#42565e}.eyebrow{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#66777e}
figure{margin:30px 0}img{display:block;width:100%;height:auto;max-width:720px;margin:auto}figcaption,caption{font:14px/1.55 system-ui,sans-serif;color:#52636b;text-align:left}figcaption{margin-top:10px}caption{caption-side:bottom;padding-top:12px}
.table-wrap{overflow-x:auto;margin:28px 0}table{border-collapse:collapse;width:100%;font:15px/1.5 system-ui,sans-serif;font-variant-numeric:tabular-nums;border-top:2px solid #283a43;border-bottom:2px solid #283a43}thead{border-bottom:1px solid #283a43}th,td{padding:11px 12px;text-align:left;white-space:nowrap}th{font-weight:600}td.numeric{text-align:right}tbody tr+tr{border-top:0}a{color:#276d80;text-decoration:none}a:hover{text-decoration:underline}.evidence{font:12px/1.5 system-ui,sans-serif;display:block;margin-top:5px;color:#6b7880}.report-footer{font:12px/1.5 system-ui,sans-serif;color:#718087;border-top:1px solid #e0e5e7;margin-top:36px;padding-top:16px}
@media(max-width:480px){article{padding:28px 18px 40px}h1{font-size:26px}h2{font-size:20px}.standfirst{font-size:18px}body{font-size:16px}th,td{padding:9px 8px}}
@media print{article{max-width:none;padding:0}figure,.table-wrap{break-inside:avoid}a{color:inherit}.evidence{display:none}}
"""


def build_report(exp, root, records, document, version, *, preview=False):
    """Reproducible package. Outputs use fresh versioned names; existing bytes never change."""
    doc = ReportDocument.model_validate(document)
    evidence = Evidence(exp, root, records)
    validate_document(evidence, doc)
    prefix = f"report-v{version}"
    report_name = "report.html" if version == 1 else prefix + ".html"
    generated = []
    esc = html.escape
    figure_number = table_number = 0

    def citations(ids):
        links = []
        for identity in ids:
            evidence.read(identity)
            links.append(
                f'<a href="/api/artifacts/{esc(identity)}">{esc(evidence.records[identity]["name"])}</a>'
            )
        return '<span class="evidence">Source: ' + ", ".join(links) + "</span>" if links else ""

    with tempfile.TemporaryDirectory(prefix=f".{prefix}-", dir=root) as tmp:
        pending = Path(tmp)
        content = []

        def image(name, caption, alt, ids):
            nonlocal figure_number
            figure_number += 1
            src = name if preview else "/api/artifacts/" + artifact_id(exp, name)
            return f'<figure><img src="{esc(src)}" alt="{esc(alt)}"><figcaption><strong>Figure {figure_number}.</strong> {esc(caption)}{citations(ids)}</figcaption></figure>'

        for block in doc.blocks:
            if block.kind == "heading":
                content.append(f"<h2>{esc(block.text)}</h2>")
            elif block.kind == "paragraph":
                content.append(f"<p>{esc(block.text)}{citations(block.evidence_ids)}</p>")
            elif block.kind == "table":
                table_number += 1
                rows = []
                for row in block.rows:
                    cells = []
                    for cell in row:
                        if cell.metric:
                            value, error = evidence.value(cell.metric, cell.uncertainty)
                            text = f"{value:.{cell.decimals}f}" + (
                                f" ± {error:.{cell.decimals}f}" if error is not None else ""
                            )
                            cells.append(f'<td class="numeric">{text}</td>')
                        else:
                            cells.append(f"<td>{esc(cell.text)}</td>")
                    rows.append("<tr>" + "".join(cells) + "</tr>")
                content.append(
                    '<div class="table-wrap"><table><caption>'
                    + f"<strong>Table {table_number}.</strong> {esc(block.caption)}{citations(block.evidence_ids)}</caption><thead><tr>"
                    + "".join(f'<th scope="col">{esc(c)}</th>' for c in block.columns)
                    + "</tr></thead><tbody>"
                    + "".join(rows)
                    + "</tbody></table></div>"
                )
            elif block.kind == "figure":
                evidence.read(block.artifact_id)
                if not evidence.records[block.artifact_id]["media_type"].startswith("image/"):
                    raise ValueError("figure must reference a registered image")
                figure_number += 1
                content.append(
                    f'<figure><img src="/api/artifacts/{esc(block.artifact_id)}" alt="{esc(block.alt)}"><figcaption><strong>Figure {figure_number}.</strong> {esc(block.caption)}{citations(block.evidence_ids)}</figcaption></figure>'
                )
            else:
                curves = block.curves if block.kind == "curve_plot" else [None]
                for curve in curves:
                    name = f"{prefix}-figure-{figure_number + 1}.svg"
                    if curve:
                        plot_curve(evidence, block, curve, pending / name)
                    else:
                        plot_metric(evidence, block, pending / name)
                    generated.append(name)
                    caption = (curve.label + ". " if curve else "") + block.caption
                    if block.error_description:
                        caption += " " + block.error_description
                    content.append(image(name, caption, block.alt, block.evidence_ids))
        label = "Synthetic demonstration" if exp["contract"].get("synthetic_demo") else "Experiment report"
        footer = (
            "Available measurements, task context and saved report versions are in the experiment’s Protocol and Evidence tabs."
            if exp["contract"].get("execution_contract") == 3
            else "Detailed protocol, raw measurements and reproducible sources are available in the experiment’s Protocol and Evidence tabs."
        )
        output = (
            f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{esc(doc.title)}</title><style>{CSS}</style></head><body><article><div class="eyebrow">{label} · Report version {version}</div><h1>{esc(doc.title)}</h1><p class="standfirst">{esc(doc.standfirst)}</p>'
            + "".join(content)
            + f'<footer class="report-footer">{footer}</footer></article></body></html>'
        )
        (pending / report_name).write_text(output)
        generated.append(report_name)
        manifest_name = prefix + ".json"
        metadata = {
            "schema_version": 1,
            "renderer_version": RENDERER_VERSION,
            "renderer_source_sha256": artifacts.sha256(Path(__file__)),
            "renderer_dependencies": {name: package_version(name) for name in ("matplotlib", "numpy")},
            "source_snapshot_sha256": (exp.get("snapshot") or {}).get("snapshot_hash"),
            "experiment_id": exp["id"],
            "evidence_revision": exp["evidence_revision"],
            "report_version": version,
            "document": doc.model_dump(mode="json"),
            "source_artifacts": list(evidence.used.values()),
            **(
                {"metric_source_ids": evidence.metric_source_ids}
                if exp["contract"].get("execution_contract") == 3
                else {}
            ),
            "outputs": {name: artifacts.sha256(pending / name) for name in generated},
        }
        (pending / manifest_name).write_text(json.dumps(metadata, indent=2) + "\n")
        generated.append(manifest_name)
        # Compare all existing outputs before moving any. A replay can adopt identical bytes.
        for name in generated:
            target = root / name
            if target.exists() or target.is_symlink():
                if artifacts.sha256(artifacts.allowed_path(target, root)) != artifacts.sha256(pending / name):
                    raise ValueError("report version already exists with different bytes")
        for name in generated:
            target = root / name
            if not target.exists():
                with (pending / name).open("rb") as file:
                    os.fsync(file.fileno())
                shutil.move(pending / name, target)
        return report_name, generated


def verify_report(exp, root, records, version, document):
    """Check a stored presentation package and its sources without rerendering."""
    by_name = {r["name"]: r for r in records}
    name = f"report-v{version}.json"
    if name not in by_name:
        raise ValueError("report provenance is missing")
    iteration = exp["contract"].get("execution_contract") == 3
    if iteration:
        base = root.parent.parent if root.parent.name == "records" else root
        metadata = artifacts.read_json(Path(by_name[name]["path"]), base)
        evidence = Evidence(exp, root, records, metric_source_ids=metadata.get("metric_source_ids", []))
        evidence.read(by_name[name]["id"])
    else:
        evidence = Evidence(exp, root, records)
        metadata = json.loads(evidence.read(by_name[name]["id"]))
    evidence.used.pop(by_name[name]["id"])
    if (metadata.get("experiment_id"), metadata.get("evidence_revision"), metadata.get("report_version")) != (
        exp["id"],
        by_name[name]["evidence_revision"] if iteration else exp["evidence_revision"],
        version,
    ):
        raise ValueError("report provenance identity differs")
    if not iteration and metadata.get("source_snapshot_sha256") != (exp.get("snapshot") or {}).get(
        "snapshot_hash"
    ):
        raise ValueError("report refers to a different scientific snapshot")
    doc = validate_document(evidence, metadata["document"])
    if doc.model_dump(mode="json") != ReportDocument.model_validate(document).model_dump(mode="json"):
        raise ValueError("report document differs from saved intent")
    recorded = {a["id"]: a for a in metadata["source_artifacts"]}
    if recorded != evidence.used:
        raise ValueError("report source registry differs from its document")
    outputs = metadata["outputs"]
    expected_html = "report.html" if version == 1 else f"report-v{version}.html"
    if expected_html not in outputs or len(outputs) > 9:
        raise ValueError("report outputs are incomplete")
    identities = []
    for filename, digest in outputs.items():
        row = (
            next(
                (
                    r
                    for r in records
                    if r["name"] == filename
                    and r["sha256"] == digest
                    and r["evidence_revision"] == metadata["evidence_revision"]
                ),
                None,
            )
            if iteration
            else by_name.get(filename)
        )
        if not row or row["sha256"] != digest:
            raise ValueError("report output registry differs")
        evidence.read(row["id"])
        identities.append(row["id"])
    return list(recorded), identities + [by_name[name]["id"]]
