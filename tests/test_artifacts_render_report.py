from experimenttracker.artifacts import render_report
from experimenttracker.models import Analysis


def test_current_fallback_report_renders_metrics_and_escapes_all_labels():
    exp = {
        "id": "experiment-1",
        "brief": "Compare requested methods",
        "evidence_revision": 2,
        "contract": {"execution_contract": 3, "synthetic_demo": True},
        "snapshot": {"snapshot_hash": "snapshot-1"},
        "model": None,
    }
    analysis = Analysis(
        title="Synthetic comparison",
        summary="Two measured variants.",
        observations=[{"text": "Both outputs are available.", "evidence_ids": ["metric-evidence"]}],
        limitations=["Synthetic only."],
        next_ideas=[],
        scientific_outcome="supported",
    )
    html = render_report(
        exp,
        {
            "metrics": [
                {"name": "<score>", "variant": "linear & control", "value": 0.125, "unit": "<units>"},
                {"name": "loss", "variant": "quadratic", "value": 0.25, "unit": "ratio"},
            ]
        },
        analysis,
        ["figure<&1>", "figure-2"],
        {"figure<&1>": "<first plot>", "figure-2": "second plot"},
    )

    assert "&lt;score&gt;" in html and "&lt;units&gt;" in html
    assert "linear &amp; control" in html and "0.125" in html and "0.25" in html
    assert "<score>" not in html and "linear & control" not in html
    assert html.count("<figure>") == 2
    assert "/api/artifacts/figure&lt;&amp;1&gt;" in html
    assert 'alt="&lt;first plot&gt;"' in html
