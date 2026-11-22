import pytest
from pydantic import ValidationError

from experimenttracker.title import TitleSummary, luna_title_model, title_prompt


def test_luna_selection_requires_advertised_low_effort():
    catalog = [
        {"id": "gpt-6.1-sol", "name": "GPT-6.1 Sol", "reasoning_efforts": ["low", "medium"]},
        {"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low", "medium"]},
    ]
    assert luna_title_model(catalog) == ("gpt-6-luna", "low")
    assert luna_title_model(catalog[:1]) is None
    assert luna_title_model([{**catalog[1], "reasoning_efforts": ["medium"]}]) is None


def test_title_output_is_bounded_and_prompt_includes_brief():
    assert TitleSummary.model_validate({"title": "DINOv3 decoder baseline on SpaceNet 7"}).title
    with pytest.raises(ValidationError):
        TitleSummary.model_validate({"title": "A" * 101})
    prompt = title_prompt("Frozen DINOv3 at scale 1", "anyup-rs", "Segmentation baseline")
    assert "Frozen DINOv3 at scale 1" in prompt
    assert "avoid adding claims about results" in prompt
