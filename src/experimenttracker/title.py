"""Select an optional managed small model for concise experiment titles."""

from typing import Literal

from pydantic import Field

from .config import StrictModel


class TitleSummary(StrictModel):
    schema_version: Literal[1] = 1
    title: str = Field(min_length=1, max_length=100)


def luna_title_model(models):
    """Return a catalog-advertised GPT-6 Luna low-effort choice, if available."""
    for model in models:
        name = " ".join(str(model.get(key, "")) for key in ("id", "name")).lower()
        efforts = model.get("reasoning_efforts") or []
        if "luna" in name and "6" in name and "low" in efforts:
            return model["id"], "low"
    return None


def title_prompt(brief, project_name, plan_title):
    return (
        "Write a concise, specific experiment title from the researcher's brief and draft plan title. "
        "Preserve the scientific method and dataset; avoid adding claims about results. "
        "Return only the requested JSON schema. Do not use tools or change files.\n"
        f"Project: {project_name}\nBrief: {brief}\nPlan title: {plan_title}"
    )
