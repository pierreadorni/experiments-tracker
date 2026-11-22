"""Bounded numeric job telemetry and sanitized agent activity descriptions."""

import re
from pathlib import PurePath

PROGRESS_PREFIX = "ET_PROGRESS "
LOG_TAIL_BYTES = 256 * 1024


def safe_filename(value):
    name = PurePath(str(value)).name
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,120}", name):
        return "context"
    if any(word in name.lower() for word in ("token", "credential", "secret", "auth", ".env")):
        return "context"
    return name


def redact_secrets(value):
    """Replace bearer credentials and known token prefixes in public text."""
    text = str(value)
    text = re.sub(r"(?i)bearer\s+[a-z0-9._~+/-]+", "Bearer [redacted]", text)
    return re.sub(r"\b(?:sk-|ghp_|ghs_)[a-zA-Z0-9_-]+", "[redacted]", text)


def public_activity_text(value):
    """Bound public model summaries; never use raw reasoning or tool output."""
    return redact_secrets(" ".join(str(value).split()))[:1200]


class DiscussionActivity:
    """Accumulate only public reasoning summaries, publishing at most once per two seconds/item."""

    def __init__(self):
        self.action_id = None
        self.items = {}

    def describe(self, action_id, message, now):
        if action_id != self.action_id:
            self.action_id, self.items = action_id, {}
        params = message.get("params", {})
        if message["method"] == "item/reasoning/summaryTextDelta":
            identity, index, delta = params.get("itemId"), params.get("summaryIndex"), params.get("delta")
            if not isinstance(identity, str) or not 1 <= len(identity) <= 200:
                return None
            if type(index) is not int or not 0 <= index < 8 or not isinstance(delta, str):
                return None
            if identity not in self.items and len(self.items) >= 60:
                return None
            item = self.items.setdefault(identity, {"parts": {}, "published_at": None})
            item["parts"][index] = (item["parts"].get(index, "") + delta)[:1200]
            if item["published_at"] is not None and now - item["published_at"] < 2:
                return None
            item["published_at"] = now
            summary = public_activity_text(" ".join(item["parts"][i] for i in sorted(item["parts"])))
            return (identity, "reasoning", summary, "running") if summary else None
        item = params.get("item", {})
        if item.get("type") == "reasoning" and not item.get("summary"):
            buffered = self.items.get(item.get("id"), {}).get("parts", {})
            if buffered:
                message = {
                    **message,
                    "params": {
                        **params,
                        "item": {**item, "summary": [buffered[i] for i in sorted(buffered)]},
                    },
                }
        return describe_item(message, public_text=True)


def describe_item(message, *, public_text=False):
    item = message.get("params", {}).get("item", {})
    identity, kind = item.get("id"), item.get("type")
    if not isinstance(identity, str) or not 1 <= len(identity) <= 200:
        return None
    complete = message["method"] == "item/completed"
    state = "complete" if complete else "running"
    if kind == "reasoning":
        parts = item.get("summary", []) if public_text else []
        summary = public_activity_text(" ".join(p for p in parts if isinstance(p, str)))
        summary = summary or ("Thinking complete" if complete else "Thinking…")
    elif kind == "agentMessage" and public_text and item.get("phase") == "commentary":
        summary = public_activity_text(item.get("text", ""))
        if not summary:
            return None
    elif kind == "commandExecution":
        actions = item.get("commandActions", [])
        action = actions[0] if actions else {}
        action_type = action.get("type")
        if action_type == "read":
            summary = ("Read " if complete else "Reading ") + safe_filename(
                action.get("path", action.get("name", "context"))
            )
        elif action_type == "search":
            summary = "Source search finished" if complete else "Searching source files…"
        elif action_type == "listFiles":
            summary = "Files listed" if complete else "Listing files…"
        else:
            summary = "Check finished" if complete else "Running a check…"
        if complete and item.get("exitCode") not in (None, 0):
            state = "failed"
            summary = "Check failed"
    elif kind == "fileChange":
        changes = item.get("changes", [])
        names = [safe_filename(change.get("path", "file")) for change in changes[:3]]
        added = removed = 0
        for change in changes:
            for line in str(change.get("diff", "")).splitlines():
                added += line.startswith("+") and not line.startswith("+++")
                removed += line.startswith("-") and not line.startswith("---")
        verb = "written" if complete else "being written"
        summary = f"{len(changes)} files {verb} · +{added} −{removed}"
        if names:
            summary += " · " + ", ".join(names)
        if item.get("status") in ("failed", "declined"):
            state, summary = "failed", "File changes were not applied"
    elif kind in ("mcpToolCall", "dynamicToolCall", "webSearch", "imageView"):
        # Names/arguments from tools can contain arbitrary content. Emit only a fixed label.
        summary = "Tool finished" if complete else "Using a tool…"
    else:
        return None
    return identity, kind, summary[: 1200 if public_text else 300], state
