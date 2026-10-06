"""Human-readable text of one Claude Code transcript line.

Moved verbatim from tools/kbindex.py (v1). Kept: user messages whose content
is a plain string and assistant `text` parts. Dropped: tool_use, tool_result,
thinking, images, attachments, session bookkeeping, meta/slash-command lines,
`<system-reminder>` blocks, base64 blobs.
"""

import re

_SYSTEM_REMINDER = re.compile(r"<system-reminder>.*?</system-reminder>", re.S | re.I)
_TASK_NOTIFICATION = re.compile(r"<task-notification>(.*?)(?:</task-notification>|\Z)", re.S | re.I)
_SUMMARY = re.compile(r"<summary>(.*?)</summary>", re.S | re.I)
_COMMAND_BLOCK = re.compile(
    r"<(command-name|command-message|command-args|local-command-stdout|"
    r"local-command-stderr|local-command-caveat)>.*?</\1>",
    re.S | re.I,
)
_OPEN_COMMAND_TAG = re.compile(
    r"</?(command-name|command-message|command-args|local-command-stdout|"
    r"local-command-stderr|local-command-caveat)>",
    re.I,
)
_DATA_URI = re.compile(r"data:[\w.+-]+/[\w.+-]+;base64,[A-Za-z0-9+/=]+")
_LONG_B64 = re.compile(r"\b[A-Za-z0-9+/]{200,}={0,2}\b")

MIN_TEXT_LEN = 2


def clean_text(text: str) -> str:
    """Strip machine noise from a raw message string."""
    if not text:
        return ""
    m = _TASK_NOTIFICATION.search(text)
    if m:
        summary = _SUMMARY.search(m.group(1))
        text = summary.group(1) if summary else ""
    text = _SYSTEM_REMINDER.sub(" ", text)
    text = _COMMAND_BLOCK.sub(" ", text)
    text = _OPEN_COMMAND_TAG.sub(" ", text)
    text = _DATA_URI.sub(" ", text)
    text = _LONG_B64.sub(" ", text)
    return text.strip()


def extract_message_text(obj):
    """Human-readable conversation text of one transcript line, or None.

    Kept: user messages whose content is a plain string, and assistant `text`
    content parts. Everything else — tool_use, tool_result, thinking, images,
    attachments, session bookkeeping, meta/slash-command lines — is noise.
    """
    if not isinstance(obj, dict):
        return None
    if obj.get("type") not in ("user", "assistant"):
        return None
    if obj.get("isMeta"):
        return None
    message = obj.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")

    if isinstance(content, str):
        parts = [content]
    elif isinstance(content, list):
        parts = [
            p.get("text") or ""
            for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        ]
    else:
        return None

    cleaned = [c for c in (clean_text(p) for p in parts) if len(c) >= MIN_TEXT_LEN]
    if not cleaned:
        return None
    return "\n".join(cleaned)
