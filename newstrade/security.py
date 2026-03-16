from __future__ import annotations

from urllib.parse import urlparse, urlunparse


def normalize_external_url(url: str, allowed_hosts: set[str] | None = None) -> str:
    text = str(url or "").strip()
    if not text:
        return ""

    try:
        parsed = urlparse(text)
    except ValueError:
        return ""

    if parsed.scheme not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""

    hostname = (parsed.hostname or "").strip().lower().rstrip(".")
    if allowed_hosts:
        normalized_hosts = {host.strip().lower().rstrip(".") for host in allowed_hosts if host.strip()}
        if not any(hostname == host or hostname.endswith(f".{host}") for host in normalized_hosts):
            return ""

    return urlunparse(parsed)


def sanitize_text(value: str, max_length: int = 400, multiline: bool = True) -> str:
    text = "".join(ch for ch in str(value or "") if ch >= " " or ch in "\n\t")
    if not multiline:
        text = " ".join(text.replace("\t", " ").replace("\n", " ").split())
    if max_length > 0:
        text = text[:max_length]
    return text
