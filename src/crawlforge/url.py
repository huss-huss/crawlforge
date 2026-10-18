"""URL normalization and request fingerprinting used for de-duplication."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

_DEFAULT_PORTS = {"http": 80, "https": 443}


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    # Resolve dot segments ("/a/./b/../c" -> "/a/c") while keeping a trailing slash.
    segments: list[str] = []
    for segment in path.split("/"):
        if segment == ".":
            continue
        if segment == "..":
            if len(segments) > 1:
                segments.pop()
            continue
        segments.append(segment)
    if path.endswith(("/.", "/..")):
        segments.append("")
    normalized = "/".join(segments)
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    # Re-quote consistently so "%7E" and "~" compare equal.
    return quote(unquote(normalized), safe="/:@!$&'()*+,;=-._~")


def normalize_url(url: str, *, keep_fragment: bool = False) -> str:
    """Return a canonical form of ``url``.

    - scheme and host are lower-cased, default ports dropped
    - empty path becomes ``/``; dot segments are resolved
    - query parameters are sorted (blank values kept)
    - the fragment is removed unless ``keep_fragment`` is set
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower().rstrip(".")
    port = parts.port
    netloc = host
    if parts.username or parts.password:
        userinfo = parts.username or ""
        if parts.password:
            userinfo += f":{parts.password}"
        netloc = f"{userinfo}@{netloc}"
    if port is not None and _DEFAULT_PORTS.get(scheme) != port:
        netloc = f"{netloc}:{port}"
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    fragment = parts.fragment if keep_fragment else ""
    return urlunsplit((scheme, netloc, _normalize_path(parts.path), query, fragment))


def domain_of(url: str) -> str:
    """Host (with non-default port) used as the politeness / breaker key."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and _DEFAULT_PORTS.get(parts.scheme.lower()) != port:
        return f"{host}:{port}"
    return host


def fingerprint(method: str, url: str, body: bytes | None = None) -> str:
    """Stable SHA-1 fingerprint of a request (method + normalized URL + body)."""
    h = hashlib.sha1(usedforsecurity=False)
    h.update(method.upper().encode())
    h.update(b"\x00")
    h.update(normalize_url(url).encode())
    h.update(b"\x00")
    if body:
        h.update(body)
    return h.hexdigest()
