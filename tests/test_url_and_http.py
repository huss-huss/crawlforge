import pytest

from crawlforge.http import Request, Response
from crawlforge.url import domain_of, fingerprint, normalize_url


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("HTTP://Example.COM", "http://example.com/"),
        ("http://example.com:80/a", "http://example.com/a"),
        ("https://example.com:443/a", "https://example.com/a"),
        ("http://example.com/a?b=2&a=1", "http://example.com/a?a=1&b=2"),
        ("http://example.com/a#section", "http://example.com/a"),
        ("http://example.com/a/./b/../c", "http://example.com/a/c"),
        ("http://example.com/%7Euser", "http://example.com/~user"),
    ],
)
def test_equivalent_urls_normalize_identically(a, b):
    assert normalize_url(a) == normalize_url(b)


def test_distinct_urls_stay_distinct():
    assert normalize_url("http://example.com/a") != normalize_url("http://example.com/a/")
    assert normalize_url("http://example.com:8080/") != normalize_url("http://example.com/")
    assert normalize_url("http://example.com/?a=") != normalize_url("http://example.com/")


def test_fingerprint_depends_on_method_and_body():
    url = "http://example.com/x?b=1&a=2"
    assert fingerprint("get", url) == fingerprint("GET", "http://EXAMPLE.com/x?a=2&b=1")
    assert fingerprint("GET", url) != fingerprint("POST", url)
    assert fingerprint("POST", url, b"a") != fingerprint("POST", url, b"b")


def test_domain_of_keeps_non_default_port():
    assert domain_of("https://Books.toscrape.com/a") == "books.toscrape.com"
    assert domain_of("http://localhost:8080/") == "localhost:8080"


class _Spider:
    def parse_item(self, response):  # pragma: no cover - only the name matters
        pass


def test_request_roundtrip_and_callback_by_name():
    req = Request(
        "http://example.com/",
        callback=_Spider().parse_item,
        method="post",
        body=b"\x00binary",
        meta={"k": 1},
        priority=5,
        depth=2,
        render=True,
    )
    assert req.callback == "parse_item"
    assert req.method == "POST"
    clone = Request.from_dict(req.to_dict())
    assert clone == req
    assert clone.fingerprint == req.fingerprint


def test_response_selectors_and_follow():
    html = b"<html><body><a href='/next'>n</a><p class='x'>hello</p></body></html>"
    req = Request("http://example.com/dir/page")
    resp = Response(
        url=req.url, status=200, headers={"content-type": "text/html"}, body=html, request=req
    )
    assert resp.css("p.x::text").get() == "hello"
    assert resp.xpath("//a/@href").get() == "/next"
    child = resp.follow("/next", callback="parse_next", priority=3)
    assert child.url == "http://example.com/next"
    assert child.callback == "parse_next"
    assert child.priority == 3
