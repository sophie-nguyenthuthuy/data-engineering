from pathlib import Path

from inflation_crawler.extract import extract_product

FIXTURES = Path(__file__).parent / "fixtures"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text()


def test_jsonld_product():
    p = extract_product(_read("laptop_jsonld.html"), "https://shop.example/laptop", "2024-01-15T00:00:00Z")
    assert p is not None
    assert p.title == "Acme Ultrabook 15"
    assert p.brand == "Acme"
    assert p.price == 749.99
    assert p.currency == "USD"
    assert p.category == "laptops"
    assert p.source == "jsonld"


def test_microdata_product():
    p = extract_product(_read("coffee_microdata.html"), "https://shop.example/coffee", "2024-03-01T00:00:00Z")
    assert p is not None
    assert "Coffee" in p.title
    assert p.price == 14.99
    assert p.source == "microdata"


def test_opengraph_product():
    p = extract_product(_read("tv_opengraph.html"), "https://shop.example/tv", "2024-06-01T00:00:00Z")
    assert p is not None
    assert p.price == 399.0
    assert p.currency == "USD"
    assert p.source == "opengraph"


def test_no_signal_returns_none():
    html = "<html><body>Nothing to see here.</body></html>"
    assert extract_product(html, "https://x/", "2024-01-01T00:00:00Z") is None


def test_stable_id_is_deterministic():
    p1 = extract_product(_read("laptop_jsonld.html"), "https://shop.example/laptop", "2024-01-15T00:00:00Z")
    p2 = extract_product(_read("laptop_jsonld.html"), "https://shop.example/laptop?utm=x", "2024-02-15T00:00:00Z")
    assert p1 and p2
    assert p1.product_id == p2.product_id  # query strings stripped
