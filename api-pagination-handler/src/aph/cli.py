"""``aphctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from aph import __version__

    print(f"api-pagination-handler {__version__}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from aph.client import PaginatedClient
    from aph.paginators.offset import OffsetPaginator
    from aph.transport import Response

    # Synthetic two-page offset API: 5 records per page, 8 total.
    def fake_transport(url: str, _headers: object) -> Response:
        from urllib.parse import parse_qsl, urlsplit

        q = dict(parse_qsl(urlsplit(url).query))
        offset = int(q.get("offset", "0"))
        limit = int(q.get("limit", "5"))
        all_records = [{"id": i} for i in range(8)]
        chunk = all_records[offset : offset + limit]
        return Response(status=200, body={"items": chunk, "total": len(all_records)}, url=url)

    client = PaginatedClient(
        transport=fake_transport,
        paginator=OffsetPaginator(limit=args.limit, records_path="items"),
    )
    records = client.fetch_all(args.base_url)
    print(f"records={len(records)} ids={[r['id'] for r in records]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="aphctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    d = sub.add_parser("demo", help="run a fake offset-paginated API end-to-end")
    d.add_argument("--base-url", dest="base_url", default="https://example.com/items")
    d.add_argument("--limit", type=int, default=5)
    d.set_defaults(func=cmd_demo)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
