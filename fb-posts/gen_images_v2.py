"""Generate 12 more FB-post illustrations (posts 7-18), cloud-themed."""
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
import random

OUT = Path(__file__).parent / "images"
OUT.mkdir(exist_ok=True)

W, H = 1200, 675
FONT = "/System/Library/Fonts/Helvetica.ttc"
FONT_BOLD = "/System/Library/Fonts/HelveticaNeue.ttc"


def font(size, bold=False):
    return ImageFont.truetype(FONT_BOLD if bold else FONT, size, index=1 if bold else 0)


def gradient(c1, c2):
    img = Image.new("RGB", (W, H), c1)
    top = Image.new("RGB", (W, H), c2)
    mask = Image.new("L", (W, H))
    data = []
    for y in range(H):
        data.extend([int(255 * y / H)] * W)
    mask.putdata(data)
    img.paste(top, (0, 0), mask)
    return img


def text(draw, xy, s, size, color="white", bold=False, anchor="lt"):
    draw.text(xy, s, font=font(size, bold), fill=color, anchor=anchor)


def card(draw, x, y, w, h, fill, radius=20):
    draw.rounded_rectangle([x, y, x + w, y + h], radius=radius, fill=fill)


def footer(d, repo):
    text(d, (W // 2, 635), f"data-engineering / {repo}", 18, (200, 210, 230), anchor="mm")


# ---------- 07. Tiered Storage / S3 cost ----------
def img7():
    im = gradient((20, 35, 55), (10, 60, 90))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Cat 70%  chi phi  S3", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Tiered storage  -  Hot / Warm / Cold / Archive", 24, (200, 220, 240), anchor="mm")

    tiers = [
        (120, "HOT",     "S3 Standard",      "$0.023/GB", (230, 90, 80)),
        (390, "WARM",    "S3 IA",            "$0.0125/GB", (240, 160, 70)),
        (660, "COLD",    "Glacier IR",       "$0.004/GB",  (90, 160, 220)),
        (930, "ARCHIVE", "Deep Archive",     "$0.00099/GB",(70, 90, 160)),
    ]
    for x, name, sub, price, color in tiers:
        card(d, x, 200, 180, 280, color)
        text(d, (x + 90, 250), name, 28, "white", bold=True, anchor="mm")
        text(d, (x + 90, 305), sub, 16, "white", anchor="mm")
        text(d, (x + 90, 410), price, 22, (255, 250, 200), bold=True, anchor="mm")

    text(d, (W // 2, 560), "lifecycle rule  -  auto-tier  -  glacier restore", 22, (220, 230, 250), anchor="mm")
    footer(d, "tiered-storage-orchestrator")
    im.save(OUT / "07_tiered_storage.png")


# ---------- 08. Serverless Autoscaler ----------
def img8():
    im = gradient((10, 30, 60), (60, 30, 100))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Serverless ETL  -  scale 0 to 1000", 44, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Khong server  -  khong idle cost  -  tu scale", 24, (210, 220, 250), anchor="mm")

    # bar chart: idle -> spike
    random.seed(3)
    base_y = 480
    for i in range(40):
        x = 120 + i * 24
        if i < 10:
            h = random.randint(20, 40)
        elif i < 18:
            h = random.randint(60, 120)
        elif i < 25:
            h = random.randint(220, 320)
        elif i < 32:
            h = random.randint(100, 200)
        else:
            h = random.randint(30, 60)
        color = (140, 220, 255) if h < 150 else (255, 200, 100) if h < 250 else (255, 130, 130)
        d.rectangle([x, base_y - h, x + 16, base_y], fill=color)

    text(d, (250, 540), "idle = 0$", 18, (140, 220, 255), bold=True, anchor="mm")
    text(d, (640, 540), "burst = autoscale", 18, (255, 200, 100), bold=True, anchor="mm")
    text(d, (1000, 540), "back to 0", 18, (140, 220, 255), bold=True, anchor="mm")

    footer(d, "serverless-autoscaler")
    im.save(OUT / "08_serverless_autoscaler.png")


# ---------- 09. Multi-region data mesh ----------
def img9():
    im = gradient((15, 25, 55), (40, 70, 120))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Multi-region Data Mesh", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Du lieu o 3 chau luc  -  query nhu o 1 cho", 24, (200, 220, 250), anchor="mm")

    # three region nodes
    regions = [
        (250, 350, "US-East",   "Sales domain",     (90, 180, 230)),
        (600, 280, "EU-West",   "Finance domain",   (255, 180, 100)),
        (950, 380, "AP-South",  "Ops domain",       (140, 220, 160)),
    ]
    # links
    for i, a in enumerate(regions):
        for b in regions[i + 1:]:
            d.line([(a[0], a[1]), (b[0], b[1])], fill=(120, 180, 220), width=3)

    for cx, cy, name, sub, color in regions:
        d.ellipse([cx - 80, cy - 80, cx + 80, cy + 80], fill=color)
        text(d, (cx, cy - 10), name, 22, "white", bold=True, anchor="mm")
        text(d, (cx, cy + 20), sub, 14, (240, 240, 240), anchor="mm")

    text(d, (W // 2, 540), "domain ownership  -  federated catalog  -  global query", 22, (220, 230, 250), anchor="mm")
    footer(d, "multi-region-data-mesh")
    im.save(OUT / "09_multi_region_mesh.png")


# ---------- 10. Lakehouse migration on-prem -> cloud ----------
def img10():
    im = gradient((30, 40, 50), (50, 100, 110))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "On-prem  ->  Cloud  Lakehouse", 44, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Migrate 200TB Hive  ->  Iceberg on S3", 24, (200, 220, 230), anchor="mm")

    # left box: on-prem
    card(d, 80, 220, 380, 280, (110, 70, 70))
    text(d, (270, 260), "ON-PREM", 28, "white", bold=True, anchor="mm")
    text(d, (270, 320), "Hadoop HDFS", 22, "white", anchor="mm")
    text(d, (270, 360), "Hive Metastore", 22, "white", anchor="mm")
    text(d, (270, 400), "Cluster YARN", 22, "white", anchor="mm")
    text(d, (270, 460), "200TB  -  legacy", 18, (255, 220, 200), anchor="mm")

    # arrow
    for ax in range(490, 700, 12):
        d.polygon([(ax, 350), (ax + 8, 360), (ax, 370)], fill=(255, 230, 120))

    # right box: cloud
    card(d, 740, 220, 380, 280, (50, 130, 110))
    text(d, (930, 260), "CLOUD", 28, "white", bold=True, anchor="mm")
    text(d, (930, 320), "S3 / GCS", 22, "white", anchor="mm")
    text(d, (930, 360), "Iceberg + Glue", 22, "white", anchor="mm")
    text(d, (930, 400), "Athena / EMR Serverless", 18, "white", anchor="mm")
    text(d, (930, 460), "elastic  -  open format", 18, (200, 255, 220), anchor="mm")

    text(d, (W // 2, 560), "zero downtime  -  parallel run  -  cut-over plan", 22, (220, 230, 230), anchor="mm")
    footer(d, "lakehouse-migration")
    im.save(OUT / "10_lakehouse_migration.png")


# ---------- 11. Multi-tenant SaaS data platform ----------
def img11():
    im = gradient((30, 20, 50), (80, 30, 100))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Multi-tenant  Data Platform", 46, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "1 cluster  -  1000 customer  -  data isolation", 24, (210, 200, 240), anchor="mm")

    # Big platform card
    card(d, 350, 200, 500, 130, (90, 60, 180))
    text(d, (600, 240), "SHARED PLATFORM", 24, "white", bold=True, anchor="mm")
    text(d, (600, 280), "compute  +  metadata  +  catalog", 18, (220, 210, 250), anchor="mm")

    # 6 tenant icons below
    for i in range(6):
        x = 150 + i * 160
        card(d, x, 400, 130, 130, (60 + i * 25, 100, 200 - i * 15))
        text(d, (x + 65, 440), f"Tenant {i+1}", 18, "white", bold=True, anchor="mm")
        text(d, (x + 65, 475), "schema_" + str(i + 1), 14, (230, 230, 250), anchor="mm")
        # line from platform to tenant
        d.line([(600, 330), (x + 65, 400)], fill=(180, 180, 220), width=2)

    text(d, (W // 2, 580), "row-level security  -  resource quota  -  per-tenant cost", 22, (220, 220, 250), anchor="mm")
    footer(d, "multi-tenant-platform")
    im.save(OUT / "11_multi_tenant.png")


# ---------- 12. Zero-downtime pipeline upgrades ----------
def img12():
    im = gradient((20, 40, 30), (50, 90, 60))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Zero-downtime  pipeline upgrade", 44, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Doi schema  -  doi engine  -  khong gian doan", 24, (210, 240, 220), anchor="mm")

    # blue-green: two pipelines side by side
    # OLD (blue)
    card(d, 100, 230, 460, 260, (60, 100, 180))
    text(d, (330, 270), "v1  -  CURRENT", 24, "white", bold=True, anchor="mm")
    text(d, (330, 320), "Spark 3.3", 20, "white", anchor="mm")
    text(d, (330, 355), "Schema v1", 20, "white", anchor="mm")
    text(d, (330, 405), "traffic 100% -> 0%", 18, (200, 220, 255), anchor="mm")

    # NEW (green)
    card(d, 640, 230, 460, 260, (60, 170, 110))
    text(d, (870, 270), "v2  -  NEW", 24, "white", bold=True, anchor="mm")
    text(d, (870, 320), "Spark 4.0", 20, "white", anchor="mm")
    text(d, (870, 355), "Schema v2", 20, "white", anchor="mm")
    text(d, (870, 405), "traffic 0% -> 100%", 18, (200, 255, 220), anchor="mm")

    text(d, (W // 2, 540), "blue-green  -  shadow run  -  diff check  -  rollback", 22, (220, 240, 220), anchor="mm")
    footer(d, "zero-downtime-pipeline-upgrades")
    im.save(OUT / "12_zero_downtime.png")


# ---------- 13. Reverse ETL ----------
def img13():
    im = gradient((30, 30, 60), (90, 30, 80))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Reverse  ETL", 50, "white", bold=True, anchor="mm")
    text(d, (W // 2, 115), "Data Warehouse  ->  Salesforce / HubSpot / Slack", 22, (220, 210, 240), anchor="mm")

    # DW box
    card(d, 100, 280, 260, 160, (80, 150, 220))
    text(d, (230, 330), "Data Warehouse", 22, "white", bold=True, anchor="mm")
    text(d, (230, 370), "Snowflake / BQ", 18, (220, 240, 255), anchor="mm")

    # arrow
    for ax in range(380, 580, 12):
        d.polygon([(ax, 350), (ax + 8, 360), (ax, 370)], fill=(255, 200, 100))

    # tools column
    tools = [("Salesforce", (60, 130, 200)),
             ("HubSpot",    (255, 120, 90)),
             ("Slack",      (140, 80, 180)),
             ("Marketo",    (90, 180, 130))]
    for i, (name, color) in enumerate(tools):
        x = 620 + (i % 2) * 250
        y = 230 + (i // 2) * 130
        card(d, x, y, 220, 100, color)
        text(d, (x + 110, y + 50), name, 22, "white", bold=True, anchor="mm")

    text(d, (W // 2, 570), "operational analytics  -  sync customer 360 vao tools", 22, (220, 220, 240), anchor="mm")
    footer(d, "reverse-etl")
    im.save(OUT / "13_reverse_etl.png")


# ---------- 14. Medallion architecture ----------
def img14():
    im = gradient((25, 25, 35), (60, 60, 80))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Medallion  Architecture", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Bronze  ->  Silver  ->  Gold  -  pattern lakehouse chuan", 22, (210, 210, 230), anchor="mm")

    layers = [
        (130, "BRONZE", "Raw / Landed",      "as-is from source", (180, 100, 60)),
        (475, "SILVER", "Cleaned / Joined",  "deduped, typed",    (200, 200, 220)),
        (820, "GOLD",   "Business / BI",     "aggregated, modeled",(230, 190, 70)),
    ]
    for x, name, sub, sub2, color in layers:
        card(d, x, 220, 250, 280, color)
        text(d, (x + 125, 270), name, 30, "white", bold=True, anchor="mm")
        text(d, (x + 125, 320), sub, 20, "white", anchor="mm")
        text(d, (x + 125, 360), sub2, 16, (255, 255, 255, 200), anchor="mm")

    # arrows
    for ax in (390, 740):
        d.polygon([(ax, 350), (ax + 25, 360), (ax, 370)], fill="white")
        d.line([(ax - 30, 360), (ax + 22, 360)], fill="white", width=4)

    text(d, (W // 2, 555), "incremental  -  quality gates  -  contract giua cac layer", 22, (220, 220, 240), anchor="mm")
    footer(d, "medallion-lakehouse")
    im.save(OUT / "14_medallion.png")


# ---------- 15. Data Catalog + Lineage ----------
def img15():
    im = gradient((10, 35, 55), (30, 80, 110))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Data Catalog  +  Lineage", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Truy ve cot 'revenue'  ->  bay nguon goc trong 2 giay", 22, (200, 220, 240), anchor="mm")

    # lineage DAG-like
    nodes = [
        (150, 280, "orders_raw",      (220, 100, 90)),
        (150, 420, "events_raw",      (220, 100, 90)),
        (450, 280, "orders_clean",    (200, 200, 220)),
        (450, 420, "events_clean",    (200, 200, 220)),
        (750, 350, "user_facts",      (200, 200, 220)),
        (1020, 350,"revenue_daily",   (240, 200, 80)),
    ]
    edges = [(0, 2), (1, 3), (2, 4), (3, 4), (4, 5)]
    for a, b in edges:
        d.line([nodes[a][0], nodes[a][1], nodes[b][0], nodes[b][1]], fill=(180, 210, 230), width=3)
    for x, y, name, color in nodes:
        card(d, x - 90, y - 30, 180, 60, color)
        text(d, (x, y), name, 16, "white", bold=True, anchor="mm")

    text(d, (W // 2, 555), "auto-discover  -  column-level lineage  -  impact analysis", 22, (220, 230, 240), anchor="mm")
    footer(d, "data-catalog-lineage")
    im.save(OUT / "15_catalog_lineage.png")


# ---------- 16. Data Contracts ----------
def img16():
    im = gradient((25, 30, 60), (60, 40, 110))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Data Contracts", 50, "white", bold=True, anchor="mm")
    text(d, (W // 2, 115), "Producer  va  Consumer  ky 'hop dong'  truoc khi ship", 22, (210, 210, 250), anchor="mm")

    # Producer box left
    card(d, 80, 250, 320, 220, (90, 130, 220))
    text(d, (240, 295), "PRODUCER", 26, "white", bold=True, anchor="mm")
    text(d, (240, 345), "backend team", 20, (220, 230, 255), anchor="mm")
    text(d, (240, 395), "publishes events", 18, (220, 230, 255), anchor="mm")

    # Contract scroll middle
    card(d, 450, 250, 300, 220, (240, 220, 130))
    text(d, (600, 295), "CONTRACT", 26, (60, 50, 30), bold=True, anchor="mm")
    text(d, (600, 345), "schema + SLA", 18, (60, 50, 30), anchor="mm")
    text(d, (600, 375), "owner + version", 18, (60, 50, 30), anchor="mm")
    text(d, (600, 415), "CI-enforced", 18, (160, 100, 30), bold=True, anchor="mm")

    # Consumer box right
    card(d, 800, 250, 320, 220, (90, 200, 140))
    text(d, (960, 295), "CONSUMER", 26, "white", bold=True, anchor="mm")
    text(d, (960, 345), "data team", 20, (220, 255, 230), anchor="mm")
    text(d, (960, 395), "builds analytics", 18, (220, 255, 230), anchor="mm")

    text(d, (W // 2, 555), "no surprise breakage  -  shift-left quality", 22, (220, 220, 240), anchor="mm")
    footer(d, "data-contract-platform")
    im.save(OUT / "16_data_contracts.png")


# ---------- 17. Column-level encryption ----------
def img17():
    im = gradient((30, 15, 30), (60, 25, 60))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Column-level  Encryption", 46, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Ma hoa PII trong DW  -  GDPR / PCI compliant", 22, (230, 200, 220), anchor="mm")

    # Table simulation
    card(d, 150, 200, 900, 320, (50, 30, 60))
    cols = ["user_id", "email", "name", "ccn", "country"]
    cell_w = 180
    for i, c in enumerate(cols):
        x = 150 + i * cell_w
        d.rectangle([x, 200, x + cell_w, 250], fill=(90, 60, 110))
        text(d, (x + cell_w // 2, 225), c, 18, "white", bold=True, anchor="mm")

    rows = [
        ["1001", "AES{xK9p}", "AES{Ml2}", "AES{4q8}", "VN"],
        ["1002", "AES{aB3r}", "AES{Yt7}", "AES{p1z}", "US"],
        ["1003", "AES{qP9w}", "AES{Vh4}", "AES{r6m}", "JP"],
        ["1004", "AES{ds8k}", "AES{Bn0}", "AES{8wj}", "DE"],
    ]
    for r, row in enumerate(rows):
        y = 260 + r * 55
        for c, val in enumerate(row):
            x = 150 + c * cell_w
            color = (200, 60, 60) if "AES" in val else (40, 20, 50)
            d.rectangle([x, y, x + cell_w, y + 55], fill=color)
            text(d, (x + cell_w // 2, y + 28), val, 14 if "AES" in val else 16, "white", anchor="mm")

    text(d, (W // 2, 565), "KMS  -  envelope encryption  -  per-column key rotation", 20, (230, 210, 230), anchor="mm")
    footer(d, "column-encryption-pipeline")
    im.save(OUT / "17_column_encryption.png")


# ---------- 18. Self-healing ETL ----------
def img18():
    im = gradient((15, 30, 25), (40, 80, 60))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Self-healing  ETL", 50, "white", bold=True, anchor="mm")
    text(d, (W // 2, 115), "Pipeline  tu sua  khi gap loi  -  3am khong can wake-up", 22, (210, 240, 220), anchor="mm")

    # Cycle diagram
    cx, cy = W // 2, 360
    radius = 160
    nodes = [
        (cx, cy - radius,       "DETECT",  (220, 100, 90)),
        (cx + radius, cy,       "DIAGNOSE",(240, 180, 80)),
        (cx, cy + radius,       "RECOVER", (90, 200, 140)),
        (cx - radius, cy,       "VERIFY",  (90, 160, 220)),
    ]
    # arrows along circle (approx)
    for i in range(4):
        a = nodes[i]
        b = nodes[(i + 1) % 4]
        d.line([a[0], a[1], b[0], b[1]], fill=(220, 240, 230), width=3)

    for x, y, name, color in nodes:
        d.ellipse([x - 80, y - 50, x + 80, y + 50], fill=color)
        text(d, (x, y), name, 20, "white", bold=True, anchor="mm")

    text(d, (W // 2, 565), "retry  -  backfill  -  circuit breaker  -  auto-rerun", 22, (220, 240, 220), anchor="mm")
    footer(d, "self-healing-etl")
    im.save(OUT / "18_self_healing.png")


if __name__ == "__main__":
    for fn in (img7, img8, img9, img10, img11, img12, img13, img14, img15, img16, img17, img18):
        fn()
    print("done")
