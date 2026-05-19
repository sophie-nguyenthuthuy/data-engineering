"""Generate 6 FB-post illustrations as PNG (1200x675, 16:9)."""
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path

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
    for y in range(H):
        for x in range(W):
            pass
    # vertical gradient via paste with alpha mask
    mask_data = []
    for y in range(H):
        mask_data.extend([int(255 * y / H)] * W)
    mask.putdata(mask_data)
    img.paste(top, (0, 0), mask)
    return img


def text(draw, xy, s, size, color="white", bold=False, anchor="lt"):
    draw.text(xy, s, font=font(size, bold), fill=color, anchor=anchor)


def card(draw, x, y, w, h, fill, radius=20):
    draw.rounded_rectangle([x, y, x + w, y + h], radius=radius, fill=fill)


# ---------- 1. Parquet vs ORC vs Avro ----------
def img1():
    im = gradient((20, 30, 60), (80, 50, 130))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Parquet  vs  ORC  vs  Avro", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Format nao nhanh nhat?", 28, (200, 200, 230), anchor="mm")

    boxes = [
        (180, 200, 240, 280, (255, 200, 80), "Parquet", "Gold"),
        (480, 200, 240, 320, (200, 200, 210), "ORC", "Silver"),
        (780, 200, 240, 240, (210, 140, 70), "Avro", "Bronze"),
    ]
    for x, y, w, h, c, name, sub in boxes:
        card(d, x, y, w, h, c)
        text(d, (x + w // 2, y + h - 70), name, 34, "white", bold=True, anchor="mm")
        text(d, (x + w // 2, y + h - 30), sub, 20, (255, 255, 255, 200), anchor="mm")

    text(d, (W // 2, 580), "10GB benchmark - read / write / compression", 22, (220, 220, 240), anchor="mm")
    text(d, (W // 2, 625), "data-engineering / parquet-vs-orc-vs-avro-lab", 18, (180, 180, 220), anchor="mm")
    im.save(OUT / "01_parquet_orc_avro.png")


# ---------- 2. Delta vs Iceberg vs Hudi ----------
def img2():
    im = gradient((10, 40, 70), (30, 90, 130))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Delta  vs  Iceberg  vs  Hudi", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Chon table format cho Lakehouse 2026", 26, (180, 220, 240), anchor="mm")

    # three iceberg triangles
    icebergs = [
        (250, (90, 180, 230), "Delta", "Spark-first"),
        (600, (130, 220, 200), "Iceberg", "Open standard"),
        (950, (180, 220, 240), "Hudi", "CDC / Upsert"),
    ]
    for cx, color, name, sub in icebergs:
        d.polygon([(cx - 110, 460), (cx + 110, 460), (cx, 230)], fill=color)
        d.polygon([(cx - 60, 460), (cx + 60, 460), (cx, 380)], fill=(255, 255, 255, 80), outline=None)
        text(d, (cx, 495), name, 30, "white", bold=True, anchor="mm")
        text(d, (cx, 525), sub, 18, (220, 240, 255), anchor="mm")

    text(d, (W // 2, 600), "50M rows  -  MERGE  -  time travel  -  schema evolution", 20, (200, 230, 250), anchor="mm")
    text(d, (W // 2, 640), "data-engineering / delta-vs-iceberg-vs-hudi", 18, (180, 200, 230), anchor="mm")
    im.save(OUT / "02_delta_iceberg_hudi.png")


# ---------- 3. Postgres vs ClickHouse ----------
def img3():
    im = gradient((30, 30, 50), (10, 10, 25))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Postgres  vs  ClickHouse", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "100 trieu dong  -  ket qua gay soc", 26, (200, 200, 220), anchor="mm")

    # Postgres side (left, slow)
    card(d, 80, 200, 480, 320, (50, 80, 130))
    text(d, (320, 250), "Postgres", 38, "white", bold=True, anchor="mm")
    text(d, (320, 310), "OLTP  -  Row store", 22, (180, 200, 240), anchor="mm")
    text(d, (320, 410), "47s", 100, (255, 180, 100), bold=True, anchor="mm")
    text(d, (320, 490), "full scan analytics", 18, (200, 200, 220), anchor="mm")

    # ClickHouse side (right, fast)
    card(d, 640, 200, 480, 320, (180, 140, 30))
    text(d, (880, 250), "ClickHouse", 38, "white", bold=True, anchor="mm")
    text(d, (880, 310), "OLAP  -  Columnar", 22, (255, 230, 180), anchor="mm")
    text(d, (880, 410), "0.3s", 100, (140, 255, 180), bold=True, anchor="mm")
    text(d, (880, 490), "vectorized execution", 18, (255, 235, 200), anchor="mm")

    text(d, (W // 2, 580), "150x faster", 30, (140, 255, 180), bold=True, anchor="mm")
    text(d, (W // 2, 625), "data-engineering / postgres-vs-clickhouse-benchmark", 18, (180, 180, 200), anchor="mm")
    im.save(OUT / "03_postgres_clickhouse.png")


# ---------- 4. MinIO + Iceberg + Trino ----------
def img4():
    im = gradient((25, 50, 50), (60, 100, 110))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Lakehouse mien phi  100%", 48, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "MinIO  +  Iceberg  +  Trino  -  docker-compose up", 24, (200, 230, 220), anchor="mm")

    # three pipeline boxes
    steps = [
        (130, 250, "MinIO", "Object Storage", (220, 80, 80)),
        (490, 250, "Iceberg", "Table Format", (100, 180, 220)),
        (850, 250, "Trino", "Query Engine", (255, 200, 100)),
    ]
    for x, y, name, sub, color in steps:
        card(d, x, y, 220, 220, color)
        text(d, (x + 110, y + 90), name, 32, "white", bold=True, anchor="mm")
        text(d, (x + 110, y + 130), sub, 18, "white", anchor="mm")

    # arrows
    for ax in (370, 730):
        d.polygon([(ax, 350), (ax + 30, 360), (ax, 370)], fill="white")
        d.line([(ax - 20, 360), (ax + 25, 360)], fill="white", width=4)

    text(d, (W // 2, 540), "ACID  -  time travel  -  schema evolution  -  SQL", 22, (220, 240, 230), anchor="mm")
    text(d, (W // 2, 590), "100% open source  -  0 dong cloud", 24, (255, 220, 130), bold=True, anchor="mm")
    text(d, (W // 2, 635), "data-engineering / minio-iceberg-lakehouse", 18, (200, 220, 220), anchor="mm")
    im.save(OUT / "04_minio_iceberg_trino.png")


# ---------- 5. CDC Debezium ----------
def img5():
    im = gradient((20, 25, 50), (60, 30, 100))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "CDC real-time  -  Postgres -> Lake", 44, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "Moi UPDATE bay sang lake trong < 1 giay", 24, (210, 200, 240), anchor="mm")

    # Pipeline boxes
    stops = [
        (80,  "Postgres",  "WAL / logical repl", (90, 150, 220)),
        (335, "Debezium",  "CDC connector",      (220, 100, 90)),
        (590, "Kafka",     "Event stream",       (60, 60, 60)),
        (845, "Lake",      "Iceberg sink",       (100, 200, 160)),
    ]
    for x, name, sub, color in stops:
        card(d, x, 250, 220, 180, color)
        text(d, (x + 110, 320), name, 30, "white", bold=True, anchor="mm")
        text(d, (x + 110, 365), sub, 16, (240, 240, 240), anchor="mm")

    # arrows with glow
    for ax in (305, 560, 815):
        d.line([(ax - 5, 340), (ax + 25, 340)], fill=(140, 255, 200), width=5)
        d.polygon([(ax + 30, 330), (ax + 45, 340), (ax + 30, 350)], fill=(140, 255, 200))

    text(d, (W // 2, 500), "exactly-once  -  schema registry  -  sub-second latency", 22, (220, 230, 250), anchor="mm")
    text(d, (W // 2, 555), "logical replication  ->  event stream  ->  data lake", 20, (180, 200, 230), anchor="mm")
    text(d, (W // 2, 635), "data-engineering / cdc-debezium-postgres-kafka", 18, (200, 200, 230), anchor="mm")
    im.save(OUT / "05_cdc_debezium.png")


# ---------- 6. Partitioning ----------
def img6():
    im = gradient((40, 10, 30), (90, 30, 60))
    d = ImageDraw.Draw(im)
    text(d, (W // 2, 60), "Partition sai  =  pipeline cham 50x", 44, "white", bold=True, anchor="mm")
    text(d, (W // 2, 110), "1 dong PARTITION BY  -  2 tuan debug", 24, (240, 200, 210), anchor="mm")

    # Left: bad pipeline (red)
    card(d, 80, 200, 480, 340, (180, 50, 60))
    text(d, (320, 245), "BAD", 38, "white", bold=True, anchor="mm")
    # many small chaotic squares
    import random
    random.seed(7)
    for _ in range(60):
        rx = random.randint(110, 530)
        ry = random.randint(290, 510)
        sz = random.randint(10, 28)
        d.rectangle([rx, ry, rx + sz, ry + sz], fill=(255, 200, 200))
    text(d, (320, 480), "million tiny files", 18, "white", anchor="mm")
    text(d, (320, 515), "no pruning  -  skew", 16, (255, 220, 220), anchor="mm")

    # Right: good pipeline (green)
    card(d, 640, 200, 480, 340, (40, 130, 90))
    text(d, (880, 245), "GOOD", 38, "white", bold=True, anchor="mm")
    # neat aligned blocks
    for r in range(4):
        for c in range(6):
            x = 690 + c * 65
            y = 300 + r * 50
            d.rectangle([x, y, x + 50, y + 35], fill=(180, 240, 200))
    text(d, (880, 515), "right-sized partitions", 18, "white", anchor="mm")

    text(d, (W // 2, 590), "date  +  bucket  -  partition pruning  -  no skew", 22, (230, 230, 230), anchor="mm")
    text(d, (W // 2, 635), "data-engineering / partitioning-strategy-advisor", 18, (210, 200, 220), anchor="mm")
    im.save(OUT / "06_partitioning.png")


if __name__ == "__main__":
    img1(); img2(); img3(); img4(); img5(); img6()
    print("done")
