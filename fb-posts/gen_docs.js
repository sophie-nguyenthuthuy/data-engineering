const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, ImageRun, HeadingLevel,
  ExternalHyperlink, AlignmentType,
} = require("docx");

const OUT_DIR = __dirname;
const IMG_DIR = path.join(__dirname, "images");
const REPO = "https://github.com/sophie-nguyenthuthuy/data-engineering/tree/main";

const posts = [
  {
    file: "01_parquet_orc_avro.docx",
    img: "01_parquet_orc_avro.png",
    title: "Parquet vs ORC vs Avro — định dạng nào nhanh nhất 2026?",
    category: "DW (Data Warehouse / Storage Format)",
    audience: "Data engineer, analytics engineer",
    hook: "Mình lấy 10GB dữ liệu giao dịch thật, ghi ra ba định dạng rồi đo từng giây. Kết quả khiến mình phải đổi cả pipeline đang chạy.",
    body: [
      "Trong thế giới data engineering hiện đại, việc chọn đúng định dạng lưu trữ ảnh hưởng trực tiếp tới chi phí storage và tốc độ truy vấn. Cùng một dataset, format khác nhau có thể chênh nhau hai tới ba lần dung lượng và hàng chục lần thời gian đọc. Đó là lý do mình quyết định benchmark thực tế ba ông lớn: Parquet, ORC và Avro.",
      "Bài test dùng 10GB dữ liệu giao dịch thật, ghi ra cả ba format rồi đo bốn chỉ số: tốc độ đọc full scan, tốc độ filter có predicate pushdown, tỉ lệ nén so với CSV gốc, và thời gian append một batch mới. Toàn bộ chạy trong docker-compose để ai cũng có thể reproduce lại được.",
      "Kết quả cho thấy Parquet thắng tuyệt đối ở các workload analytics nặng nhờ kiến trúc columnar và pushdown rất mạnh. ORC nhỉnh hơn một chút khi stack đang gắn chặt với Hive cũ. Avro lại tỏa sáng ở các pipeline streaming, nơi schema thay đổi liên tục và cần đọc tuần tự thay vì filter theo cột.",
      "Bài học rút ra: không có format nào tốt nhất — chỉ có format phù hợp nhất với workload của bạn. Cần analytics nặng thì chọn Parquet. Stack Hive truyền thống thì giữ ORC. Streaming hoặc schema evolution liên tục thì Avro là lựa chọn an toàn nhất.",
    ],
    outline: "Mở bài bằng câu chuyện chi phí storage và tốc độ query. Tiếp theo phân tích kiến trúc ba format (row-based, columnar, hybrid). Sau đó dựng benchmark với 10GB dataset trong docker-compose, đo bốn chỉ số chính, trình bày kết quả dưới dạng bảng số rõ ràng. Cuối cùng đưa ra ma trận quyết định khi nào chọn cái nào, kèm checklist năm câu hỏi để người đọc tự đối chiếu workload của mình.",
    cta: "Code và benchmark đầy đủ chạy được bằng docker-compose:",
    link: `${REPO}/parquet-vs-orc-vs-avro-lab`,
    tags: "#dataengineering #parquet #bigdata #orc #avro",
  },
  {
    file: "02_delta_iceberg_hudi.docx",
    img: "02_delta_iceberg_hudi.png",
    title: "Delta vs Iceberg vs Hudi — chọn table format nào cho Lakehouse 2026?",
    category: "DW (Lakehouse / Table Format)",
    audience: "Data engineer, data architect",
    hook: "Ba ông lớn lakehouse, mỗi ông một triết lý — mình thử hết cả ba trên cùng dataset 50 triệu dòng và đây là lựa chọn cuối cùng.",
    body: [
      "Lakehouse đã trở thành kiến trúc mặc định cho data platform hiện đại, nhưng câu hỏi khó nhất vẫn là chọn table format nào. Delta, Iceberg và Hudi đều giải bài toán ACID trên object storage, nhưng triết lý và điểm mạnh lại khác nhau rất rõ. Mình dành hai tuần test cả ba trên cùng một workload để có cái nhìn khách quan.",
      "Delta Lake gắn chặt với Spark và Databricks, mang lại trải nghiệm MERGE rất mượt và tài liệu cực kỳ chỉn chu. Iceberg đi theo hướng chuẩn mở, hỗ trợ đa engine từ Trino, Spark, Flink tới DuckDB, phù hợp với những team không muốn vendor lock-in. Hudi tỏa sáng nhất trong các workload CDC và upsert liên tục, với indexing và record-level update tối ưu hơn hai đối thủ còn lại.",
      "Mình test ba kịch bản: time travel quay về snapshot cũ, schema evolution thêm và đổi tên cột, và MERGE một triệu dòng vào bảng 50 triệu. Cả ba đều xử lý được, nhưng Iceberg có schema evolution sạch sẽ nhất, Delta và Hudi nhỉnh hơn về tốc độ MERGE thực tế.",
      "Quyết định cuối cùng phụ thuộc nhiều vào stack hiện tại của bạn hơn là chỉ số benchmark. Team đã quen Databricks thì Delta là con đường ngắn nhất. Team muốn linh hoạt nhiều engine thì Iceberg gần như là mặc định mới. Team chạy CDC nặng, cần upsert hàng giờ thì Hudi xứng đáng cân nhắc nghiêm túc.",
    ],
    outline: "Bắt đầu bằng bối cảnh vì sao lakehouse và table format trở nên quan trọng năm 2026. Phân tích kiến trúc manifest, snapshot, metadata layout của từng format. Tiếp theo dựng test với 50 triệu dòng, ba kịch bản: time travel, schema evolution, MERGE một triệu dòng. Trình bày kết quả so sánh dưới dạng bảng. Phần ecosystem nói về engine support và mức độ vendor lock-in. Kết luận bằng ma trận quyết định gắn với stack hiện tại của người đọc.",
    cta: "Code và so sánh chi tiết:",
    link: `${REPO}/delta-vs-iceberg-vs-hudi`,
    tags: "#lakehouse #iceberg #deltalake #hudi #dataengineering",
  },
  {
    file: "03_postgres_clickhouse.docx",
    img: "03_postgres_clickhouse.png",
    title: "Postgres vs ClickHouse trên 100 triệu dòng — kết quả gây sốc",
    category: "DW (OLTP vs OLAP) + Case Study",
    audience: "Backend dev, data engineer, founder hoặc CTO",
    hook: "Cùng một câu query analytics, Postgres chạy mất 47 giây, ClickHouse chỉ 0.3 giây. Hơn 150 lần. Lý do nằm ở kiến trúc, không phải ở phần cứng.",
    body: [
      "Câu chuyện bắt đầu từ một dashboard analytics đơn giản: tổng hợp doanh thu theo ngày trên một triệu transaction mỗi ngày, dữ liệu tích lũy đã chạm 100 triệu dòng. Postgres mất 47 giây cho mỗi lần load dashboard, user phàn nàn liên tục. Mình thử ClickHouse trên cùng dataset, cùng câu query, và kết quả là 0.3 giây. Khác biệt không phải do bug hay thiếu index — mà do kiến trúc nền tảng.",
      "Postgres là database row-store tối ưu cho OLTP, mỗi dòng nằm liền kề trên đĩa nên ghi đọc theo bản ghi rất nhanh. Nhưng khi query chỉ cần ba trên hai mươi cột, Postgres vẫn phải đọc cả dòng. ClickHouse thì ngược lại: lưu theo cột, chỉ đọc đúng cột cần dùng, cộng thêm vectorized execution xử lý hàng nghìn giá trị mỗi lần thay vì một-một, và sparse primary index giúp skip block không liên quan rất nhanh.",
      "Mình chạy năm câu query mẫu phổ biến trong dashboard: aggregation theo ngày, group by user, top N sản phẩm, percentile latency, và time-series rollup. Postgres trung bình chậm hơn 80 tới 200 lần tùy query. EXPLAIN cho thấy Postgres scan toàn bảng, còn ClickHouse chỉ chạm vào vài phần trăm dữ liệu nhờ skip index.",
      "Bài này không cổ vũ bạn bỏ Postgres. Postgres vẫn là OLTP tốt nhất hành tinh, vẫn nên là source of truth cho app. Vấn đề là ranh giới: khi nào cần thêm một OLAP engine bên cạnh. Pattern phổ biến và an toàn là dùng Postgres cho app, replicate sang ClickHouse cho dashboard và analytics. Đó là kiến trúc mình triển khai cho production, và dashboard 47 giây giờ chạy dưới 1 giây.",
    ],
    outline: "Mở bài bằng câu chuyện thật về dashboard 47 giây và user bỏ đi. Tiếp theo phân tích vì sao Postgres chậm khi làm analytics — overhead của row-store. Phần lý thuyết về ClickHouse: MergeTree, columnar storage, vectorized execution, sparse index. Tiếp theo benchmark cụ thể với 100 triệu dòng, năm câu query mẫu, kèm EXPLAIN cả hai bên để giải thích chênh lệch. Phần kiến trúc hybrid mô tả pattern Postgres cho app, ClickHouse cho dashboard. Kết luận bằng hướng dẫn khi nào nên migrate và khi nào nên ở lại với Postgres.",
    cta: "Toàn bộ benchmark và dataset generator:",
    link: `${REPO}/postgres-vs-clickhouse-benchmark`,
    tags: "#clickhouse #postgres #olap #dataengineering #benchmark",
  },
  {
    file: "04_minio_iceberg_trino.docx",
    img: "04_minio_iceberg_trino.png",
    title: "Dựng Lakehouse tại nhà — 0 đồng Databricks, 0 đồng Snowflake",
    category: "Project (Tutorial / Build từ A-Z)",
    audience: "Data engineer mới, sinh viên, người tự học",
    hook: "Không cần cloud đắt đỏ, không cần Databricks, không cần Snowflake. Toàn bộ stack lakehouse hiện đại chạy trên laptop của bạn trong một buổi tối.",
    body: [
      "Trước khi đụng tới cloud, mỗi data engineer nên build lakehouse local ít nhất một lần. Lý do đơn giản: bạn cần hiểu rõ từng tầng — storage, metastore, table format, query engine — thay vì gọi API rồi đoán. Cloud giấu hết các tầng đó, còn local thì lộ ra tất cả, debug dễ hơn nhiều, và quan trọng nhất là miễn phí.",
      "Stack mình build gồm bốn thành phần. MinIO đóng vai object storage tương thích S3 API, cho phép viết code y hệt như production. Hive Metastore lưu schema và pointer tới data, là cầu nối giữa Iceberg và các engine. Apache Iceberg là table format mở, mang ACID transaction xuống object storage. Trino là query engine, đọc Iceberg bằng SQL chuẩn. Tất cả gói gọn trong một docker-compose chạy bằng make up là xong.",
      "Sau khi stack chạy, bài tutorial dẫn bạn qua các bước thực tế: tạo bucket trong MinIO, tạo Iceberg catalog trong Trino, tạo namespace và bảng đầu tiên, load data bằng Spark, query bằng Trino, demo time travel quay về snapshot trước đó, và schema evolution thêm cột mới mà không cần rewrite bảng.",
      "Khi bạn chạy thành thạo lakehouse local, việc lên cloud trở thành chuyện đổi tên service. MinIO thay bằng S3, Hive Metastore thay bằng AWS Glue, Trino thay bằng Athena hoặc tự host. Logic code và cấu trúc data vẫn giữ nguyên. Đó chính là sức mạnh của open standard — học một lần, dùng mọi nơi.",
    ],
    outline: "Mở bài giải thích vì sao nên build lakehouse local trước khi đụng cloud. Phần một phân tích bốn thành phần trong docker-compose: MinIO, Hive Metastore, Iceberg, Trino. Phần hai hướng dẫn từng bước tạo bucket, catalog, namespace, bảng đầu tiên. Phần ba demo load data bằng Spark và query bằng Trino. Phần bốn demo các tính năng ACID, time travel, schema evolution. Kết luận bằng roadmap upgrade lên cloud, ánh xạ từng service local sang AWS hoặc GCP tương đương.",
    cta: "Repo và compose file:",
    link: `${REPO}/minio-iceberg-lakehouse`,
    tags: "#lakehouse #minio #iceberg #trino #opensource",
  },
  {
    file: "05_cdc_debezium.docx",
    img: "05_cdc_debezium.png",
    title: "CDC real-time: mỗi UPDATE ở Postgres bay sang Data Lake trong dưới 1 giây",
    category: "ETL (Streaming Ingestion / CDC)",
    audience: "Data engineer, backend dev làm event-driven",
    hook: "Pattern mọi công ty data-driven cần khi muốn rời ETL batch mỗi đêm và tiến tới analytics gần thời gian thực.",
    body: [
      "ETL batch mỗi đêm đang dần trở thành di sản. Khi business yêu cầu dashboard cập nhật theo phút, hoặc khi machine learning model cần feature gần real-time, batch không còn đủ. Change Data Capture, viết tắt CDC, là lời giải: thay vì query đầy đủ mỗi đêm, ta lắng nghe từng thay đổi tại nguồn và đẩy đi ngay khi xảy ra.",
      "Có nhiều cách làm CDC, nhưng log-based là phương án production-grade duy nhất. Thay vì query bảng hay dùng trigger, Debezium đọc trực tiếp Write-Ahead Log của Postgres thông qua logical replication. Mỗi INSERT, UPDATE, DELETE biến thành một event Kafka kèm đầy đủ trạng thái trước và sau. Không có miss, không có duplicate, không có pressure lên DB nguồn.",
      "Bài hướng dẫn dẫn bạn qua toàn bộ chain. Cấu hình Postgres bật wal_level logical, tạo publication và replication slot. Deploy Debezium connector với JSON config từng field một. Cấu hình schema registry để xử lý schema evolution khi bảng nguồn đổi cấu trúc. Cuối cùng dựng sink connector đẩy event xuống Iceberg, đảm bảo exactly-once để không bị nhân đôi event khi connector restart.",
      "Phần quan trọng nhất, thường bị bỏ qua, là vận hành. Khi connector chết giữa chừng, làm sao biết nó dừng ở đâu để resume. Khi schema bảng nguồn đổi, làm sao downstream tự thích nghi. Khi DB nguồn failover sang replica, làm sao replication slot không bị mất. Bài đưa ra checklist production-ready để bạn không phải học từ sự cố.",
    ],
    outline: "Mở bài giải thích vì sao batch ETL mỗi đêm không còn đủ với business hiện đại. Phần lý thuyết so sánh ba kiểu CDC: log-based, trigger-based, polling. Phần thực hành cấu hình Postgres với wal_level logical, replication slot, publication. Tiếp theo deploy Debezium connector, đi qua từng field trong config JSON. Phần schema registry xử lý schema evolution và layout topic Kafka. Phần sink xuống Iceberg, đảm bảo exactly-once. Cuối cùng là phần vận hành thực tế, khi connector chết, khi schema đổi, khi DB failover, kết bằng checklist production-ready.",
    cta: "Code và ví dụ chạy được:",
    link: `${REPO}/cdc-debezium-postgres-kafka`,
    tags: "#cdc #debezium #kafka #realtime #dataengineering",
  },
  {
    file: "06_partitioning.docx",
    img: "06_partitioning.png",
    title: "Pipeline chậm 50 lần — gốc rễ chỉ là một dòng PARTITION BY",
    category: "Project (Performance Tuning) + Case Study",
    audience: "Data engineer dùng Spark, Hive hoặc Iceberg",
    hook: "Mình mất hai tuần debug pipeline chậm bất thường. Hoá ra gốc rễ chỉ là một dòng PARTITION BY chọn sai cột.",
    body: [
      "Pipeline đang chạy ổn định 30 phút mỗi đêm. Một ngày đẹp trời, sau khi thêm một dimension mới vào bảng, runtime nhảy lên 12 tiếng. Không có code mới, không có schema change rõ ràng, không có data spike. Mình spend hai tuần đào logs, EXPLAIN từng query, rồi tìm ra thủ phạm: một dòng PARTITION BY trông vô hại đã phá tan partition pruning.",
      "Partitioning là kỹ thuật tăng tốc query bằng cách chia dữ liệu thành các thư mục nhỏ theo giá trị cột, để query có thể skip phần lớn dữ liệu không liên quan. Đây là một trong những quyết định kiến trúc quan trọng nhất khi thiết kế bảng lakehouse, nhưng cũng là nơi phổ biến nhất để mắc sai lầm khó debug.",
      "Có ba anti-pattern mình gặp đi gặp lại. Một là partition theo cột cardinality quá cao như user_id hay session_id, tạo ra hàng triệu file nhỏ, biến mọi query thành cơn ác mộng metadata. Hai là partition không khớp pattern query, ví dụ partition theo region trong khi 99% query filter theo date, làm pruning vô tác dụng. Ba là skew nặng, một vài partition gánh 80% dữ liệu, trong khi phần còn lại trống rỗng, biến parallel execution thành sequential.",
      "Mình build một advisor nhỏ để tránh mắc lại các bẫy này. Tool nhận vào schema bảng và query log thực tế, phân tích pattern truy vấn rồi đề xuất chiến lược partition tối ưu — có thể là date đơn giản, bucket theo hash, hoặc hybrid kết hợp cả hai. Trước khi viết PARTITION BY, hãy luôn trả lời bốn câu hỏi: query thường filter theo cột nào, cardinality bao nhiêu, distribution có skew không, và file size trung bình dự kiến bao nhiêu MB.",
    ],
    outline: "Mở bài bằng case study thật, pipeline 30 phút thành 12 tiếng sau một dòng code. Phần lý thuyết giải thích partition pruning hoạt động thế nào trong Spark và Iceberg. Phần ba anti-pattern phổ biến với ví dụ cụ thể: high cardinality, mismatch với query, skew nặng. Phần chiến lược đúng đi qua date partition, bucket partition và hybrid. Phần advisor tool mô tả input là schema kèm query log, output là gợi ý partition. Phần đo trước và sau cho thấy thay đổi về file size, runtime, scan size. Kết bằng checklist bốn câu hỏi cần trả lời trước khi viết PARTITION BY.",
    cta: "Repo và ví dụ chạy được:",
    link: `${REPO}/partitioning-strategy-advisor`,
    tags: "#dataengineering #spark #performance #partitioning",
  },
];

function meta(label, value) {
  return new Paragraph({
    spacing: { after: 80 },
    children: [
      new TextRun({ text: `${label}: `, bold: true }),
      new TextRun({ text: value }),
    ],
  });
}

function para(text, opts = {}) {
  return new Paragraph({
    spacing: { after: 200 },
    children: [new TextRun({ text, ...opts })],
  });
}

function buildDoc(p) {
  const imgBytes = fs.readFileSync(path.join(IMG_DIR, p.img));

  const children = [
    new Paragraph({
      heading: HeadingLevel.HEADING_1,
      children: [new TextRun({ text: p.title, bold: true })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new ImageRun({
        type: "png",
        data: imgBytes,
        transformation: { width: 580, height: 326 },
        altText: { title: p.title, description: p.title, name: p.img },
      })],
    }),
    new Paragraph({ children: [new TextRun("")] }),

    new Paragraph({
      heading: HeadingLevel.HEADING_2,
      children: [new TextRun({ text: "Thông tin bài", bold: true })],
    }),
    meta("Chủ đề chính", p.category),
    meta("Đối tượng đọc", p.audience),
    new Paragraph({ children: [new TextRun("")] }),

    new Paragraph({
      heading: HeadingLevel.HEADING_2,
      children: [new TextRun({ text: "Nội dung", bold: true })],
    }),
    ...p.body.map(t => para(t)),

    new Paragraph({
      heading: HeadingLevel.HEADING_2,
      children: [new TextRun({ text: "Repository", bold: true })],
    }),
    new Paragraph({
      spacing: { after: 120 },
      children: [new TextRun({ text: p.cta })],
    }),
    new Paragraph({
      spacing: { after: 200 },
      children: [new ExternalHyperlink({
        children: [new TextRun({ text: p.link, style: "Hyperlink", color: "1155CC", underline: {} })],
        link: p.link,
      })],
    }),
    new Paragraph({
      children: [new TextRun({ text: p.tags, color: "888888" })],
    }),
  ];

  return new Document({
    creator: "Sophie",
    title: p.title,
    styles: {
      default: { document: { run: { font: "Arial", size: 24 } } },
      paragraphStyles: [
        {
          id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 32, bold: true, font: "Arial", color: "1F2D5A" },
          paragraph: { spacing: { before: 240, after: 240 }, outlineLevel: 0 },
        },
        {
          id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 26, bold: true, font: "Arial", color: "1F2D5A" },
          paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 1 },
        },
      ],
    },
    sections: [{
      properties: {
        page: {
          size: { width: 12240, height: 15840 },
          margin: { top: 1080, right: 1080, bottom: 1080, left: 1080 },
        },
      },
      children,
    }],
  });
}

(async () => {
  for (const p of posts) {
    const doc = buildDoc(p);
    const buf = await Packer.toBuffer(doc);
    fs.writeFileSync(path.join(OUT_DIR, p.file), buf);
    console.log("wrote", p.file);
  }
})();
