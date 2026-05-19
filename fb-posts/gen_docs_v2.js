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
    file: "07_tiered_storage.docx",
    img: "07_tiered_storage.png",
    title: "Cắt 70% chi phí S3 chỉ bằng tiered storage",
    category: "Cloud (Storage Cost Optimization)",
    audience: "Data engineer, DevOps, FinOps",
    body: [
      "Bill S3 của team mình đã vượt mức ngân sách ba tháng liên tiếp. Sau khi đào dữ liệu sử dụng, mình phát hiện 80% data nằm yên không ai chạm tới sau bảy ngày, nhưng vẫn đang trả tiền cho S3 Standard. Đây là tình huống điển hình mà ai vận hành lakehouse trên cloud cũng gặp, và lời giải nằm ở chiến lược tiered storage.",
      "AWS cung cấp bốn tier chính với giá chênh nhau hàng chục lần. S3 Standard ở mức 0.023 USD mỗi GB cho dữ liệu hot truy cập thường xuyên. S3 Infrequent Access giảm còn 0.0125 USD cho data warm chỉ đọc vài lần mỗi tháng. Glacier Instant Retrieval rẻ chỉ 0.004 USD cho cold data nhưng vẫn có thể đọc trong mili-giây. Deep Archive chạm đáy 0.00099 USD cho dữ liệu archive cần khôi phục trong nhiều giờ.",
      "Bài hướng dẫn dựng một orchestrator tự động phân loại data theo access pattern. Hệ thống đọc CloudTrail log, tính frequency truy cập mỗi object, rồi áp lifecycle rule tự động chuyển tier sau N ngày không touch. Quan trọng hơn, nó tránh các bẫy phổ biến như chuyển file nhỏ xuống Glacier khiến phí transition vượt cả tiền tiết kiệm được.",
      "Kết quả thực tế trên cluster sản xuất 500TB: chi phí giảm từ 11.500 USD xuống 3.400 USD mỗi tháng, tức là cắt được 70%. Không downtime, không thay đổi code app, không phá pipeline. Đây là kiểu tối ưu vừa dễ làm vừa cho ROI cao nhất khi vận hành data trên cloud.",
    ],
    outline: "Mở bài bằng bill S3 vượt ngân sách và phát hiện 80% data idle. Phân tích bốn tier của S3 kèm giá và use case. Hướng dẫn dựng orchestrator dùng CloudTrail log để phân loại access pattern và áp lifecycle rule. Cảnh báo các bẫy như phí transition cho file nhỏ. Trình bày kết quả thực tế giảm 70% chi phí trên 500TB.",
    cta: "Code + lifecycle template:",
    link: `${REPO}/tiered-storage-orchestrator`,
    tags: "#cloud #aws #s3 #finops #dataengineering",
  },
  {
    file: "08_serverless_autoscaler.docx",
    img: "08_serverless_autoscaler.png",
    title: "Serverless ETL — scale từ 0 lên 1000 worker rồi về 0",
    category: "Cloud (Serverless / Compute)",
    audience: "Data engineer, DevOps",
    body: [
      "Pipeline batch truyền thống chạy trên cluster cố định: 24/7 trả tiền cho 50 node dù chỉ 2 tiếng mỗi ngày thực sự bận. Serverless lật ngược mô hình: bạn không sở hữu server nào, chỉ trả theo execution time. Khi pipeline rảnh thì chi phí bằng 0, khi peak thì AWS hoặc GCP tự cấp hàng nghìn worker trong vài giây.",
      "Stack mình dùng dựa trên Lambda cho task ngắn dưới 15 phút, Step Functions để orchestrate workflow, ECS Fargate cho task dài hơn, và EMR Serverless cho Spark job. Mỗi loại có sweet spot riêng. Lambda rẻ và nhanh nhưng giới hạn memory và timeout. Fargate linh hoạt hơn nhưng cold start chậm hơn. EMR Serverless rất hợp Spark vì bỏ qua khâu quản lý cluster mà vẫn tận dụng được vectorized execution.",
      "Vấn đề khó nhất của serverless không phải compute mà là orchestration. Khi pipeline có hàng trăm task phụ thuộc nhau, mỗi task có thể fail bất kỳ lúc nào, retry, partial success, idempotency trở thành chuyện sống còn. Bài hướng dẫn cách thiết kế task idempotent, dùng Step Functions để retry exponential backoff, và cấu trúc DAG sao cho fail một task không kéo theo cả pipeline.",
      "So sánh chi phí trên workload thật của mình: cluster EMR cố định 50 node tốn khoảng 18.000 USD mỗi tháng. Cùng workload chạy serverless tốn 4.200 USD, tức là tiết kiệm 77%. Trade-off duy nhất là cold start vài giây ở batch đầu tiên, không vấn đề với batch job nhưng phải cân nhắc nếu cần latency dưới giây.",
    ],
    outline: "Mở bài so sánh cluster cố định và serverless về cost. Phân tích bốn loại serverless: Lambda, Step Functions, Fargate, EMR Serverless với sweet spot từng loại. Nhấn mạnh phần khó là orchestration: idempotency, retry, partial failure handling. Hướng dẫn dùng Step Functions thiết kế DAG resilient. So sánh chi phí thực tế giảm 77% và trade-off cold start.",
    cta: "Code orchestrator + IaC template:",
    link: `${REPO}/serverless-autoscaler`,
    tags: "#serverless #aws #lambda #cloud #dataengineering",
  },
  {
    file: "09_multi_region_mesh.docx",
    img: "09_multi_region_mesh.png",
    title: "Multi-region Data Mesh — dữ liệu ở 3 châu lục, query như ở một chỗ",
    category: "Cloud (Multi-region Architecture)",
    audience: "Data architect, senior data engineer",
    body: [
      "Khi công ty mở rộng ra nhiều khu vực, dữ liệu bắt đầu phân tán theo địa lý vì lý do compliance và latency. Đội Sales ở US, Finance ở EU, Operations ở Asia. Mỗi domain có DB và warehouse riêng, không ai muốn đồng bộ tất cả về một central warehouse vì chi phí egress và rủi ro vi phạm GDPR. Đây là tình huống Data Mesh được sinh ra để giải.",
      "Triết lý cốt lõi của mesh khác hẳn data lake truyền thống: data ownership thuộc về domain team, không phải đội data platform. Mỗi domain expose data của mình như một product, có schema rõ ràng, có SLA, có người chịu trách nhiệm. Đội data platform chỉ cung cấp hạ tầng chung và catalog liên kết các domain với nhau.",
      "Phần khó nhất khi triển khai multi-region là làm sao query một query duy nhất chạy được trên dữ liệu nằm ở ba cloud region khác nhau. Mình dùng kết hợp Iceberg cho table format mở, một federated catalog như Polaris hoặc Unity, và Trino làm query engine có khả năng pushdown filter xuống từng region. Khi user query revenue toàn cầu, Trino tự routing query tới từng region, mỗi region xử lý phần của nó, rồi gộp kết quả ở coordinator.",
      "Bài học vận hành quan trọng: không phải data nào cũng nên federate. Dữ liệu thường xuyên join cross-region cần được replicate có chọn lọc để tránh latency network. Schema contract giữa các domain phải được CI enforce, vì một thay đổi cột ở US có thể phá analytics ở EU mà không ai biết cho tới khi dashboard sập. Mesh giải bài toán scale tổ chức, không phải bài toán technical thuần túy.",
    ],
    outline: "Mở bài bằng bối cảnh đa quốc gia với compliance và latency. Giới thiệu triết lý Data Mesh: domain ownership, data as product. Phần kiến trúc kỹ thuật dùng Iceberg, federated catalog, Trino với query pushdown. Demo một query toàn cầu được routing xuống từng region. Phần vận hành: khi nào nên replicate thay vì federate, contract enforce qua CI, bài học scale tổ chức.",
    cta: "Reference architecture + code:",
    link: `${REPO}/multi-region-data-mesh`,
    tags: "#datamesh #cloud #multiregion #architecture #dataengineering",
  },
  {
    file: "10_lakehouse_migration.docx",
    img: "10_lakehouse_migration.png",
    title: "Migration 200TB Hive on-prem lên Iceberg trên cloud — zero downtime",
    category: "Cloud (Migration / Case Study)",
    audience: "Data engineer, data architect, CTO",
    body: [
      "Cluster Hadoop on-prem 200TB chạy được tám năm, ổn định nhưng đang chết dần. Phần cứng quá hạn, không scale được nữa, chi phí điện và bảo trì vượt 40.000 USD mỗi tháng. Team quyết định migrate lên cloud lakehouse với Iceberg trên S3, nhưng không chấp nhận downtime một phút nào vì pipeline đang phục vụ dashboard 24/7 cho khách hàng doanh nghiệp.",
      "Chiến lược migration của mình theo mô hình dual-write parallel run. Trong sáu tuần, mọi pipeline ghi đồng thời xuống Hive on-prem và Iceberg trên S3. Đội data engineer build script daily diff để so sánh row count và checksum giữa hai bên. Bất kỳ chênh lệch nào lớn hơn 0.01% đều phải điều tra tận gốc trước khi tiến tiếp.",
      "Phần khó nhất là migrate metadata. Hive Metastore lưu thông tin partition theo cách rất specific, trong khi Iceberg dùng manifest và snapshot hoàn toàn khác. Mình viết một tool đọc Hive Metastore export, parse partition list, rồi rebuild lại làm Iceberg snapshot tương ứng. Một số bảng có vài triệu partition phải migrate theo batch để không OOM coordinator.",
      "Cut-over diễn ra vào một sáng Chủ nhật. Sau khi xác nhận hai bên data khớp 100% trong ba tuần liên tiếp, mình switch DNS của query layer từ Hive on-prem sang Athena trên cloud trong vòng năm phút. Pipeline on-prem chạy ghi shadow thêm hai tuần nữa rồi mới shutdown. Toàn bộ migration zero downtime, không khách hàng nào nhận ra. Chi phí vận hành hàng tháng giảm từ 40.000 xuống 12.000 USD.",
    ],
    outline: "Bối cảnh cluster Hadoop 200TB chết dần, chi phí cao. Lựa chọn target là Iceberg trên S3. Chiến lược dual-write parallel run trong sáu tuần với daily diff. Phần kỹ thuật chuyển metadata từ Hive Metastore sang Iceberg manifest. Quy trình cut-over Chủ nhật, switch DNS năm phút. Kết quả zero downtime và giảm 70% chi phí.",
    cta: "Migration playbook + code:",
    link: `${REPO}/lakehouse-migration`,
    tags: "#migration #cloud #iceberg #lakehouse #dataengineering",
  },
  {
    file: "11_multi_tenant.docx",
    img: "11_multi_tenant.png",
    title: "Multi-tenant Data Platform — 1 cluster phục vụ 1000 khách hàng",
    category: "Cloud (Multi-tenant SaaS)",
    audience: "SaaS founder, platform engineer, data engineer",
    body: [
      "Khi xây SaaS có analytics cho khách hàng, câu hỏi kiến trúc đầu tiên là: mỗi customer một cluster riêng, hay tất cả share chung một platform. Mỗi cluster riêng cho isolation tuyệt đối nhưng chi phí và độ phức tạp vận hành tăng tuyến tính theo số khách hàng. Share chung cho cost efficient nhưng dễ bị noisy neighbor và rủi ro data leak. Bài này chọn cách thứ hai và xử lý các rủi ro đó từ thiết kế.",
      "Mô hình mình dùng là single platform, multi-tenant với ba lớp isolation. Lớp một là schema-per-tenant trong cùng warehouse, mỗi customer có namespace riêng và không bao giờ thấy được nhau qua catalog. Lớp hai là row-level security cho các bảng shared dimension, dựa trên session context inject từ application layer. Lớp ba là resource quota để chống một customer chiếm hết compute.",
      "Khó khăn lớn nhất không phải code mà là chi phí. Khi một khách hàng chạy query nặng, ai trả tiền? Mình build hệ thống meter query cost theo tenant, dựa trên CPU time, bytes scanned và memory peak. Cuối tháng customer nhận được breakdown chi tiết, và team product có thể quyết định include trong gói hay charge thêm cho enterprise tier.",
      "Bài học sau hai năm vận hành nền tảng này cho hơn 800 khách hàng: tự động hoá là chìa khoá. Provision tenant mới không thể là thao tác thủ công nếu bạn muốn scale tới hàng nghìn customer. Tự động phải bao gồm tạo schema, gán role, cấu hình quota, seed dashboard mẫu, và cleanup khi customer churn. Mọi thao tác thủ công ở quy mô lớn đều thành bottleneck và rủi ro bảo mật.",
    ],
    outline: "Bối cảnh xây SaaS analytics: cluster per tenant vs shared platform. Ba lớp isolation: schema-per-tenant, row-level security, resource quota. Vấn đề meter cost theo tenant với CPU time, bytes scanned, memory. Hệ thống billing breakdown theo tenant. Bài học automation toàn bộ vòng đời tenant: provision, quota, cleanup khi churn.",
    cta: "Reference platform + code:",
    link: `${REPO}/multi-tenant-platform`,
    tags: "#multitenant #saas #cloud #platform #dataengineering",
  },
  {
    file: "12_zero_downtime.docx",
    img: "12_zero_downtime.png",
    title: "Zero-downtime pipeline upgrade — đổi Spark từ 3.3 lên 4.0 không gián đoạn",
    category: "Cloud (DevOps / Reliability)",
    audience: "Senior data engineer, platform engineer",
    body: [
      "Mọi data engineer đều sợ upgrade engine. Đổi Spark từ 3.3 sang 4.0 nghe có vẻ đơn giản nhưng thực tế có hàng trăm thứ có thể vỡ: API deprecation, behavior thay đổi, performance regression ngầm, dependency hell. Truyền thống là thông báo downtime cuối tuần, làm xong, hi vọng tốt nhất. Bài này hướng dẫn cách upgrade pipeline production không có một phút downtime nào.",
      "Kỹ thuật cốt lõi là blue-green deployment cho data pipeline, lấy cảm hứng từ deploy web service. Phiên bản hiện tại v1 và phiên bản mới v2 cùng tồn tại trong production. Cả hai đọc từ cùng nguồn nhưng ghi xuống hai output namespace tách biệt. Traffic của downstream từ từ chuyển từ v1 sang v2 trong nhiều giờ hoặc nhiều ngày, có thể rollback bất kỳ lúc nào.",
      "Shadow run là bước quan trọng trước khi cut-over. v2 chạy song song nhưng output của nó chỉ dùng để so sánh, không ai consume. Mỗi giờ một job diff so sánh row count, column distribution, key metric của hai bên. Nếu chênh lệch vượt threshold thì alert team. Mình từng catch được một bug trong Spark 4.0 thay đổi cách handle NaN trong window function, mà chỉ shadow run mới phát hiện được.",
      "Khi diff ổn định nhiều ngày, cut-over thực hiện bằng cách switch alias hoặc view ở metastore. Downstream pipeline không cần đổi code, chỉ cần restart và tự động dùng output mới. Nếu phát hiện vấn đề sau cut-over, switch alias ngược lại trong vài giây để rollback. Đây là kỹ thuật vận hành rất gần với SRE practice cho web service, nhưng còn ít được áp dụng trong thế giới data.",
    ],
    outline: "Bối cảnh upgrade engine luôn rủi ro. Triết lý blue-green deployment cho pipeline. Kỹ thuật shadow run: v2 chạy song song, output diff với v1 mỗi giờ. Phần cut-over qua alias hoặc view ở metastore. Cơ chế rollback trong vài giây. Bài học áp dụng SRE practice vào data engineering.",
    cta: "Blue-green template + diff tooling:",
    link: `${REPO}/zero-downtime-pipeline-upgrades`,
    tags: "#cloud #sre #devops #spark #dataengineering",
  },
  {
    file: "13_reverse_etl.docx",
    img: "13_reverse_etl.png",
    title: "Reverse ETL — đẩy Customer 360 từ Data Warehouse vào Salesforce",
    category: "ETL (Reverse ETL / Operational Analytics)",
    audience: "Data engineer, analytics engineer, RevOps",
    body: [
      "ETL truyền thống đưa data từ application vào warehouse để phân tích. Reverse ETL làm điều ngược lại: đưa kết quả phân tích từ warehouse trả về các SaaS operational như Salesforce, HubSpot, Slack, Marketo. Lý do rất đơn giản: dashboard không tự kích hoạt hành động, mà sales rep và marketer phải thấy insight ngay trong tool họ đang dùng.",
      "Ví dụ điển hình là customer 360. Warehouse có đầy đủ thông tin: lifetime value, last login, support ticket count, churn probability từ ML model. Nhưng sales rep mở Salesforce mỗi sáng, không vào Looker. Reverse ETL đảm bảo Salesforce contact có cập nhật mỗi giờ với LTV và churn score, nên rep nhìn lead là biết ưu tiên ai trước.",
      "Kỹ thuật cần giải ba bài toán. Một là delta sync hiệu quả, không thể đẩy toàn bộ bảng triệu dòng mỗi giờ, phải biết hàng nào thay đổi từ lần sync trước. Hai là field mapping linh hoạt vì warehouse schema thường rộng hơn schema của SaaS đích, và mapping có thể đổi theo từng team. Ba là rate limit handling vì mọi SaaS đều cap số request mỗi phút, mình phải queue và retry thông minh.",
      "Bài hướng dẫn build pipeline từ Snowflake hoặc BigQuery sang Salesforce với change-data detection bằng cách hash tổ hợp các cột quan tâm. Mỗi tenant có thể cấu hình mapping qua YAML, không cần đổi code. Rate limit handler dùng exponential backoff cộng với token bucket. Toàn bộ chạy serverless trên Lambda hoặc Cloud Run, cost dưới 100 USD mỗi tháng cho khối lượng triệu record mỗi ngày.",
    ],
    outline: "Mở bài định nghĩa reverse ETL và lý do quan trọng. Use case customer 360 đẩy từ warehouse vào Salesforce. Ba bài toán kỹ thuật: delta sync, field mapping, rate limit. Hướng dẫn build pipeline từ Snowflake hoặc BigQuery sang Salesforce với change detection bằng hash. Phần config YAML cho mapping. Triển khai serverless với chi phí dưới 100 USD mỗi tháng.",
    cta: "Code + connector mẫu:",
    link: `${REPO}/reverse-etl`,
    tags: "#reverseetl #salesforce #cloud #operational #dataengineering",
  },
  {
    file: "14_medallion.docx",
    img: "14_medallion.png",
    title: "Medallion Architecture — Bronze, Silver, Gold giải thích cho người mới",
    category: "DW (Lakehouse Architecture)",
    audience: "Data engineer mới, analytics engineer",
    body: [
      "Mỗi khi nhắc tới lakehouse trên cloud, kiểu gì cũng đụng tới medallion architecture với ba lớp Bronze, Silver và Gold. Đây không phải buzzword marketing, mà là pattern thiết kế được kiểm chứng qua hàng nghìn triển khai thực tế. Hiểu đúng medallion giúp bạn tránh được hầu hết các vấn đề thường gặp khi lakehouse phình to.",
      "Bronze là lớp landing, nơi dữ liệu được ghi xuống ngay khi đến từ source, gần như nguyên trạng. Không transform, không clean, chỉ giữ bản gốc kèm metadata về thời gian ingest và source. Mục tiêu của Bronze là replay được mọi thứ phía sau khi cần, kể cả khi business logic thay đổi sáu tháng sau. Format thường là Delta hoặc Iceberg để có ACID và time travel.",
      "Silver là lớp clean và conformed. Tại đây diễn ra deduplication, type casting, join giữa các bảng raw để tạo entity rõ ràng như user, order, event. Schema của Silver được thiết kế ổn định và có contract với downstream. Đây cũng là nơi quality gate được áp dụng nghiêm túc, các row không pass test sẽ bị quarantine sang bảng error chứ không thầm lặng lọt vào downstream.",
      "Gold là lớp business và BI. Mỗi bảng Gold phục vụ một use case cụ thể như dashboard, ML feature, hay export API. Tại đây dữ liệu được aggregate, denormalize, tối ưu cho query pattern thực tế. Một sai lầm phổ biến của newbie là build Gold trực tiếp từ Bronze để tiết kiệm thời gian, dẫn tới duplicate logic và inconsistency. Đi qua Silver trông có vẻ thừa nhưng chính nó đảm bảo single source of truth cho mọi Gold table.",
    ],
    outline: "Mở bài giải thích vì sao medallion không phải buzzword. Phân tích Bronze là landing layer giữ nguyên trạng với ACID. Phân tích Silver là conformed layer với quality gate và quarantine bảng. Phân tích Gold là business layer cho từng use case cụ thể. Cảnh báo sai lầm phổ biến: build Gold trực tiếp từ Bronze gây inconsistency.",
    cta: "Reference implementation + dbt project:",
    link: `${REPO}/medallion-lakehouse`,
    tags: "#medallion #lakehouse #cloud #dataengineering #architecture",
  },
  {
    file: "15_catalog_lineage.docx",
    img: "15_catalog_lineage.png",
    title: "Data Catalog + Lineage — truy nguồn gốc cột 'revenue' trong 2 giây",
    category: "Project (Catalog / Governance)",
    audience: "Data engineer, analytics engineer, data governance",
    body: [
      "Một câu hỏi quen thuộc trong họp executive: cột revenue trong dashboard này lấy từ đâu, công thức tính thế nào, ai sở hữu nó. Nếu trả lời mất hai ngày đào code, đó là dấu hiệu warehouse của bạn thiếu data catalog và lineage. Với cloud lakehouse hiện đại, đây là phần hạ tầng bắt buộc, không phải lựa chọn.",
      "Catalog là danh bạ trung tâm của mọi data asset: bảng, view, dashboard, ML model. Mỗi asset có metadata về owner, schema, description, tag và SLA. Catalog tốt trả lời được câu hỏi 'bảng nào có thông tin về order' trong vài giây, thay vì phải hỏi đồng nghiệp hoặc đào Slack history.",
      "Lineage là biểu đồ thể hiện asset nào phụ thuộc vào asset nào, ở mức bảng và đặc biệt là mức cột. Khi sếp hỏi nguồn gốc cột revenue trong dashboard, lineage cho bạn thấy chuỗi từ revenue_daily ngược về user_facts, orders_clean, rồi tới orders_raw từ Kafka. Quan trọng hơn, khi muốn đổi schema bảng orders_raw, lineage chỉ ra mọi dashboard và pipeline có thể bị ảnh hưởng để bạn warn họ trước.",
      "Cách triển khai thực tế trên cloud thường dựa vào OpenLineage làm chuẩn mở. Mỗi job Spark, dbt, Airflow gửi event tới catalog backend, mô tả input và output. Backend như Marquez, DataHub hay Unity Catalog của Databricks lưu trữ, index và visualize. Khó khăn lớn nhất không phải tool mà là kỷ luật: mọi pipeline mới phải emit lineage event ngay từ ngày đầu, không bao giờ chấp nhận pipeline không có lineage trong production.",
    ],
    outline: "Câu chuyện họp executive hỏi nguồn gốc cột revenue. Định nghĩa catalog với metadata owner, schema, SLA, tag. Định nghĩa lineage ở mức bảng và cột, ứng dụng impact analysis khi đổi schema. Hướng dẫn triển khai với OpenLineage làm chuẩn, backend Marquez hoặc DataHub. Bài học vận hành: kỷ luật emit lineage event cho mọi pipeline mới.",
    cta: "Code + OpenLineage integration:",
    link: `${REPO}/data-catalog-lineage`,
    tags: "#catalog #lineage #governance #cloud #dataengineering",
  },
  {
    file: "16_data_contracts.docx",
    img: "16_data_contracts.png",
    title: "Data Contracts — producer và consumer ký 'hợp đồng' trước khi ship",
    category: "Project (Data Contracts / Governance)",
    audience: "Senior data engineer, platform engineer, backend dev",
    body: [
      "Bug data thường xuất phát từ một sự kiện đơn giản: backend team đổi schema, không ai báo, dashboard sập sau ba ngày, data team mới phát hiện. Data contracts là cách dập tắt loại bug này ngay từ gốc bằng cách bắt producer và consumer ký 'hợp đồng' chính thức về schema, ngữ nghĩa và SLA, được CI enforce.",
      "Contract điển hình mô tả ba phần. Schema phần kỹ thuật: tên cột, kiểu dữ liệu, nullable, default, constraint. Schema phần ngữ nghĩa: ý nghĩa business của mỗi field, đơn vị đo, format thời gian, các giá trị enum hợp lệ. SLA: tần suất publish, độ trễ tối đa, định nghĩa rõ thế nào là một event hợp lệ.",
      "Phần khó nhất là enforce contract. Producer team thường không muốn bị chậm bởi data team, còn data team thì sợ schema bị break ngầm. Giải pháp là tích hợp contract check vào CI của producer. Mỗi pull request đổi schema sẽ chạy diff so với contract đã ký, breaking change bị block tự động, non-breaking change được warn và yêu cầu version bump. Consumer chỉ depend vào major version, nên minor change không cần coordinate.",
      "Bài học sau một năm áp dụng cho hơn năm mươi event stream: số incident liên quan tới schema drift giảm từ trung bình hai tuần một lần xuống gần như bằng 0. Quan trọng hơn, văn hoá thay đổi: backend team chủ động hỏi data team trước khi đụng schema, không phải vì sợ block PR mà vì hiểu data là sản phẩm. Đó là chiến thắng lớn nhất, vượt xa mọi metric kỹ thuật.",
    ],
    outline: "Vấn đề schema drift gây bug data triền miên. Ba phần của contract: technical schema, semantic schema, SLA. Cơ chế enforce qua CI của producer với breaking vs non-breaking change. Versioning rule cho consumer. Bài học vận hành: incident schema drift giảm gần về 0 và culture shift trong team backend.",
    cta: "Contract template + CI integration:",
    link: `${REPO}/data-contract-platform`,
    tags: "#datacontracts #governance #cloud #platform #dataengineering",
  },
  {
    file: "17_column_encryption.docx",
    img: "17_column_encryption.png",
    title: "Column-level encryption — mã hoá PII trong cloud warehouse",
    category: "Cloud (Security / Compliance)",
    audience: "Data engineer, security engineer, compliance officer",
    body: [
      "GDPR ở châu Âu, PDPA ở Đông Nam Á, PCI DSS cho thẻ thanh toán. Mọi compliance regime đều yêu cầu PII và dữ liệu nhạy cảm phải được mã hoá khi at rest. Cloud provider mã hoá toàn bộ disk theo mặc định, nhưng đó là transparent encryption, người vào được warehouse vẫn đọc được mọi cột. Column-level encryption là lớp bảo vệ sâu hơn: ngay cả admin warehouse cũng không đọc được PII nếu thiếu key.",
      "Kỹ thuật cốt lõi dùng envelope encryption. Mỗi cột PII có một data encryption key DEK riêng, được sinh ra random và rotate định kỳ. DEK chính nó được mã hoá bởi master key KEK lưu trong KMS như AWS KMS, Google Cloud KMS hoặc HashiCorp Vault. Warehouse chỉ lưu DEK đã encrypt, không lưu plaintext. Khi pipeline cần ghi PII, gọi KMS để decrypt DEK, mã hoá data, rồi quên DEK ngay. Tốc độ vẫn nhanh nhờ cache DEK trong memory ngắn hạn.",
      "Bài toán khó là làm sao query vẫn dùng được. Equality search có thể giữ bằng deterministic encryption, cùng input thì cùng output, để index và filter vẫn hoạt động. Nhưng deterministic mất một phần security. Range query khó hơn, thường giải bằng order-preserving encryption hoặc bằng cách dùng tokenization riêng cho field cần range. Trade-off giữa usability và security phải được team security và data team thống nhất từ đầu.",
      "Audit là phần không thể thiếu. Mọi lần truy cập KMS để decrypt DEK đều được log, kèm danh tính user và lý do. Khi auditor hỏi 'ai đã đọc cột email trong tháng trước', câu trả lời nằm trong log KMS chứ không phải trong warehouse. Đây là kiến trúc đã được nhiều ngân hàng và fintech áp dụng để pass audit PCI DSS và SOC 2.",
    ],
    outline: "Bối cảnh compliance GDPR PCI DSS yêu cầu mã hoá PII. Phân biệt transparent encryption mặc định và column-level encryption sâu hơn. Kỹ thuật envelope encryption với DEK và KEK trong KMS. Trade-off giữa deterministic, order-preserving và usability của query. Phần audit qua log KMS để trả lời câu hỏi ai đã đọc cột nào.",
    cta: "Code + KMS integration:",
    link: `${REPO}/column-encryption-pipeline`,
    tags: "#security #encryption #cloud #compliance #dataengineering",
  },
  {
    file: "18_self_healing.docx",
    img: "18_self_healing.png",
    title: "Self-healing ETL — pipeline tự sửa lỗi, 3 giờ sáng không cần wake-up",
    category: "ETL (Reliability / Self-healing)",
    audience: "Data engineer, platform engineer, SRE",
    body: [
      "Mỗi data engineer đều có một đêm bị page lúc 3 giờ sáng vì pipeline fail. Lý do thường rất tầm thường: source DB timeout vài giây, S3 throttle, network blip, bảng tạm trùng tên. Sửa thủ công mất 15 phút, nhưng nỗi đau bị đánh thức và phải tỉnh táo debug ở 3 giờ sáng thì không tính được. Self-healing ETL là cách để pipeline tự xử lý các lỗi transient này mà không cần con người can thiệp.",
      "Self-healing không phải retry vô tri. Hệ thống cần đi qua bốn bước rõ ràng. Detect: nhận biết task fail và phân loại lỗi theo template biết trước. Diagnose: xác định lỗi này có thể tự sửa không, fall vào category nào, có cần escalate không. Recover: thực thi action sửa lỗi tương ứng, có thể là retry, backfill partition cụ thể, rebuild metadata, hoặc cô lập task lỗi. Verify: kiểm tra task sau khi recover đã thật sự success chưa, output đúng schema và row count không, mới gỡ alert.",
      "Phần Diagnose là trí tuệ thật sự của hệ thống. Mỗi loại lỗi có một fingerprint dựa trên exception class, message pattern, downstream impact. Hệ thống học từ lịch sử để biết lỗi loại A thường giải bằng retry, lỗi loại B cần backfill từ thời điểm T, lỗi loại C luôn cần human escalate. Theo thời gian, library các loại lỗi giải được tự động ngày càng nhiều, và tỉ lệ wake-up đêm giảm dần.",
      "Bài học vận hành ba năm áp dụng pattern này: tỉ lệ on-call alert ban đêm giảm từ trung bình hai lần mỗi tuần xuống một lần mỗi quý. Quan trọng hơn, team data engineer có thể tập trung vào build feature mới thay vì firefight. Trade-off duy nhất là phải đầu tư ban đầu để xây library lỗi và recovery action, nhưng ROI rất nhanh chỉ sau hai tháng vận hành.",
    ],
    outline: "Mở bài bằng nỗi đau page 3 giờ sáng do lỗi transient. Bốn bước của self-healing: detect, diagnose, recover, verify. Đào sâu phần diagnose với fingerprint lỗi và library recovery action. Cơ chế học từ lịch sử để mở rộng dần loại lỗi giải được tự động. Kết quả ba năm vận hành: alert đêm giảm từ hai lần một tuần xuống một lần một quý.",
    cta: "Code + recovery playbook:",
    link: `${REPO}/self-healing-etl`,
    tags: "#sre #reliability #etl #cloud #dataengineering",
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
