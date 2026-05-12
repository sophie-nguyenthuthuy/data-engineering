# Bài nộp Lab 03 — DATA MODELING FOR ANALYTICS

## 1. Yêu cầu 1 & 2: Ảnh chụp kết quả khởi tạo và truy vấn bảng JOIN
Quá trình khởi tạo mô hình **Star Schema** (`dim_customers`, `dim_products`, `dim_date`, `fact_orders`) và seed data đã thành công. Từ đó, em đã query Join kết hợp dữ liệu từ 4 bảng để có cái nhìn tổng quát từng đơn hàng:

```text
 order_id | full_date  |  full_name   | product_name | quantity | gross_amount 
----------+------------+--------------+--------------+----------+--------------
        1 | 2026-03-01 | Alice Nguyen | Notebook     |        2 |        31.00
        2 | 2026-03-01 | Bao Tran     | Pen Set      |        1 |        20.00
        3 | 2026-03-02 | Alice Nguyen | Desk Lamp    |        3 |        36.00
        4 | 2026-03-03 | Chi Le       | Notebook     |        5 |        77.50
(4 rows)
```

## 2. Yêu cầu 3: Kết quả mô phỏng các kiểu Slowly Changing Dimensions (SCD)
Kịch bản thay đổi thành phố:
- Khách hàng 101 (`Alice Nguyen`): Dùng **SCD Type 1**, trực tiếp Update đè City từ Hanoi thành Haiphong.
- Khách hàng 102 (`Bao Tran`): Dùng **SCD Type 2**, kết thúc vòng đời bản ghi `Danang` ở mốc `2026-03-31` và sinh ra một Record mới (surrogate key id 4) trỏ về `Hue` báo hiệu current_flag là `true`. Cách này không làm gãy các aggregate báo cáo doanh số trong quý 1 cũ.
- Khách hàng 103 (`Chi Le`): Dùng **SCD Type 6**, là sự kết hợp của (Type 1, Type 2, Type 3) bằng cách giữ cả history tracking lịch sử (insert Record dòng key 5), bên cạnh đó overwrite luôn Current Value vào dòng bản ghi cũ (key 3) thành `Vung Tau` để tiện join cho báo cáo tổng kết điểm tiếp xúc hiện tại. 

Bảng Dimension Customer sau quá trình SCD:
```text
 customer_key | customer_id |  full_name   |   city   | effective_date |  end_date  | current_flag | previous_city | current_city 
--------------+-------------+--------------+----------+----------------+------------+--------------+---------------+--------------
            1 |         101 | Alice Nguyen | Haiphong | 2026-01-01     |            | t            |               | Haiphong
            2 |         102 | Bao Tran     | Danang   | 2026-01-01     | 2026-03-31 | f            |               | Danang
            4 |         102 | Bao Tran     | Hue      | 2026-04-01     |            | t            | Danang        | Hue
            3 |         103 | Chi Le       | HCMC     | 2026-01-01     | 2026-06-30 | f            |               | Vung Tau
            5 |         103 | Chi Le       | Vung Tau | 2026-07-01     |            | t            | HCMC          | Vung Tau
(5 rows)
```

## 3. Yêu cầu 4: Data Mart phục vụ doanh thu
Dựa vào star schema có sẵn, bảng tổng hợp phân cấp dữ liệu ở business line (`mart_daily_category_sales`) đã được thiết kế thành công để cung cấp thông tin aggregate hàng ngày cho công cụ Business Intelligence (Metabase, Tableau):

```text
 full_date  |  category   | total_qty | total_revenue 
------------+-------------+-----------+---------------
 2026-03-01 | Stationery  |         3 |         51.00
 2026-03-02 | Home Office |         3 |         36.00
 2026-03-03 | Stationery  |         5 |         77.50
(3 rows)
```

## 4. Yêu cầu lý thuyết đóng nắp kiến thức
**Câu hỏi:** "Bạn sẽ chọn star schema hay snowflake cho dashboard doanh thu đầu tiên, và vì sao?"

**Trả lời:** Mặc định khi xây dựng Data Mart hay BI Dashboard phục vụ doanh thu chốt đầu tiên cho doanh nghiệp, em sẽ chọn **Star Schema**. Lý do là:
1. **Dễ hiểu, thân thiện con người**: Star Schema chỉ gồm duy nhất một bảng `Fact_Orders` làm trung tâm, bao quanh bởi các bảng phẳng Dimension. Điều này dễ đào tạo cho các bạn Data Analyst, Business User tự sử dụng SQL/UI để kéo thả biểu đồ trên Superset/Tableau.
2. **Hiệu năng BI tối ưu**: Do dữ liệu đã được denormalized (thêm nhiều tính dư thừa, không chia nhỏ rẽ nhánh liên kết bảng mẹ con nhiều như Snowflake) dẫn tới BI tool tiêu thụ số lượng phép JOIN ít nhất có thể, qua đó trả về kết quả truy xuất báo cáo (Select queries) với latency rất thấp. 

*(File mã lệnh `lab03_setup.sql` đã được đính kèm vào repository phục vụ lưu lại logic SCD/Mart query)*
