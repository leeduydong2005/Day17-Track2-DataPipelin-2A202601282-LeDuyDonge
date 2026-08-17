# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Duy Đông  **Lớp:** E403  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>


  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 22.8s
  run 2/3 … 23.3s
  run 3/3 … 25.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (+ bài mở rộng A & B hoàn thành)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| Mục | Nội dung |
|---|---|
| **Triệu chứng** | Bấm Clear Task trên Airflow khiến số dòng của bảng `gold_training_set` tăng lên liên tục sau mỗi lần chạy lại; số lượng ticket bị trùng lặp nhiều lần. |
| **Nguyên nhân** | Model incremental trong dbt không khai báo `unique_key`, nên dbt mặc định sử dụng chiến lược `append` (sinh câu lệnh `INSERT` thuần). Khi chạy lại hoặc khi một ticket có nhiều bản ghi cập nhật (`op='u'`) ở các ngày khác nhau, dbt chèn thêm dòng mới thay vì ghi đè. Đồng thời DAG đặt `catchup=True` và không giới hạn `max_active_runs=1` làm tăng nguy cơ chạy dồn đồng thời. |
| **Cách khắc phục** | 1. Trong `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key='ticket_id'` và `incremental_strategy='delete+insert'`.<br>2. Trong `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: đúng 12,480 hàng · checksum 3 lượt: `8dd7c98653` (ổn định 100%). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| Mục | Nội dung |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` thiếu khoảng 5% (455 hàng) dữ liệu ở các ngày trong quá khứ so với đối chiếu thực tế. |
| **P99 độ trễ đo được** | **2.92 ngày** (Tối đa: 2.95 ngày, tỷ lệ đến muộn ~5%) |
| **Lookback đã chọn** | 3 ngày — vì P99 độ trễ là 2.92 ngày, lùi 3 ngày đảm bảo phủ được trên 99% các bản ghi bị trễ mà không tốn công tính toán lại quá nhiều ngày cũ. |
| **Nguyên nhân** | Khối `is_incremental()` sử dụng điều kiện `where event_date > (select max(event_date) from target)`. Khi có sự kiện xảy ra ở quá khứ nhưng đến muộn ở ngày hiện tại, điều kiện này loại bỏ hoàn toàn các sự kiện đó vì `event_date` của chúng nhỏ hơn `max(event_date)` đã có trong kho. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`: Thêm `unique_key=['event_date', 'customer_id']`, `incremental_strategy='delete+insert'` và nới cửa sổ `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. |
| **Bằng chứng** | trước: 8,645 hàng (thiếu 455 hàng) · sau: đúng đủ 9,100 hàng (14 ngày × 650 khách hàng), ỔN ĐỊNH ✓. |

**Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**
> Chọn **P99** thay vì `max` là sự đánh đổi cân bằng giữa **tính đầy đủ của dữ liệu** và **chi phí tài nguyên tính toán (I/O & compute)**. Nếu chọn theo `max` (ví dụ có bản ghi lỗi trễ 30 hay 90 ngày), mọi lượt chạy hằng ngày đều phải quét và tính lại 90 ngày dữ liệu cũ, làm tăng chi phí theo cấp số nhân và làm chậm pipeline. Dữ liệu trôi dạt quá P99 nên được xử lý bằng batch backfill định kỳ riêng thay vì gánh vào pipeline hằng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| Mục | Nội dung |
|---|---|
| **Triệu chứng** | Backend đổi kiểu dữ liệu `priority` từ số sang chuỗi, pipeline không báo lỗi nhưng model AI dự đoán kém do có hơn 6,606 bản ghi bị NULL/sai giá trị. |
| **Nguyên nhân** | Cột `priority` gặp hiện tượng Schema Evolution (chuyển sang nhãn chuỗi 'urgent'..'low') cùng với 312 bản ghi bị lỗi dữ liệu thật. `try_cast` cũ biến chuỗi hợp lệ thành NULL nhưng lại cho các số sai miền giá trị (0, 5, -1) đi qua. Model chưa bật Data Contract để ép kiểu và chưa phân luồng dữ liệu lỗi. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** ('1'..'4'): Ép kiểu nguyên, giữ nguyên.<br>2. **Nhãn chuỗi** ('urgent', 'high', 'medium', 'low'): Map về 1, 2, 3, 4 tương ứng.<br>3. **Dữ liệu lỗi** ('P1', '0', '5', '-1', '', null): Trả về `NULL` để đưa vào `quarantine_tickets`. |
| **Cách khắc phục** | 1. Sửa macro `normalize_priority.sql` bằng mệnh đề `CASE` phân loại 3 nhóm.<br>2. Sửa `silver_tickets.sql`: Lọc bỏ bản ghi lỗi (`is not null`) trước khi xếp hạng `row_number()`.<br>3. Sửa `quarantine_tickets.sql`: Tiếp nhận các bản ghi có macro trả về `NULL`.<br>4. Sửa `schema.yml`: Bật `contract: enforced: true`, thêm test `not_null` và `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets.priority` sạch 100% · `dbt test` 11/11 pass. |

**Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?**
> **Nên chặn và xử lý ở tầng Silver, không chặn ở Bronze:** Bronze đóng vai trò là "Data Lake / Single Source of Truth" lưu nguyên trạng dữ liệu thô. Nếu từ chối ở Bronze, ta sẽ mất dấu vết dữ liệu gốc và không thể audit hoặc replay khi cần điều tra sự cố.
> **Không để pipeline dừng khi gặp bản ghi lỗi:** Vì trong thực tế vận hành, vài trăm bản ghi lỗi không được phép làm gián đoạn hàng trăm nghìn bản ghi hợp lệ đang phục vụ người dùng theo thời gian thực. Việc cô lập bản ghi lỗi vào bảng `quarantine` giúp pipeline tiếp tục vận hành bình thường (graceful degradation) trong khi đội ngũ dữ liệu có thể xem xét và xử lý sau.

---

## 4 · Bài trong EXTRA.md (Bài A & Bài B)

| Mục | Nội dung |
|---|---|
| **Bài đã làm** | **Cả hai bài (Bài A & Bài B)** |
| **Nguyên nhân** | **Bài A:** Small-file problem (5.000 file parquet nhỏ) và vị từ không sargable (`strftime()`) khiến DuckDB quét 5.000.000 rows.<br>**Bài B:** Thứ tự `commit()` trước `write_batch()` dẫn tới mất dữ liệu (At-most-once) khi tiến trình bị kill ở giữa batch; câu lệnh `INSERT` thiếu xử lý xung đột. |
| **Cách khắc phục** | **Bài A:** Viết `tools/compact.py` partition theo `event_date`, sắp xếp theo `customer_name, event_time`, viết lại query dạng sargable `event_date = '2026-08-09'`.<br>**Bài B:** Đổi DDL thêm `primary key (event_id)`, sửa `write_batch()` dùng `ON CONFLICT (event_id) DO UPDATE`, đảo thứ tự ghi dữ liệu trước rồi mới commit offset (At-least-once + Idempotent). |
| **Bằng chứng** | **Bài A:** Rows scanned giảm 536.3× (từ 5,000,000 xuống 9,324), số file giảm từ 5,000 xuống 14 file, hash kết quả giữ nguyên.<br>**Bài B:** Chạy `.\run.ps1 crash-test` đạt thông báo `NHIỆM VỤ MỞ RỘNG B: ĐẠT`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra cấu hình `materialization` của model incremental: đã có `unique_key` và chiến lược upsert (`delete+insert`/`merge`) rõ ràng chưa, có bị sinh câu `INSERT` thuần gây trùng lặp khi chạy lại hay không. |
| **2** | Kiểm tra phân bố độ trễ (`_ingested_at - event_time`) và xem điều kiện lọc tăng dần có lookback window phù hợp (dựa trên P99) để hứng dữ liệu đến muộn hay không. |
| **3** | Kiểm tra Data Contract và Data Quality Tests (schema enforcement, test null/accepted values), đồng thời xác nhận có cơ chế Dead Letter Queue / Quarantine để cách ly dữ liệu lỗi thay vì làm sập cả hệ thống. |
