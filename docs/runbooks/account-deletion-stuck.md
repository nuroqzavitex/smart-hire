# Runbook: AccountDeletionStuck

**Alert**: `max(account_deletions{status="STUCK"}) > 0`. Có ít nhất một yêu cầu xoá tài khoản đã quá 24 giờ mà chưa đủ 5 service xác nhận `user.data_purged` (ADR 0014).

**Tác động**: tài khoản vẫn ở `DELETION_PENDING`: không đăng nhập được, nhưng một phần dữ liệu cá nhân có thể vẫn còn ở service chưa xác nhận. Đây là nghĩa vụ pháp lý (NĐ 13/2023), nên xử lý trong ngày.

## 1. Tìm yêu cầu bị treo và service còn thiếu

```bash
docker compose -p smarthire exec postgres psql -U postgres -d auth_db -c \
  "SELECT id, user_id, requested_at, last_published_at, expected_services, purged_services
   FROM deletion_requests WHERE status = 'STUCK' ORDER BY requested_at;"
```

Service còn thiếu là phần chênh giữa `expected_services` và `purged_services`. Admin cũng xem được qua API: `GET /v1/admin/deletion-requests/{id}`.

## 2. Xem service đó vì sao không xác nhận

| Kiểm tra | Cách |
|---|---|
| Consumer có chạy không | `docker compose -p smarthire ps <service>`; log: `docker compose -p smarthire logs --since 1h <service> \| grep user.deleted` |
| Sự kiện có bị đẩy vào DLQ không | Redpanda Console `:8081`, topic `user.deleted.dlq`, header `dlq-error`; trên Grafana xem `dlq_messages_total{topic="user.deleted"}` |
| Consumer lag | Grafana: `kafka_consumer_lag{topic="user.deleted"}` |
| Lỗi phụ thuộc | Candidate cần MinIO để xoá file CV; AI cần Qdrant. Xem log tìm `delete cv file` hoặc lỗi Qdrant |
| Câu trả lời bị kẹt trong outbox | `outbox_pending{service_name="<service>"}`; `SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_type='user.data_purged'` trong DB của service đó |

## 3. Sửa nguyên nhân rồi để tracker tự hoàn tất

Tracker của Auth phát lại `user.deleted` (event_id mới) cho mọi yêu cầu chưa xong, cứ 15 phút một lần (`AUTH_DELETION_REPUBLISH_AFTER`), **kể cả** yêu cầu `STUCK`. Sau khi sửa service (khởi động lại, cấp lại quyền MinIO, v.v.), lần phát lại tiếp theo sẽ được xử lý. Khi xác nhận cuối cùng tới, Auth xoá cứng user và đặt `COMPLETED`. Alert tự hết.

Nếu không muốn chờ tới 15 phút, ép phát lại ngay:

```sql
-- trong auth_db: lần sweep kế tiếp (≤ 1 phút) sẽ phát lại
UPDATE deletion_requests SET last_published_at = now() - interval '1 day' WHERE id = '<id>';
```

Sự kiện nằm trong `user.deleted.dlq` **không cần** replay: lần phát lại mang event_id mới.

## 4. Khi một sự kiện là poison

Nếu log báo `poison event` cho `user.deleted` (payload sai schema), lỗi nằm ở phía Auth. Không sửa tay dữ liệu ở các service: sửa Auth, deploy lại, rồi ép phát lại như ở bước 3.

## Không được làm

- Không tự đặt `status='COMPLETED'` hay tự xoá user trong `auth_db` khi chưa đủ 5 xác nhận: làm vậy là mất bằng chứng đã xoá.
- Không xoá hàng `deletion_requests`: đó là bằng chứng.
