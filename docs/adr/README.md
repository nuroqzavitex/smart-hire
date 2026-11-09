# Architecture Decision Records

Mỗi ADR ghi một quyết định kiến trúc theo khuôn: **Context / Decision / Consequences / Alternatives considered**.

**Quy ước đánh số:** ADR được đánh số theo thứ tự được viết, không theo thứ tự chủ đề. `spec.md` §10 chỉ liệt kê các chủ đề dự kiến mà chưa gán số; số được cấp khi milestone tương ứng viết ADR đó.

## Mục lục

| # | Tiêu đề | Milestone |
|---|---|---|
| [0001](0001-monorepo.md) | Monorepo + Go workspace | M0 |
| [0002](0002-gateway-hybrid.md) | Gateway hybrid chi + grpc-gateway | M0 |
| [0003](0003-jwt-verify-at-service.md) | Verify JWT ở cả Gateway và service | M0 |
| [0004](0004-refresh-token-rotation.md) | Refresh token rotation + phát hiện dùng lại | M0 |

## Chủ đề dự kiến (chưa có số)

Các chủ đề trong `spec.md` §10, nhận số khi milestone tương ứng viết chúng:

- `grpc-for-sync`
- `saga-orchestration`
- `outbox-pattern`
- `cqrs-job-search`
- `account-deletion-choreography`
- `when-not-to-use-saga`
