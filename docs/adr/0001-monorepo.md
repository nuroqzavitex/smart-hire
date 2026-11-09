# ADR 0001: Monorepo + Go workspace

- Trạng thái: Chấp nhận (M0)
- Ngày: 2026-11-08

## Context

SmartHire gồm nhiều service Go (Auth, Gateway, về sau Job, Candidate, Application, Notification) và một service Python. Chúng chia sẻ hợp đồng gRPC (proto), thư viện dùng chung (`config`, `logger`, `otel`, `apperr`, `grpcx`, `authz`, sau này `kafka`, `outbox`) và cùng một bộ tooling (buf, sqlc, lint, CI). Dự án do một người làm, và người đọc chủ yếu duyệt repo GitHub: clone một lần, chạy `make bootstrap`, thấy toàn bộ hệ thống.

Cần quyết định: một repo hay nhiều repo, và nếu một repo thì tổ chức module Go thế nào để các service không dính vào nhau.

## Decision

Dùng **một monorepo** với **Go workspace** (`go.work`) gồm các module độc lập, prefix `github.com/nuroqzavitex/smarthire`:

| Module | Vai trò |
|---|---|
| `pkg` | Thư viện dùng chung, mỗi package làm một việc, không phụ thuộc service nào |
| `gen/go` | Code sinh từ proto, **được commit**, không sửa tay |
| `services/auth`, `services/gateway` | Mỗi service một module; thêm service là thêm một module |
| `tools` | Công cụ dev (`keygen`, `openapimerge`, `smoke`) |

Quy tắc ràng buộc:

- Service **không import `internal/` của service khác**; chỉ chia sẻ qua `pkg/` và `gen/go/`. Quy tắc này được Go cưỡng chế (`internal/`) cho ranh giới giữa các module.
- Hợp đồng đi trước: `contracts/proto` (buf) sinh `gen/go`; CI chạy `make proto` rồi `git diff --exit-code` để chắc `gen/` luôn khớp proto. Buf deps khoá bằng `buf.lock` đã commit; nâng deps là hành động chủ động (`make proto-deps`).
- Mọi service dùng cùng cấu trúc hexagonal; `services/auth` là bản mẫu.
- Makefile viết cho POSIX shell để chạy giống nhau từ Git Bash trên Windows và Ubuntu trên CI. Lệnh `make test` lặp qua mọi module trong workspace.

## Consequences

Tích cực:

- Thay đổi proto, `pkg` và các service dùng nó nằm trong một commit/PR; không cần phát hành và nâng phiên bản thư viện nội bộ giữa các repo.
- Một lệnh dựng và kiểm thử toàn hệ thống; một CI cấu hình chung (path filter để không build thừa).
- Người xem repo thấy ngay kiến trúc tổng thể.

Đánh đổi:

- Không có quyền truy cập hay vòng đời release tách theo service; ở quy mô nhóm lớn sẽ cần CODEOWNERS và CI theo path.
- `go.work` chỉ phục vụ phát triển local. Docker build chạy với `GOWORK=off` và chỉ copy `pkg/`, `gen/go/` và service cần build, nên mỗi `go.mod` của service vẫn giữ `replace` trỏ tới `pkg` và `gen/go`; hai cơ chế phải được giữ đồng bộ.
- `gen/go` commit vào repo làm diff proto ồn hơn; đổi lại clone là build được, không cần cài buf.
- Mọi module dùng chung một phiên bản Go (directive `go` đồng nhất).

## Alternatives considered

- **Polyrepo (mỗi service một repo):** ranh giới rõ nhưng một người phải đồng bộ proto, `pkg` và CI qua nhiều repo; người xem phải clone nhiều nơi. Chi phí cao, lợi ích (phân quyền theo team) không có ở dự án này.
- **Một module Go duy nhất:** đơn giản nhất, nhưng mọi service chia sẻ dependency graph, và không có ranh giới `internal/` giữa các service; dễ import chéo vô tình. Khó chứng minh độc lập triển khai.
- **Chỉ dùng `replace` trong `go.mod`, không có `go.work`:** build được, nhưng lệnh lặp qua nhiều module, `gopls` và test liên module kém tiện hơn. Dự án dùng cả hai: `go.work` cho dev, `replace` cho Docker build (`GOWORK=off`).
