# CHANGE LOGS (Developer-facing)

---

## [2.0.0] — 2026-04-09

### Breaking Changes

- `docker-compose.yml` split into 4 module files — must use `docker-compose/scripts/dc.sh` (or `-f docker-compose/compose.core.yml -f docker-compose/compose.ops.yml -f docker-compose/compose.access.yml -f compose.apps.yml`) instead of plain `docker compose`
- Env var renames: `DOMAIN` replaces individual `SUBDOMAIN_*` vars; `STACK_NAME` replaces `COMPOSE_PROJECT_NAME`; `PROJECT_NAME` is new (required)
- `TAILSCALE_CLIENT_SECRET` → `TAILSCALE_AUTHKEY` (standardised Tailscale env naming)
- `APP_PORT` now drives the app container port directly; `SUBDOMAIN_APP`, `SUBDOMAIN_DOZZLE`, etc. removed

### Added

- **`docker-compose/scripts/dc.sh`** — main orchestrator: loads `.env`, reads `ENABLE_*` flags, builds `--profile` args, calls all 4 compose files in one command
- **`docker-compose/compose.core.yml`** — caddy + cloudflared, network + volumes definition; always-on
- **`docker-compose/compose.ops.yml`** — dozzle, filebrowser, webssh, webssh-windows; all profile-gated
- **`docker-compose/compose.access.yml`** — tailscale-linux, tailscale-windows; profile-gated
- **`compose.apps.yml`** — parameterised app service (`APP_IMAGE` + `APP_PORT`)
- **`docker-compose/scripts/up.sh` / `docker-compose/scripts/down.sh` / `docker-compose/scripts/logs.sh`** — one-liner shortcuts wrapping `dc.sh`
- **`docker-compose/scripts/validate-env.js`** — checks required vars, format validation (bcrypt, domain, port), subdomain preview
- **`docker-compose/scripts/validate-ts.js`** — Tailscale auth key format check + optional expiry lookup via TS API
- **`docker-compose/scripts/validate-compose.js`** — runs `docker compose config` across all 4 files to catch YAML errors
- **`npm run dockerapp-validate:all`** — combined validation pipeline (env → compose → TS)
- **`docs/DEPLOY.md`** — full deployment guide with mermaid flow diagrams, use cases, security checklist
- Subdomain auto-convention: all routes derived from `${PROJECT_NAME}.${DOMAIN}` pattern
- `DC_VERBOSE=1` debug flag for `docker-compose/scripts/dc.sh`
- `HEALTH_PATH` env to customise healthcheck endpoint per image

### Changed

- Image versions pinned (caddy `2.9.1-alpine`, cloudflared `2025.1.0`, dozzle `v8.x`, filebrowser `v2.30.0`, tailscale `stable`)
- Caddy `CADDY_INGRESS_NETWORKS` now uses `${STACK_NAME}_net` (was `app_net`)
- Network name: `${STACK_NAME:-mystack}_net` (dynamic, avoids conflicts between stacks)
- GitHub Actions and Azure Pipelines updated to call `docker-compose/scripts/dc.sh up` instead of bare `docker compose up`
- `detect-os.sh` no longer writes `COMPOSE_PROFILES` (profiles now fully managed by `docker-compose/scripts/dc.sh`)
- `.env.example` fully rewritten to match new schema

### Removed

- Monolithic `docker-compose.yml` (replaced by 4 module files)
- `SUBDOMAIN_APP`, `SUBDOMAIN_DOZZLE`, `SUBDOMAIN_FILEBROWSER`, `SUBDOMAIN_WEBSSH` env vars
- `TAILSCALE_CLIENT_SECRET` (use `TAILSCALE_AUTHKEY`)
- Hardcoded `build: ./services/app` in compose (now `APP_IMAGE` param)
- `scripts/generate-cf-config.js` and the generated-config workflow (maintain `cloudflared/config.yml` manually)

---

## [2.0.1] — 2026-04-16

### Added

- Admin UI chia theo tab: Tổng quan, Cron jobs, Accounts, Public Bucket Proxy, Tất cả file, Runtime, Logs
- Tab `Tất cả file` để xem toàn bộ file đã track theo account/scope, chọn file để replace hoặc delete trực tiếp
- API admin mới cho quản lý file track: list public files riêng, list all tracked files, replace file theo `encodedKey`, delete file theo `encodedKey`
- Báo cáo chạy cron chi tiết theo từng account khi bấm `run now`
- Toast thông báo thành công/thất bại và trạng thái loading cho các thao tác nặng trên form/admin actions
- Header sort cho bảng Accounts với mặc định sắp theo `addedAt` mới nhất

### Changed

- `admin/api/overview` không còn tải sẵn danh sách file public để tránh màn hình admin load chậm
- Tab `Public Bucket Proxy` chỉ tải danh sách khi bấm refresh, không fetch lúc load trang
- Lưu account thành công sẽ clear form account và phần `Thêm nhanh từ dịch vụ S3`
- Refresh dữ liệu theo từng tab mà không cần reload toàn trang

### Fixed

- Cải thiện UX cho thao tác cron, account, public file và managed file: có trạng thái đang xử lý, log rõ hơn và refresh cục bộ
- Đồng bộ metadata + usage về RTDB sau khi replace/delete tracked file từ admin

---
