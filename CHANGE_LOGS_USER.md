# CHANGE LOGS (User-facing)

---

## [2.0.0] — 2026-04-09 — Modular Stack Template

### What's New

**Deploy any Docker image in minutes**
Change two lines in `.env` (`APP_IMAGE` and `APP_PORT`) and your app is live — no YAML editing required.

**Feature flags — enable what you need**
Turn ops tools on or off with simple env vars:

```env
ENABLE_DOZZLE=true
ENABLE_FILEBROWSER=true
ENABLE_WEBSSH=false
ENABLE_TAILSCALE=false
```

**Subdomains auto-generated**
Set `PROJECT_NAME=gitea` and `DOMAIN=example.com` once. All service URLs follow automatically:

- `gitea.example.com` → your app
- `logs.gitea.example.com` → log viewer
- `files.gitea.example.com` → file manager
- `ttyd.gitea.example.com` → web terminal

**One-command validation before deploy**

```bash
npm run dockerapp-validate:all
```

Checks env vars, Tailscale key format, and compose YAML — all at once.

**One-command deploy**

```bash
npm run dockerapp-exec:up
```

### What Changed (migration from v1)

If upgrading from the previous `docker-compose.yml` setup:

1. Replace `SUBDOMAIN_APP/DOZZLE/FILEBROWSER/WEBSSH` with just `PROJECT_NAME`
2. Replace `TAILSCALE_CLIENT_SECRET` with `TAILSCALE_AUTHKEY`
3. Replace `docker compose up` with `bash docker-compose/scripts/dc.sh up` or `npm run dockerapp-exec:up`
4. Update `cloudflared/config.yml` manually from `cloudflared/config.yml.example` if you use Cloudflare Tunnel

See `docs/DEPLOY.md` for the full migration guide.

---

## [2.0.1] — 2026-04-16 — Admin Tabs + File Manager

### Có gì mới

- Trang admin đã được tách thành các tab rõ ràng: Tổng quan, Cron jobs, Accounts, Public Bucket Proxy, Tất cả file, Runtime, Logs
- Có thêm tab **Tất cả file** để xem file của tất cả account hoặc lọc theo từng account
- Có thể **replace** hoặc **xóa** file đã track ngay trong admin mà không cần reload cả trang
- Cron jobs có thể **run trực tiếp** và xem log / báo cáo chi tiết ngay trên giao diện

### Cải thiện trải nghiệm dùng

- Bảng **Accounts** mặc định sắp theo ngày thêm account mới nhất
- Có thể bấm vào header để sắp xếp account theo các tiêu chí chính
- Tab **Public Bucket Proxy** chỉ tải danh sách file khi bạn bấm refresh, giúp vào trang nhanh hơn
- Các thao tác nặng đã có **loading**, **thông báo thành công / thất bại**, và refresh dữ liệu cục bộ theo từng tab
- Khi lưu account thành công, form account và phần thêm nhanh từ dịch vụ S3 sẽ tự clear

---
