# StoryFlow Scripts

Bộ scripts hỗ trợ deployment, quản lý và debugging StoryFlow Core.

> **⭐ NEW:** Quick Reference Guide - [SCRIPTS_QUICK_REFERENCE.md](../docs/SCRIPTS_QUICK_REFERENCE.md)

## 🔧 Development & Debug Scripts (NEW)

### restart-service.sh - Restart Single Service
Restart hoặc rebuild một service worker cụ thể mà không cần restart toàn bộ hệ thống.

```bash
# Quick restart (no rebuild)
./scripts/restart-service.sh health-checker

# Rebuild and restart (sau khi sửa code)
./scripts/restart-service.sh health-checker --rebuild

# List all services
./scripts/restart-service.sh --help
```

**Use cases:**
- Fix lỗi worker và deploy lại nhanh (~5-10s for restart, ~30-60s for rebuild)
- Restart service sau khi thay đổi config
- Rebuild image sau khi sửa code

---

### run-tool.sh - Debug Tools Runner
Wrapper để chạy debug tools trong Docker containers với đầy đủ dependencies.

```bash
# List all available tools
./scripts/run-tool.sh --list

# Debug site parsers
./scripts/run-tool.sh debug_site_parse xtruyen

# Kafka debugging
./scripts/run-tool.sh kafka/analyze
./scripts/run-tool.sh kafka/peek
./scripts/run-tool.sh kafka/duplicates

# Test tools
./scripts/run-tool.sh test_harvester_api_v2

# Run in specific container
./scripts/run-tool.sh --container crawler-consumer debug_site_parse
```

**Available tools categories:**
- Site-specific debug: `debug_site_parse`, `debug_metruyenful`, `debug_quykiep`, etc.
- Kafka tools: `kafka/analyze`, `kafka/peek`, `kafka/duplicates`
- Test tools: `test_harvester_api_v2`, `test_db_connection`, etc.
- Inspection: `inspect_site_data`, `check_filter_config`, `query_database`

📖 **Full documentation:** [SCRIPTS_QUICK_REFERENCE.md](../docs/SCRIPTS_QUICK_REFERENCE.md)

---

## 🚀 Deployment Scripts

### 🚀 deploy.sh - Main Deployment Script

Script chính để deploy toàn bộ hệ thống.

```bash
./scripts/deploy.sh [IMAGE_TAG]
```

**Examples:**
```bash
./scripts/deploy.sh latest          # Deploy bản latest
./scripts/deploy.sh develop         # Deploy branch develop
./scripts/deploy.sh v1.0.0          # Deploy version cụ thể
./scripts/deploy.sh main-abc1234    # Deploy commit cụ thể
```

**Features:**
- ✅ Automatic backup before deployment
- ✅ Pull latest Docker image
- ✅ Update environment variables
- ✅ Graceful service restart
- ✅ Health check verification
- ✅ Auto rollback on failure
- ✅ Cleanup old images

---

### 🔄 update-services.sh - Quick Service Update

Cập nhật nhanh một hoặc nhiều services.

```bash
./scripts/update-services.sh [service1] [service2] ...
```

**Examples:**
```bash
./scripts/update-services.sh                          # Update all
./scripts/update-services.sh crawler-producer         # Update one
./scripts/update-services.sh crawler-producer crawler-consumer  # Update multiple
```

---

### 💚 health-check.sh - Health Status Check

Kiểm tra health status của tất cả services.

```bash
./scripts/health-check.sh
```

**Checks:**
- Container running status
- Service health status
- Kafka connectivity
- Dashboard API (port 8080)
- Grafana API (port 3000)

**Exit codes:**
- `0` - All services healthy
- `1` - Some services unhealthy

---

### 📋 logs.sh - View Service Logs

Xem logs từ các services.

```bash
./scripts/logs.sh [service] [options]
```

**Examples:**
```bash
./scripts/logs.sh                           # All logs
./scripts/logs.sh crawler-producer          # Specific service
./scripts/logs.sh crawler-consumer -f       # Follow logs
./scripts/logs.sh crawler-producer --tail=200  # Last 200 lines
```

---

## Quick Start

### First Time Setup

```bash
# 1. Make scripts executable
chmod +x scripts/*.sh

# 2. Deploy
./scripts/deploy.sh latest

# 3. Check health
./scripts/health-check.sh

# 4. View logs
./scripts/logs.sh
```

### Regular Operations

```bash
# Update to new version
./scripts/deploy.sh v1.1.0

# Quick restart a service
./scripts/update-services.sh crawler-producer

# Monitor logs
./scripts/logs.sh crawler-producer -f

# Health check
./scripts/health-check.sh
```

---

## Prerequisites

- Docker và Docker Compose đã cài đặt
- Quyền truy cập Docker (user trong docker group)
- Network connectivity to Docker Hub
- Đủ disk space cho images

---

## Environment Variables

Scripts sử dụng các biến môi trường sau:

```bash
DOCKER_IMAGE=muonroii/storyflow-core    # Docker image name
STORYFLOW_CORE_IMAGE=...                # Full image with tag
```

Set trong `.env` file hoặc export:

```bash
export DOCKER_IMAGE=muonroii/storyflow-core
export STORYFLOW_CORE_IMAGE=muonroii/storyflow-core:v1.0.0
```

---

## Troubleshooting

### Script Permission Denied

```bash
chmod +x scripts/*.sh
```

### Docker Permission Denied

```bash
sudo usermod -aG docker $USER
# Logout and login again
```

### Service Won't Start

```bash
# Check logs
./scripts/logs.sh service-name

# Check resources
docker stats

# Check disk space
df -h
```

### Deployment Failed

Script tự động rollback, hoặc manual:

```bash
# Check backup directory
ls -la backups/

# Restore from backup
cd docker
docker compose down
# Restore config files from backup
docker compose up -d
```

---

## Advanced Usage

### Custom Docker Image

```bash
export DOCKER_IMAGE=your-dockerhub/storyflow-core
./scripts/deploy.sh v1.0.0
```

### Deploy without Auto-pull

```bash
# Modify deploy.sh to skip pull step
# Or manually:
cd docker
export STORYFLOW_CORE_IMAGE=muonroii/storyflow-core:v1.0.0
docker compose up -d
```

### Selective Service Restart

```bash
# Only restart specific services
cd docker
docker compose restart crawler-producer crawler-consumer
```

---

## Integration with CI/CD

Scripts được thiết kế để tích hợp với GitHub Actions:

```yaml
- name: Deploy
  run: |
    ./scripts/deploy.sh ${{ steps.meta.outputs.tags }}
```

Xem [CI_CD_SETUP.md](../docs/CI_CD_SETUP.md) để biết thêm chi tiết.

---

## Best Practices

1. **Always backup before deploy**: Scripts tự động làm điều này
2. **Check health after deploy**: Chạy `health-check.sh`
3. **Monitor logs**: Theo dõi logs trong 10-15 phút đầu
4. **Keep backups**: Giữ lại ít nhất 3 backups gần nhất
5. **Test on staging first**: Deploy staging trước production

---

## Support

Nếu gặp vấn đề, tham khảo:
- [CI/CD Setup Guide](../docs/CI_CD_SETUP.md)
- [System Overview](../docs/SYSTEM_OVERVIEW.md)
- Create GitHub Issue với logs

---

**Last Updated:** 2025-11-27
