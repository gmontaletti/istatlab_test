# ISTAT Dashboard - Docker Deployment

Interactive dashboard for exploring ISTAT (Italian National Institute of Statistics) data on labor force, employment, and wages.

## Prerequisites

- Docker Engine 20.10 or later
- Docker Compose 2.0 or later
- At least 2 GB RAM available

## Quick Start

### Using Docker Compose (Recommended)

```bash
cd deploy/docker

# Build and start
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Using Docker directly

```bash
cd deploy/docker

# Build
docker build -t istat-dashboard:latest .

# Run
docker run -d \
  --name istat-dashboard \
  -p 3838:3838 \
  --restart unless-stopped \
  istat-dashboard:latest
```

## Access the Dashboard

```
http://localhost:3838/istat-dashboard/
```

Or on a remote server:

```
http://<server-ip>:3838/istat-dashboard/
```

## Management Commands

### Status
```bash
docker ps -f name=istat-dashboard
```

### Logs
```bash
docker logs istat-dashboard
docker logs -f istat-dashboard
```

### Stop
```bash
docker-compose down
# or
docker stop istat-dashboard && docker rm istat-dashboard
```

### Restart
```bash
docker restart istat-dashboard
```

## Updating Data

### Option A: Rebuild with new data

```bash
# Copy new optimized qs files (recommended)
cp /path/to/new/data/quarterly/*.qs app/data/quarterly/
cp /path/to/new/data/vacancies/*.qs app/data/vacancies/
cp /path/to/new/data/wages/*.qs app/data/wages/

# Copy legacy RDS files (backward compatibility)
cp /path/to/new/data/*.rds app/data/

# Rebuild
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Option B: Volume mount for frequent updates

Uncomment the volumes section in docker-compose.yml, then:

```bash
# Update files in app/data/
docker-compose restart
```

## Upgrading from v1.0 (RDS-only) to v1.1 (qs format)

If you have an existing installation using RDS files, follow these steps:

```bash
# 1. On your development machine, generate new data
cd /path/to/istatlab_test
Rscript R/prepare_deployment_data.R

# 2. Transfer files to remote server
scp -r deploy/data/quarterly user@server:/path/to/docker/app/data/
scp -r deploy/data/vacancies user@server:/path/to/docker/app/data/
scp -r deploy/data/wages user@server:/path/to/docker/app/data/
scp deploy/docker/app/app.Rmd user@server:/path/to/docker/app/
scp deploy/docker/Dockerfile user@server:/path/to/docker/

# 3. On the remote server, rebuild
ssh user@server
cd /path/to/docker
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Configuration

### Change port

Edit docker-compose.yml:
```yaml
ports:
  - "8080:3838"
```

### Adjust memory

Edit docker-compose.yml:
```yaml
deploy:
  resources:
    limits:
      memory: 4G
```

## Troubleshooting

### Port already in use
```bash
lsof -i :3838
```

### Check resource usage
```bash
docker stats istat-dashboard
```

### Health status
```bash
docker inspect --format='{{.State.Health.Status}}' istat-dashboard
```

### Shiny Server logs
```bash
docker exec istat-dashboard cat /var/log/shiny-server.log
```

## Technical Details

| Specification | Value |
|--------------|-------|
| Base image | rocker/shiny:4.4.0 |
| Port | 3838 |
| Memory limit | 2 GB |
| Data size | ~27 MB (qs) + ~17 MB (rds) |
| Image size | ~1.5-2 GB |
| Startup time | <10 seconds (with qs) |

### Data Files

**Optimized qs format (lazy loading, faster startup):**

| Directory | Size | Description |
|-----------|------|-------------|
| quarterly/ | ~24 MB | 12 partitioned quarterly datasets |
| vacancies/ | ~1.5 MB | 2 partitioned monthly datasets |
| wages/ | ~1.4 MB | 2 partitioned monthly datasets |

**Legacy RDS format (backward compatibility):**

| File | Size | Description |
|------|------|-------------|
| quarterly_data.rds | ~17 MB | Combined quarterly data |
| vacancies_data.rds | ~1.3 MB | Combined vacancies data |
| wages_data.rds | ~1.9 MB | Combined wages data |

### Performance Improvements (v1.1)

| Metric | v1.0 (RDS) | v1.1 (qs) |
|--------|------------|-----------|
| Dashboard startup | 3-5s | <0.5s |
| Per-dataset load | All at once | On-demand |
| Memory usage | All data | Active dataset only |

## Author

Giampaolo Montaletti
- Email: giampaolo.montaletti@gmail.com
- GitHub: https://github.com/gmontaletti
- ORCID: https://orcid.org/0009-0002-5327-1122
