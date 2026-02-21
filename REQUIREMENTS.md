# Spec: Robot Data Logger (RDL)
 
## Goals
 
A production-grade, always-on logging daemon for ROS 2 robots that is reliable enough to be the system of record for everything the robot does. It should require zero operator intervention in normal operation and recover gracefully from every failure mode.
 
---
 
## Core Requirements
 
**Reliability**
- No silent failures. Every dropped message, missed topic, or write error must be observable.
- Survive process crash, power loss, disk full, and middleware restart without corrupting previously written data.
- Resume recording automatically after any failure with no data gap beyond a configurable maximum window.
 
**Data Integrity**
- Every closed MCAP file must be verifiable — a checksum or index written at close time confirms the file is complete and uncorrupted.
- Files that were open at crash time must be marked as incomplete rather than left in an ambiguous state, and recoverable where possible.
- `fsync` on file close, and optionally on a configurable interval, to protect against OS page cache loss.
 
**Performance**
- Buffered writes with a configurable in-memory ring buffer, so disk I/O does not block message ingestion.
- Backpressure handling — if the write buffer fills, the system should drop the oldest buffered data (not the newest) and record the drop event, rather than blocking or crashing.
 
**Storage Management**
- Automatic file rotation by duration and size, both configurable.
- Automatic deletion of oldest files when disk usage exceeds a configurable threshold (e.g. 80% full).
- Reserved disk headroom — always keep a minimum free space buffer so the OS and other processes are not starved.
 
**Observability**
- Exposes a ROS 2 topic (or HTTP endpoint) publishing logger health: recording status, current file, buffer utilisation, drop count, disk usage, estimated time to full.
- Emits a structured log on every significant event: file open, file close, topic added, topic dropped, write error, recovery action taken.
- Alerting hooks — configurable callbacks (e.g. shell script or HTTP POST) when thresholds are breached.
 
**Topic Management**
- Wildcard and regex topic selection, with an explicit exclude list.
- QoS negotiation — automatically match the publisher's QoS rather than failing silently.
- Dynamic topic discovery — new topics published after startup are automatically included if they match the filter.
- Per-topic configuration: different buffer sizes or flush policies for high-frequency vs low-frequency topics.
 
**Boot and Lifecycle**
- Ships as a systemd service with a well-defined startup order relative to the ROS 2 middleware.
- Waits for middleware readiness before subscribing, with a timeout and retry rather than a crash loop.
- Handles middleware restart (e.g. if the ROS 2 daemon is restarted) by re-subscribing automatically.
- Graceful shutdown — flushes buffer, closes and fsync's the current file cleanly on SIGTERM.
 
---
 
## File Format
 
MCAP, with the following conventions:
 
- One MCAP file per rotation window.
- File naming: `<robot_id>_<ISO8601_timestamp>_<sequence>.mcap` — no dependency on a metadata.yaml sidecar.
- A `.incomplete` suffix on any file that was open at crash time, removed on clean close.
- An optional companion `.sha256` checksum file written atomically after each clean close.
- A small header chunk written at open time with robot ID, session ID, software version, and a file sequence number so gaps in the sequence are detectable.
 
---
 
## Recovery Behaviour
 
On startup, the daemon should:
 
1. Scan the log directory for `.incomplete` files.
2. Attempt to recover each one using the MCAP recovery/index-rebuild path where possible.
3. Log the outcome (recovered, partial, unrecoverable) and rename accordingly.
4. Begin a fresh file for new data regardless of recovery outcome.
 
---
 
## Storage Media Awareness
 
- Monitor write latency as a proxy for media health — log a warning if median write latency exceeds a configurable threshold.
- Track total bytes written over the lifetime of the mount as a rough wear indicator for eMMC/SD.
- Configurable option to spread writes across a secondary storage device if primary is degraded.
 
---
 
## Configuration
 
A single YAML config file, with environment variable overrides for deployment flexibility:
 
```yaml
robot_id: robot_001
log_directory: /data/logs
max_bag_duration_s: 60
max_bag_size_mb: 512
disk_usage_limit_pct: 80
min_free_disk_mb: 500
write_buffer_mb: 128
fsync_interval_s: 5
topics:
  include: [".*"]
  exclude: ["/rosout", "/diagnostics"]
health_topic: /rdl/health
alerts:
  disk_warning_pct: 70
  on_threshold_breach: "/etc/rdl/alert_hook.sh"
```
 
---
 
## What This Deliberately Does Not Include
 
- **Cloud sync / offload** — this is a local logging daemon. Data offload is a separate concern and should be handled by a separate service that reads completed MCAP files.
- **Playback or querying** — out of scope. Use Foxglove, `mcap` CLI, or your own tooling for that.
- **Encryption** — left to the filesystem or a wrapper layer.
- **Multi-robot coordination** — each robot runs its own instance. Fleet-level concerns live elsewhere.
 
