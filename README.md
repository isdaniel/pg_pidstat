# pg_pidstat

A PostgreSQL extension that adds **real-time per-backend CPU, memory, and I/O monitoring** to PostgreSQL. 

## Features

- Per-backend CPU usage percentage of total system capacity (100% = all vCPUs saturated)
- Per-backend memory usage (percentage of total RAM and absolute MB)
- Per-backend I/O throughput (read/write bytes per second)
- Per-backend I/O operations (read/write IOPS)
- I/O wait detection (process blocked on disk)
- Context switch rates (voluntary and non-voluntary)
- Seamlessly joins with `pg_stat_activity` for full connection context
- Background worker samples all backends every 1 second
- Minimal lock contention — lock-free reads with brief exclusive writes
- Supports PostgreSQL 14, 15, 16, 17 (default), and 18

## Requirements

- **Linux** (reads from `/proc` filesystem)
- PostgreSQL 15–18
- Rust toolchain and [pgrx](https://github.com/pgcentralfoundation/pgrx) 0.16.1

## Installation

1. Build and install the extension:

   ```bash
   cargo pgrx install --release
   ```

2. Add to `postgresql.conf`:

   ```
   shared_preload_libraries = 'pg_pidstat'
   ```

3. Restart PostgreSQL (required for shared memory and background worker registration).

4. Create the extension in your database:

   ```sql
   CREATE EXTENSION pg_pidstat;
   ```

## Usage

### Find resource-heavy backends

```sql
SELECT pid, datname, usename, state, query,
       cpu_percent, memory_percent, memory_usage_mb
FROM pg_pidstat
ORDER BY cpu_percent DESC;
```

### Find I/O-heavy queries

```sql
SELECT pid, usename, state, query,
       io_read_bytes_per_sec,
       io_write_bytes_per_sec,
       io_read_ops_per_sec,
       io_write_ops_per_sec,
       io_wait
FROM pg_pidstat
WHERE state = 'active'
ORDER BY io_read_bytes_per_sec + io_write_bytes_per_sec DESC;
```

### Detect context switch hotspots

```sql
SELECT pid, usename, query,
       voluntary_ctxt_switches_per_sec,
       nonvoluntary_ctxt_switches_per_sec
FROM pg_pidstat
WHERE state = 'active'
ORDER BY voluntary_ctxt_switches_per_sec DESC;
```

### Full troubleshooting overview

```sql
SELECT pid, datname, usename, state,
       cpu_percent,
       memory_usage_mb,
       io_read_bytes_per_sec AS read_bps,
       io_write_bytes_per_sec AS write_bps,
       io_read_ops_per_sec AS read_iops,
       io_write_ops_per_sec AS write_iops,
       io_wait,
       voluntary_ctxt_switches_per_sec AS vol_cs,
       nonvoluntary_ctxt_switches_per_sec AS nonvol_cs,
       query
FROM pg_pidstat
WHERE state = 'active'
ORDER BY cpu_percent DESC;
```

## Column Reference

The `pg_pidstat` view includes all standard `pg_stat_activity` columns plus:

| Column | Type | Unit | Description |
|---|---|---|---|
| `cpu_percent` | `double precision` | % of total CPU | CPU usage as percentage of total system capacity (e.g. on 8 vCPUs: 1 busy core = 12.5%, all cores = 100%) |
| `memory_percent` | `double precision` | % of total RAM | Resident memory as a percentage of total system RAM |
| `memory_usage_mb` | `double precision` | MB | Resident memory (RSS) in megabytes |
| `io_read_bytes_per_sec` | `double precision` | bytes/s | Storage read throughput |
| `io_write_bytes_per_sec` | `double precision` | bytes/s | Storage write throughput |
| `io_read_ops_per_sec` | `double precision` | ops/s | Read syscall rate (IOPS) |
| `io_write_ops_per_sec` | `double precision` | ops/s | Write syscall rate (IOPS) |
| `io_wait` | `boolean` | — | `true` if the process is in uninterruptible disk sleep (state D) |
| `voluntary_ctxt_switches_per_sec` | `double precision` | switches/s | Voluntary context switch rate (I/O waits, lock contention) |
| `nonvoluntary_ctxt_switches_per_sec` | `double precision` | switches/s | Non-voluntary context switch rate (CPU preemption) |

You can also call the underlying function directly:

```sql
SELECT * FROM pg_pidstat_stats();
```

## Architecture

The extension consists of four modules:

- **`proc_stats`** — Reads `/proc/[pid]/stat`, `/proc/[pid]/statm`, `/proc/[pid]/io`, and `/proc/[pid]/status` for CPU ticks, RSS, I/O counters, process state, and context switches of all PostgreSQL backend processes.
- **`shmem`** — Manages a double-buffered shared memory region (protected by a PostgreSQL lightweight lock) holding snapshots for up to 1024 backends.
- **`bgworker`** — A background worker that samples all backends every second, rotating snapshots in shared memory.
- **`activity`** — Exposes `pg_pidstat_stats()` and the `pg_pidstat` view to SQL, computing per-second rates from snapshot deltas.

### Data Sources

| Metric | `/proc` file | Fields used |
|---|---|---|
| CPU ticks | `/proc/[pid]/stat` | `utime`, `stime` |
| Memory (RSS) | `/proc/[pid]/statm` | `resident` |
| I/O bytes | `/proc/[pid]/io` | `read_bytes`, `write_bytes` |
| I/O syscalls | `/proc/[pid]/io` | `syscr`, `syscw` |
| I/O wait | `/proc/[pid]/stat` | `state` (char `D` = disk sleep) |
| Context switches | `/proc/[pid]/status` | `voluntary_ctxt_switches`, `nonvoluntary_ctxt_switches` |
| Total memory | `/proc/meminfo` | `MemTotal` |

## Build Optimization

Release builds use maximum optimization (`opt-level = 3`, fat LTO, single codegen unit) for best runtime performance.

## License

See the project license file for details.
