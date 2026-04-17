//! SQL-exposed function and view for querying resource usage data.

use crate::shmem::{PidSnapshot, USAGE_DATA};
use pgrx::prelude::*;
use std::collections::HashMap;

/// Returns per-backend CPU, memory, I/O, and context switch usage stats by reading shared memory.
///
/// This function is designed to be LEFT JOINed with pg_stat_activity on pid.
///
/// ## Column units
///
/// - `cpu_percent`: percentage of **total** system CPU capacity (0–100%, where 100% = all vCPUs fully saturated)
/// - `memory_percent`: percentage of total system RAM
/// - `memory_usage_mb`: resident memory in megabytes (RSS)
/// - `io_read_bytes_per_sec` / `io_write_bytes_per_sec`: bytes per second
/// - `io_read_ops_per_sec` / `io_write_ops_per_sec`: syscalls per second (IOPS)
/// - `io_wait`: true when the process is in uninterruptible disk sleep (state 'D')
/// - `voluntary_ctxt_switches_per_sec` / `nonvoluntary_ctxt_switches_per_sec`: switches per second
#[pg_extern]
fn pg_pidstat_stats() -> TableIterator<
    'static,
    (
        name!(pid, i32),
        name!(cpu_percent, f64),
        name!(memory_percent, f64),
        name!(memory_usage_mb, f64),
        name!(io_read_bytes_per_sec, f64),
        name!(io_write_bytes_per_sec, f64),
        name!(io_read_ops_per_sec, f64),
        name!(io_write_ops_per_sec, f64),
        name!(io_wait, bool),
        name!(voluntary_ctxt_switches_per_sec, f64),
        name!(nonvoluntary_ctxt_switches_per_sec, f64),
    ),
> {
    let rows = compute_usage_rows();
    TableIterator::new(rows)
}

/// Compute usage rows from shared memory snapshots.
fn compute_usage_rows() -> Vec<(i32, f64, f64, f64, f64, f64, f64, f64, bool, f64, f64)> {
    // Read shared memory under a shared lock, copy to local variables
    let (prev_snapshots, curr_snapshots, total_memory_bytes, page_size, ticks_per_second, num_cpus) = {
        let data = USAGE_DATA.share();
        let prev: Vec<PidSnapshot> = data.prev[..data.prev_count].to_vec();
        let curr: Vec<PidSnapshot> = data.curr[..data.curr_count].to_vec();
        (prev, curr, data.total_memory_bytes, data.page_size, data.ticks_per_second, data.num_cpus)
    };
    // Lock is released here

    compute_usage_from_snapshots(
        &prev_snapshots,
        &curr_snapshots,
        total_memory_bytes,
        page_size,
        ticks_per_second,
        num_cpus,
    )
}

/// Pure computation: given two snapshot buffers and system constants, produce output rows.
///
/// Separated from `compute_usage_rows` so it can be unit-tested without shared memory.
pub(crate) fn compute_usage_from_snapshots(
    prev_snapshots: &[PidSnapshot],
    curr_snapshots: &[PidSnapshot],
    total_memory_bytes: u64,
    page_size: u64,
    ticks_per_second: u64,
    num_cpus: u64,
) -> Vec<(i32, f64, f64, f64, f64, f64, f64, f64, bool, f64, f64)> {
    if curr_snapshots.is_empty() || ticks_per_second == 0 {
        return Vec::new();
    }

    let num_cpus = if num_cpus == 0 { 1 } else { num_cpus };

    // Build a lookup map for previous snapshots by PID
    let prev_map: HashMap<i32, &PidSnapshot> = prev_snapshots
        .iter()
        .map(|s| (s.pid, s))
        .collect();

    let mut rows = Vec::with_capacity(curr_snapshots.len());

    for curr in curr_snapshots {
        // Delta time in seconds between samples
        let delta_seconds = if let Some(prev) = prev_map.get(&curr.pid) {
            if curr.sample_time_us > prev.sample_time_us {
                (curr.sample_time_us - prev.sample_time_us) as f64 / 1_000_000.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        // CPU% as percentage of total system capacity.
        // e.g. on 8 vCPUs: a process using 1 full core = 12.5%, all 8 cores = 100%.
        let cpu_percent = if let Some(prev) = prev_map.get(&curr.pid) {
            if curr.cpu_ticks >= prev.cpu_ticks && delta_seconds > 0.0 {
                let delta_ticks = curr.cpu_ticks - prev.cpu_ticks;
                let cpu_seconds = delta_ticks as f64 / ticks_per_second as f64;
                let raw_pct = cpu_seconds / delta_seconds * 100.0;
                (raw_pct / num_cpus as f64).min(100.0)
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Memory calculation — RSS in MB
        let memory_bytes = curr.rss_pages * page_size;
        let memory_usage_mb = memory_bytes as f64 / (1024.0 * 1024.0);
        let memory_percent = if total_memory_bytes > 0 {
            (memory_bytes as f64 / total_memory_bytes as f64) * 100.0
        } else {
            0.0
        };

        // I/O and context switch rates — delta-based like CPU%
        let (io_read_bps, io_write_bps, io_read_ops, io_write_ops, vol_cs_rate, nonvol_cs_rate) =
            if let Some(prev) = prev_map.get(&curr.pid) {
                if delta_seconds > 0.0
                    && curr.read_bytes >= prev.read_bytes
                    && curr.write_bytes >= prev.write_bytes
                {
                    (
                        (curr.read_bytes - prev.read_bytes) as f64 / delta_seconds,
                        (curr.write_bytes - prev.write_bytes) as f64 / delta_seconds,
                        (curr.syscr.saturating_sub(prev.syscr)) as f64 / delta_seconds,
                        (curr.syscw.saturating_sub(prev.syscw)) as f64 / delta_seconds,
                        (curr.voluntary_ctxt_switches.saturating_sub(prev.voluntary_ctxt_switches))
                            as f64
                            / delta_seconds,
                        (curr
                            .nonvoluntary_ctxt_switches
                            .saturating_sub(prev.nonvoluntary_ctxt_switches))
                            as f64
                            / delta_seconds,
                    )
                } else {
                    (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                }
            } else {
                (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            };

        let io_wait = curr.io_wait != 0;

        rows.push((
            curr.pid,
            cpu_percent,
            memory_percent,
            memory_usage_mb,
            io_read_bps,
            io_write_bps,
            io_read_ops,
            io_write_ops,
            io_wait,
            vol_cs_rate,
            nonvol_cs_rate,
        ));
    }

    rows
}

// Create the view that joins pg_stat_activity with our resource stats
extension_sql!(
    r#"
CREATE VIEW pg_pidstat AS
SELECT
    a.pid,
    a.datname,
    a.usename,
    a.application_name,
    a.client_addr,
    a.client_port,
    a.backend_start,
    a.xact_start,
    a.query_start,
    a.state_change,
    a.wait_event_type,
    a.wait_event,
    a.state,
    a.backend_type,
    a.query,
    COALESCE(s.cpu_percent, 0.0) AS cpu_percent,
    COALESCE(s.memory_percent, 0.0) AS memory_percent,
    COALESCE(s.memory_usage_mb, 0.0) AS memory_usage_mb,
    COALESCE(s.io_read_bytes_per_sec, 0.0) AS io_read_bytes_per_sec,
    COALESCE(s.io_write_bytes_per_sec, 0.0) AS io_write_bytes_per_sec,
    COALESCE(s.io_read_ops_per_sec, 0.0) AS io_read_ops_per_sec,
    COALESCE(s.io_write_ops_per_sec, 0.0) AS io_write_ops_per_sec,
    COALESCE(s.io_wait, false) AS io_wait,
    COALESCE(s.voluntary_ctxt_switches_per_sec, 0.0) AS voluntary_ctxt_switches_per_sec,
    COALESCE(s.nonvoluntary_ctxt_switches_per_sec, 0.0) AS nonvoluntary_ctxt_switches_per_sec
FROM pg_stat_activity a
LEFT JOIN pg_pidstat_stats() s ON a.pid = s.pid;
"#,
    name = "create_pg_pidstat_view",
    requires = [pg_pidstat_stats]
);
