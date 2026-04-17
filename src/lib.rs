//! pg_pidstat — Monitor CPU, memory, and I/O usage per PostgreSQL backend.
//!
//! This extension adds a `pg_pidstat` view that extends `pg_stat_activity`
//! with per-backend CPU percentage, memory (MB), I/O throughput, IOPS, I/O wait,
//! and context switch rates.
//!
//! A background worker periodically samples `/proc/[pid]/stat`,
//! `/proc/[pid]/statm`, `/proc/[pid]/io`, and `/proc/[pid]/status` for all
//! PostgreSQL backend processes and stores snapshots in shared memory.
//!
//! ## Setup
//!
//! Add to `postgresql.conf`:
//! ```text
//! shared_preload_libraries = 'pg_pidstat'
//! ```
//!
//! Then restart PostgreSQL and run:
//! ```sql
//! CREATE EXTENSION pg_pidstat;
//! SELECT * FROM pg_pidstat;
//! ```

mod activity;
mod bgworker;
mod proc_stats;
mod shmem;

use pgrx::prelude::*;
use pgrx::pg_shmem_init;

// Re-export for pg_shmem_init! macro (requires a plain ident)
use shmem::USAGE_DATA;

::pgrx::pg_module_magic!(name, version);

/// Extension initialization — called when PostgreSQL loads the shared library.
///
/// This function:
/// 1. Verifies loading via `shared_preload_libraries`
/// 2. Initializes shared memory for resource usage data
/// 3. Registers the background worker for periodic sampling
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    if !pg_sys::process_shared_preload_libraries_in_progress {
        pgrx::error!("pg_pidstat must be loaded via shared_preload_libraries");
    }

    // Skip during binary upgrade
    if pg_sys::IsBinaryUpgrade {
        return;
    }

    // Initialize shared memory
    pg_shmem_init!(USAGE_DATA);

    // Register the background worker
    bgworker::register_background_worker();
}

// =============================================================================
// pgrx in-database tests (require a running PostgreSQL instance)
// =============================================================================
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pg_pidstat_stats_returns_rows() {
        let result = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_pidstat_stats()",
        );
        assert!(result.unwrap().unwrap() >= 0);
    }

    #[pg_test]
    fn test_pg_pidstat_view_returns_rows() {
        let result = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_pidstat",
        );
        assert!(result.unwrap().unwrap() >= 1);
    }

    #[pg_test]
    fn test_pg_pidstat_view_has_resource_columns() {
        let result = Spi::get_one::<bool>(
            "SELECT cpu_percent IS NOT NULL AND memory_percent IS NOT NULL AND memory_usage_mb IS NOT NULL
                AND io_read_bytes_per_sec IS NOT NULL AND io_write_bytes_per_sec IS NOT NULL
                AND io_read_ops_per_sec IS NOT NULL AND io_write_ops_per_sec IS NOT NULL
                AND io_wait IS NOT NULL
                AND voluntary_ctxt_switches_per_sec IS NOT NULL AND nonvoluntary_ctxt_switches_per_sec IS NOT NULL
             FROM pg_pidstat LIMIT 1",
        );
        assert_eq!(result.unwrap().unwrap(), true);
    }

    #[pg_test]
    fn test_memory_usage_mb_is_non_negative() {
        let result = Spi::get_one::<f64>(
            "SELECT memory_usage_mb FROM pg_pidstat WHERE pid = pg_backend_pid()",
        );
        let mb = result.unwrap().unwrap();
        assert!(mb >= 0.0, "memory_usage_mb should be >= 0, got {mb}");
    }

    #[pg_test]
    fn test_cpu_percent_is_non_negative() {
        let result = Spi::get_one::<f64>(
            "SELECT cpu_percent FROM pg_pidstat WHERE pid = pg_backend_pid()",
        );
        let cpu = result.unwrap().unwrap();
        assert!(cpu >= 0.0, "cpu_percent should be >= 0, got {cpu}");
    }

    #[pg_test]
    fn test_cpu_percent_does_not_exceed_100() {
        let result = Spi::get_one::<f64>(
            "SELECT cpu_percent FROM pg_pidstat WHERE pid = pg_backend_pid()",
        );
        let cpu = result.unwrap().unwrap();
        assert!(cpu <= 100.0, "cpu_percent should be <= 100, got {cpu}");
    }

    #[pg_test]
    fn test_io_columns_are_non_negative() {
        let result = Spi::get_one::<bool>(
            "SELECT io_read_bytes_per_sec >= 0 AND io_write_bytes_per_sec >= 0
                AND io_read_ops_per_sec >= 0 AND io_write_ops_per_sec >= 0
             FROM pg_pidstat WHERE pid = pg_backend_pid()",
        );
        assert_eq!(result.unwrap().unwrap(), true);
    }

    #[pg_test]
    fn test_context_switch_columns_are_non_negative() {
        let result = Spi::get_one::<bool>(
            "SELECT voluntary_ctxt_switches_per_sec >= 0
                AND nonvoluntary_ctxt_switches_per_sec >= 0
             FROM pg_pidstat WHERE pid = pg_backend_pid()",
        );
        assert_eq!(result.unwrap().unwrap(), true);
    }
}

/// This module is required by `cargo pgrx test` invocations.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_pidstat'"]
    }
}

// =============================================================================
// Pure Rust unit tests — run with `cargo test` (no PostgreSQL required)
// =============================================================================
#[cfg(test)]
mod unit_tests {
    use crate::activity::compute_usage_from_snapshots;
    use crate::shmem::PidSnapshot;

    fn make_snapshot(
        pid: i32,
        cpu_ticks: u64,
        rss_pages: u64,
        sample_time_us: u64,
        read_bytes: u64,
        write_bytes: u64,
        syscr: u64,
        syscw: u64,
        io_wait: u8,
        vol_cs: u64,
        nonvol_cs: u64,
    ) -> PidSnapshot {
        PidSnapshot {
            pid,
            cpu_ticks,
            rss_pages,
            sample_time_us,
            read_bytes,
            write_bytes,
            syscr,
            syscw,
            io_wait,
            voluntary_ctxt_switches: vol_cs,
            nonvoluntary_ctxt_switches: nonvol_cs,
        }
    }

    const PAGE_SIZE: u64 = 4096;
    const TICKS_PER_SEC: u64 = 100;
    const TOTAL_MEM: u64 = 16 * 1024 * 1024 * 1024; // 16 GB
    const NUM_CPUS: u64 = 8;

    #[test]
    fn test_empty_snapshots_returns_empty() {
        let rows = compute_usage_from_snapshots(&[], &[], TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_zero_ticks_per_second_returns_empty() {
        let curr = vec![make_snapshot(1, 100, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&[], &curr, TOTAL_MEM, PAGE_SIZE, 0, NUM_CPUS);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_no_previous_sample_returns_zero_rates() {
        let curr = vec![make_snapshot(42, 500, 2000, 2_000_000, 4096, 8192, 10, 20, 0, 50, 10)];
        let rows = compute_usage_from_snapshots(&[], &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        assert_eq!(rows.len(), 1);
        let (pid, cpu, _mem_pct, _mem_mb, read_bps, write_bps, read_ops, write_ops, io_wait, vol_cs, nonvol_cs) = rows[0];
        assert_eq!(pid, 42);
        assert_eq!(cpu, 0.0);
        assert_eq!(read_bps, 0.0);
        assert_eq!(write_bps, 0.0);
        assert_eq!(read_ops, 0.0);
        assert_eq!(write_ops, 0.0);
        assert!(!io_wait);
        assert_eq!(vol_cs, 0.0);
        assert_eq!(nonvol_cs, 0.0);
    }

    #[test]
    fn test_cpu_percent_single_core_usage_on_8_vcpu() {
        // 1 second interval, 100 ticks = 1 full core on 8 vCPU system = 12.5%
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 200, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 8);

        let cpu = rows[0].1;
        assert!((cpu - 12.5).abs() < 0.01, "expected 12.5% (1/8 cores), got {cpu}");
    }

    #[test]
    fn test_cpu_percent_half_system_on_8_vcpu() {
        // 1 second, 400 ticks = 4 full cores on 8 vCPU = 50%
        let prev = vec![make_snapshot(1, 0, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 400, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 8);

        let cpu = rows[0].1;
        assert!((cpu - 50.0).abs() < 0.01, "expected 50% (4/8 cores), got {cpu}");
    }

    #[test]
    fn test_cpu_percent_capped_at_100() {
        // Even with huge ticks, should cap at 100%
        let prev = vec![make_snapshot(1, 0, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 10000, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 4);

        let cpu = rows[0].1;
        assert!((cpu - 100.0).abs() < 0.01, "expected capped at 100%, got {cpu}");
    }

    #[test]
    fn test_cpu_percent_single_vcpu_system() {
        // On 1 vCPU, 50 ticks / 100 tps / 1s = 50%
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 150, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 1);

        let cpu = rows[0].1;
        assert!((cpu - 50.0).abs() < 0.01, "expected 50% on 1 vCPU, got {cpu}");
    }

    #[test]
    fn test_cpu_percent_zero_num_cpus_treated_as_one() {
        // num_cpus=0 should be treated as 1 (fallback)
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 150, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 0);

        let cpu = rows[0].1;
        assert!((cpu - 50.0).abs() < 0.01, "expected 50% with num_cpus=0 fallback, got {cpu}");
    }

    #[test]
    fn test_memory_usage_mb() {
        // 2000 pages * 4096 bytes = 8192000 bytes = ~7.8125 MB
        let curr = vec![make_snapshot(1, 0, 2000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&[], &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        let mem_mb = rows[0].3;
        let expected = 2000.0 * 4096.0 / (1024.0 * 1024.0);
        assert!((mem_mb - expected).abs() < 0.001, "expected {expected} MB, got {mem_mb}");
    }

    #[test]
    fn test_memory_percent() {
        let rss_pages = 1000u64;
        let curr = vec![make_snapshot(1, 0, rss_pages, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&[], &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        let mem_pct = rows[0].2;
        let expected = (rss_pages * PAGE_SIZE) as f64 / TOTAL_MEM as f64 * 100.0;
        assert!((mem_pct - expected).abs() < 0.0001, "expected {expected}%, got {mem_pct}");
    }

    #[test]
    fn test_io_bytes_per_sec() {
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 100, 1000, 2_000_000, 1_048_576, 2_097_152, 100, 200, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        let read_bps = rows[0].4;
        let write_bps = rows[0].5;
        assert!((read_bps - 1_048_576.0).abs() < 0.01, "expected ~1MB/s read, got {read_bps}");
        assert!((write_bps - 2_097_152.0).abs() < 0.01, "expected ~2MB/s write, got {write_bps}");
    }

    #[test]
    fn test_io_ops_per_sec() {
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 100, 1000, 2_000_000, 4096, 8192, 500, 1000, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        let read_ops = rows[0].6;
        let write_ops = rows[0].7;
        assert!((read_ops - 500.0).abs() < 0.01, "expected 500 read IOPS, got {read_ops}");
        assert!((write_ops - 1000.0).abs() < 0.01, "expected 1000 write IOPS, got {write_ops}");
    }

    #[test]
    fn test_io_wait_flag() {
        let curr_waiting = vec![make_snapshot(1, 0, 1000, 1_000_000, 0, 0, 0, 0, 1, 0, 0)];
        let curr_not_waiting = vec![make_snapshot(2, 0, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];

        let rows1 = compute_usage_from_snapshots(&[], &curr_waiting, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);
        let rows2 = compute_usage_from_snapshots(&[], &curr_not_waiting, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        assert!(rows1[0].8, "io_wait should be true for state D");
        assert!(!rows2[0].8, "io_wait should be false for non-D state");
    }

    #[test]
    fn test_context_switch_rates() {
        let prev = vec![make_snapshot(1, 100, 1000, 1_000_000, 0, 0, 0, 0, 0, 100, 50)];
        let curr = vec![make_snapshot(1, 100, 1000, 2_000_000, 0, 0, 0, 0, 0, 350, 150)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        let vol_cs = rows[0].9;
        let nonvol_cs = rows[0].10;
        assert!((vol_cs - 250.0).abs() < 0.01, "expected 250 vol cs/s, got {vol_cs}");
        assert!((nonvol_cs - 100.0).abs() < 0.01, "expected 100 nonvol cs/s, got {nonvol_cs}");
    }

    #[test]
    fn test_pid_reuse_guard_returns_zero_cpu() {
        let prev = vec![make_snapshot(1, 500, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let curr = vec![make_snapshot(1, 100, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        assert_eq!(rows[0].1, 0.0, "CPU should be 0 when PID was recycled");
    }

    #[test]
    fn test_multiple_pids() {
        let prev = vec![
            make_snapshot(1, 100, 500, 1_000_000, 0, 0, 0, 0, 0, 0, 0),
            make_snapshot(2, 200, 1000, 1_000_000, 0, 0, 0, 0, 0, 0, 0),
        ];
        let curr = vec![
            make_snapshot(1, 200, 500, 2_000_000, 0, 0, 0, 0, 0, 0, 0),
            make_snapshot(2, 250, 1000, 2_000_000, 0, 0, 0, 0, 0, 0, 0),
            make_snapshot(3, 50, 750, 2_000_000, 0, 0, 0, 0, 0, 0, 0), // new PID
        ];
        // Use 4 vCPUs for this test
        let rows = compute_usage_from_snapshots(&prev, &curr, TOTAL_MEM, PAGE_SIZE, TICKS_PER_SEC, 4);

        assert_eq!(rows.len(), 3);
        // PID 1: 100 ticks / 100 tps / 1s = 100% raw, / 4 cpus = 25%
        assert!((rows[0].1 - 25.0).abs() < 0.01, "PID 1 expected 25%, got {}", rows[0].1);
        // PID 2: 50 ticks / 100 tps / 1s = 50% raw, / 4 cpus = 12.5%
        assert!((rows[1].1 - 12.5).abs() < 0.01, "PID 2 expected 12.5%, got {}", rows[1].1);
        // PID 3: no prev sample, cpu = 0
        assert_eq!(rows[2].1, 0.0);
    }

    #[test]
    fn test_zero_total_memory_returns_zero_percent() {
        let curr = vec![make_snapshot(1, 0, 2000, 1_000_000, 0, 0, 0, 0, 0, 0, 0)];
        let rows = compute_usage_from_snapshots(&[], &curr, 0, PAGE_SIZE, TICKS_PER_SEC, NUM_CPUS);

        assert_eq!(rows[0].2, 0.0, "memory percent should be 0 when total memory is 0");
        let mem_mb = rows[0].3;
        assert!(mem_mb > 0.0, "memory MB should still be computed from RSS");
    }
}
