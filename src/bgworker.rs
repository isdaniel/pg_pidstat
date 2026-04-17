//! Background worker that periodically samples /proc stats for all PostgreSQL backends.

use crate::proc_stats;
use crate::shmem::{PidSnapshot, USAGE_DATA, MAX_BACKENDS};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Name of the background worker as it appears in pg_stat_activity.
const BGW_NAME: &str = "pg_pidstat sampler";

/// Name of the shared library (must match the crate name).
const BGW_LIBRARY: &str = "pg_pidstat";

/// Name of the entry point function.
const BGW_FUNCTION: &str = "pg_pidstat_bgw_main";

/// Default sampling interval in milliseconds.
const DEFAULT_SAMPLE_INTERVAL_MS: i64 = 1000;

/// Register the background worker during _PG_init.
///
/// # Safety
/// Must be called during shared_preload_libraries initialization.
pub unsafe fn register_background_worker() {
    let mut bgw: pg_sys::BackgroundWorker = std::mem::zeroed();

    // Only need shared memory access (no database connection)
    bgw.bgw_flags = pg_sys::BGWORKER_SHMEM_ACCESS as _;
    bgw.bgw_start_time = pg_sys::BgWorkerStartTime::BgWorkerStart_PostmasterStart;
    bgw.bgw_restart_time = 10; // restart after 10s if crashed

    copy_to_carray(&mut bgw.bgw_library_name, BGW_LIBRARY);
    copy_to_carray(&mut bgw.bgw_function_name, BGW_FUNCTION);
    copy_to_carray(&mut bgw.bgw_name, BGW_NAME);
    copy_to_carray(&mut bgw.bgw_type, BGW_NAME);

    pg_sys::RegisterBackgroundWorker(&mut bgw);
}

/// Copy a Rust string into a fixed-size C character array.
fn copy_to_carray(dest: &mut [std::os::raw::c_char], src: &str) {
    let bytes = src.as_bytes();
    let len = std::cmp::min(bytes.len(), dest.len() - 1);
    for (i, &b) in bytes[..len].iter().enumerate() {
        dest[i] = b as std::os::raw::c_char;
    }
    dest[len] = 0;
}

/// Get current time in microseconds since UNIX epoch.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros() as u64
}

/// Background worker entry point.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn pg_pidstat_bgw_main(_main_arg: pg_sys::Datum) {
    // Set up signal handlers
    #[cfg(not(feature = "pg18"))]
    {
        pg_sys::pqsignal(
            pg_sys::SIGTERM as _,
            as_pqsigfunc(pg_sys::die),
        );
        pg_sys::pqsignal(
            pg_sys::SIGHUP as _,
            as_pqsigfunc(pg_sys::SignalHandlerForConfigReload),
        );
    }
    #[cfg(feature = "pg18")]
    {
        pg_sys::pqsignal_be(
            pg_sys::SIGTERM as _,
            as_pqsigfunc(pg_sys::die),
        );
        pg_sys::pqsignal_be(
            pg_sys::SIGHUP as _,
            as_pqsigfunc(pg_sys::SignalHandlerForConfigReload),
        );
    }
    pg_sys::BackgroundWorkerUnblockSignals();

    let postmaster_pid = pg_sys::PostmasterPid;

    // Main loop
    loop {
        pgrx::check_for_interrupts!();

        // Sample all backend PIDs from /proc
        let sample_time = now_us();
        let stats = proc_stats::discover_pg_backend_pids(postmaster_pid);

        // Update shared memory
        {
            let mut data = USAGE_DATA.exclusive();

            // Initialize system constants on first iteration
            if data.total_memory_bytes == 0 {
                data.total_memory_bytes = proc_stats::total_memory_bytes();
                data.page_size = proc_stats::page_size();
                data.ticks_per_second = proc_stats::ticks_per_second();
                data.num_cpus = proc_stats::num_cpus();
            }

            // Rotate: curr -> prev
            data.prev = data.curr;
            data.prev_count = data.curr_count;

            // Write new current snapshots
            let count = stats.len().min(MAX_BACKENDS);
            for (i, s) in stats.iter().enumerate().take(count) {
                data.curr[i] = PidSnapshot {
                    pid: s.pid,
                    cpu_ticks: s.cpu_ticks,
                    rss_pages: s.rss_pages,
                    sample_time_us: sample_time,
                    read_bytes: s.read_bytes,
                    write_bytes: s.write_bytes,
                    syscr: s.syscr,
                    syscw: s.syscw,
                    io_wait: if s.state_char == 'D' { 1 } else { 0 },
                    voluntary_ctxt_switches: s.voluntary_ctxt_switches,
                    nonvoluntary_ctxt_switches: s.nonvoluntary_ctxt_switches,
                };
            }
            data.curr_count = count;
        }

        // Wait for the sampling interval or a signal
        let rc = pg_sys::WaitLatch(
            pg_sys::MyLatch,
            (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as _,
            DEFAULT_SAMPLE_INTERVAL_MS,
            pg_sys::PG_WAIT_EXTENSION,
        );

        pg_sys::ResetLatch(pg_sys::MyLatch);

        // Exit if postmaster died
        if (rc as u32) & pg_sys::WL_POSTMASTER_DEATH != 0 {
            pg_sys::proc_exit(1);
        }

        // Handle config reload
        if pg_sys::ConfigReloadPending != 0 {
            pg_sys::ConfigReloadPending = 0;
            pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
        }
    }
}

/// Cast a Rust `unsafe fn(c_int)` to a `pqsigfunc`.
#[inline]
unsafe fn as_pqsigfunc(
    f: unsafe fn(std::os::raw::c_int),
) -> pg_sys::pqsigfunc {
    Some(std::mem::transmute::<
        unsafe fn(std::os::raw::c_int),
        unsafe extern "C-unwind" fn(std::os::raw::c_int),
    >(f))
}
