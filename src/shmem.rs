//! Shared memory types for storing per-PID resource usage snapshots.

use pgrx::lwlock::PgLwLock;
use pgrx::shmem::PGRXSharedMemory;

/// Maximum number of backends we can track.
/// Should be >= max_connections + autovacuum_max_workers + other backend slots.
pub const MAX_BACKENDS: usize = 1024;

/// A single snapshot of a process's resource usage at a point in time.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct PidSnapshot {
    pub pid: i32,
    /// utime + stime in clock ticks
    pub cpu_ticks: u64,
    /// Resident set size in pages
    pub rss_pages: u64,
    /// Timestamp of this sample in microseconds since UNIX epoch
    pub sample_time_us: u64,
    /// Actual bytes fetched from storage layer
    pub read_bytes: u64,
    /// Actual bytes sent to storage layer
    pub write_bytes: u64,
    /// Number of read syscalls
    pub syscr: u64,
    /// Number of write syscalls
    pub syscw: u64,
    /// 1 if process is in disk sleep (I/O wait), 0 otherwise
    pub io_wait: u8,
    /// Voluntary context switches
    pub voluntary_ctxt_switches: u64,
    /// Non-voluntary context switches
    pub nonvoluntary_ctxt_switches: u64,
}

impl Default for PidSnapshot {
    fn default() -> Self {
        Self {
            pid: 0,
            cpu_ticks: 0,
            rss_pages: 0,
            sample_time_us: 0,
            read_bytes: 0,
            write_bytes: 0,
            syscr: 0,
            syscw: 0,
            io_wait: 0,
            voluntary_ctxt_switches: 0,
            nonvoluntary_ctxt_switches: 0,
        }
    }
}

// Safety: all fields are Copy primitives, no heap allocations, no pointers.
unsafe impl PGRXSharedMemory for PidSnapshot {}

/// The shared memory region. Contains two snapshot buffers for delta computation.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct UsageData {
    pub prev: [PidSnapshot; MAX_BACKENDS],
    pub prev_count: usize,
    pub curr: [PidSnapshot; MAX_BACKENDS],
    pub curr_count: usize,
    /// Total system memory in bytes (read once, cached)
    pub total_memory_bytes: u64,
    /// System page size in bytes
    pub page_size: u64,
    /// Clock ticks per second
    pub ticks_per_second: u64,
    /// Number of online CPUs (logical cores / vCPUs)
    pub num_cpus: u64,
}

impl Default for UsageData {
    fn default() -> Self {
        Self {
            prev: [PidSnapshot::default(); MAX_BACKENDS],
            prev_count: 0,
            curr: [PidSnapshot::default(); MAX_BACKENDS],
            curr_count: 0,
            total_memory_bytes: 0,
            page_size: 0,
            ticks_per_second: 0,
            num_cpus: 0,
        }
    }
}

// Safety: all fields are Copy, fixed-size arrays of Copy types, no pointers, no heap.
unsafe impl PGRXSharedMemory for UsageData {}

/// Global shared memory region protected by a PostgreSQL LWLock.
pub static USAGE_DATA: PgLwLock<UsageData> = unsafe { PgLwLock::new(c"pg_pidstat") };
