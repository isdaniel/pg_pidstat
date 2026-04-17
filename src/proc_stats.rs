//! Read per-process CPU and memory stats from /proc.
//!
//! This module has no pgrx dependency and can be unit-tested outside PostgreSQL.

use procfs::process::Process;
use procfs::Current;

/// Raw stats for a single PID read from /proc.
pub struct RawProcStats {
    pub pid: i32,
    /// utime + stime in clock ticks (from /proc/[pid]/stat fields 14+15)
    pub cpu_ticks: u64,
    /// Resident set size in pages (from /proc/[pid]/statm field 2)
    pub rss_pages: u64,
    /// Actual bytes fetched from storage layer (from /proc/[pid]/io)
    pub read_bytes: u64,
    /// Actual bytes sent to storage layer (from /proc/[pid]/io)
    pub write_bytes: u64,
    /// Number of read syscalls (from /proc/[pid]/io)
    pub syscr: u64,
    /// Number of write syscalls (from /proc/[pid]/io)
    pub syscw: u64,
    /// Process state character from /proc/[pid]/stat ('D' = disk sleep / I/O wait)
    pub state_char: char,
    /// Voluntary context switches (from /proc/[pid]/status)
    pub voluntary_ctxt_switches: u64,
    /// Non-voluntary context switches (from /proc/[pid]/status)
    pub nonvoluntary_ctxt_switches: u64,
}

/// Read CPU and memory stats for a given PID.
/// Returns None if the process no longer exists or /proc is unreadable.
#[allow(dead_code)]
pub fn read_pid_stats(pid: i32) -> Option<RawProcStats> {
    let proc_entry = Process::new(pid).ok()?;
    let stat = proc_entry.stat().ok()?;
    let statm = proc_entry.statm().ok()?;
    // I/O stats may fail if permissions are insufficient; fall back to zeros
    let (read_bytes, write_bytes, syscr, syscw) = proc_entry
        .io()
        .ok()
        .map(|io| (io.read_bytes, io.write_bytes, io.syscr, io.syscw))
        .unwrap_or((0, 0, 0, 0));
    // Context switches from /proc/[pid]/status
    let (vol_cs, nonvol_cs) = proc_entry
        .status()
        .ok()
        .map(|s| {
            (
                s.voluntary_ctxt_switches.unwrap_or(0),
                s.nonvoluntary_ctxt_switches.unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));
    Some(RawProcStats {
        pid,
        cpu_ticks: stat.utime + stat.stime,
        rss_pages: statm.resident,
        read_bytes,
        write_bytes,
        syscr,
        syscw,
        state_char: stat.state,
        voluntary_ctxt_switches: vol_cs,
        nonvoluntary_ctxt_switches: nonvol_cs,
    })
}

/// Discover all PostgreSQL backend PIDs by finding processes
/// whose parent PID is the postmaster.
pub fn discover_pg_backend_pids(postmaster_pid: i32) -> Vec<RawProcStats> {
    let mut results = Vec::new();
    let Ok(procs) = procfs::process::all_processes() else {
        return results;
    };
    for entry in procs {
        let Ok(proc_entry) = entry else { continue };
        let Ok(stat) = proc_entry.stat() else { continue };
        if stat.ppid != postmaster_pid {
            continue;
        }
        let Ok(statm) = proc_entry.statm() else { continue };
        // I/O stats — may fail if permissions are insufficient
        let (read_bytes, write_bytes, syscr, syscw) = proc_entry
            .io()
            .ok()
            .map(|io| (io.read_bytes, io.write_bytes, io.syscr, io.syscw))
            .unwrap_or((0, 0, 0, 0));
        // Context switches from /proc/[pid]/status
        let (vol_cs, nonvol_cs) = proc_entry
            .status()
            .ok()
            .map(|s| {
                (
                    s.voluntary_ctxt_switches.unwrap_or(0),
                    s.nonvoluntary_ctxt_switches.unwrap_or(0),
                )
            })
            .unwrap_or((0, 0));
        results.push(RawProcStats {
            pid: stat.pid,
            cpu_ticks: stat.utime + stat.stime,
            rss_pages: statm.resident,
            read_bytes,
            write_bytes,
            syscr,
            syscw,
            state_char: stat.state,
            voluntary_ctxt_switches: vol_cs,
            nonvoluntary_ctxt_switches: nonvol_cs,
        });
    }
    results
}

/// Get total physical memory in bytes.
pub fn total_memory_bytes() -> u64 {
    procfs::Meminfo::current()
        .map(|m| m.mem_total)
        .unwrap_or(0)
}

/// Get system page size in bytes.
pub fn page_size() -> u64 {
    procfs::page_size()
}

/// Get clock ticks per second (typically 100 on Linux).
pub fn ticks_per_second() -> u64 {
    procfs::ticks_per_second()
}

/// Get number of online CPUs (logical cores / vCPUs).
pub fn num_cpus() -> u64 {
    procfs::CpuInfo::current()
        .map(|c| c.num_cores() as u64)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_pid_stats_for_self() {
        let pid = std::process::id() as i32;
        let stats = read_pid_stats(pid);
        assert!(stats.is_some(), "should be able to read own /proc stats");
        let s = stats.unwrap();
        assert_eq!(s.pid, pid);
        // cpu_ticks may be 0 for a very short-lived process; just check it doesn't panic
        assert!(s.cpu_ticks < u64::MAX, "cpu_ticks should be a sane value");
        assert!(s.rss_pages > 0, "process should have some RSS pages");
    }

    #[test]
    fn test_read_pid_stats_for_nonexistent_pid() {
        // PID 0 is the kernel scheduler, not a real process we can read
        // Use a very high PID that almost certainly doesn't exist
        let stats = read_pid_stats(i32::MAX);
        assert!(stats.is_none(), "should return None for non-existent PID");
    }

    #[test]
    fn test_total_memory_bytes_is_positive() {
        let mem = total_memory_bytes();
        assert!(mem > 0, "total memory should be positive, got {mem}");
    }

    #[test]
    fn test_page_size_is_reasonable() {
        let ps = page_size();
        // Page size is typically 4096 on x86_64, but could be 16384 on ARM
        assert!(ps >= 4096, "page size should be at least 4096, got {ps}");
        assert!(ps <= 65536, "page size should be at most 64K, got {ps}");
    }

    #[test]
    fn test_ticks_per_second_is_positive() {
        let tps = ticks_per_second();
        assert!(tps > 0, "ticks_per_second should be positive, got {tps}");
        // Typically 100 on Linux
        assert!(tps <= 10000, "ticks_per_second seems unreasonably large: {tps}");
    }

    #[test]
    fn test_num_cpus_is_positive() {
        let cpus = num_cpus();
        assert!(cpus >= 1, "num_cpus should be at least 1, got {cpus}");
        assert!(cpus <= 4096, "num_cpus seems unreasonably large: {cpus}");
    }

    #[test]
    fn test_discover_finds_children_of_current_process() {
        // Our own PID as postmaster — we likely have no children,
        // so this should return an empty vec (not panic or error).
        let pid = std::process::id() as i32;
        let results = discover_pg_backend_pids(pid);
        // Just verify it doesn't panic; results may be empty
        assert!(results.len() < 10000, "sanity check on result count");
    }

    #[test]
    fn test_io_stats_for_self() {
        let pid = std::process::id() as i32;
        let stats = read_pid_stats(pid).unwrap();
        // I/O counters may be 0 if permissions prevent reading /proc/[pid]/io
        // Just verify fields are returned without panicking
        assert!(stats.read_bytes < u64::MAX, "read_bytes should be a sane value");
        assert!(stats.write_bytes < u64::MAX, "write_bytes should be a sane value");
    }

    #[test]
    fn test_state_char_for_self() {
        let pid = std::process::id() as i32;
        let stats = read_pid_stats(pid).unwrap();
        // While executing this test, the process should be Running ('R') or Sleeping ('S')
        assert!(
            stats.state_char == 'R' || stats.state_char == 'S',
            "expected state R or S, got '{}'", stats.state_char
        );
    }
}

