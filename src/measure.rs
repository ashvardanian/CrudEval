//! Worker-local latency histograms and sampled process resource usage.

use std::{
    alloc::System,
    collections::BTreeMap,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use serde::Serialize;
use sysinfo::{ProcessesToUpdate, System as ProcessSystem};

use crate::{workload::Operation, Result};

#[derive(Default, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

pub struct WorkloadMeasurements {
    pub elapsed: Duration,
    pub calls: u64,
    pub entries: u64,
    pub missing: u64,
    pub failed: u64,
    pub aborted: u64,
    pub corrupted: u64,
    pub processed_bytes: u64,
    pub histograms: [Option<Histogram<u64>>; 9],
    pub timeline_buffer_growths: u64,
    pub timeline: Vec<u64, System>,
    pub error: Option<String>,
    pub hardware_counters: Option<crate::perf_counters::CounterSample>,
}

impl Default for WorkloadMeasurements {
    fn default() -> Self {
        Self {
            elapsed: Duration::ZERO,
            calls: 0,
            entries: 0,
            missing: 0,
            failed: 0,
            aborted: 0,
            corrupted: 0,
            processed_bytes: 0,
            histograms: std::array::from_fn(|_| None),
            timeline_buffer_growths: 0,
            timeline: Vec::new_in(System),
            error: None,
            hardware_counters: None,
        }
    }
}
impl WorkloadMeasurements {
    /// Passes `verified` through, counting a failed verification as corruption.
    pub fn count_corruption(&mut self, verified: Result<()>) -> Result<()> {
        self.corrupted += u64::from(verified.is_err());
        verified
    }
    pub fn new(
        operations: impl IntoIterator<Item = Operation>,
        transaction: bool,
        timeline_seconds: usize,
    ) -> Result<Self> {
        let mut result = Self::default();
        for operation in operations {
            result.histograms[operation as usize] =
                Some(Histogram::new_with_max(u64::MAX, 3).map_err(|e| e.to_string())?);
        }
        if transaction {
            result.histograms[8] = Some(Histogram::new_with_max(u64::MAX, 3).map_err(|e| e.to_string())?);
        }
        result
            .timeline
            .try_reserve(timeline_seconds)
            .map_err(|e| e.to_string())?;
        Ok(result)
    }
    fn record_index(&mut self, index: usize, latency: Duration) -> Result<()> {
        self.histograms[index]
            .as_mut()
            .ok_or("unprepared latency histogram")?
            .record(latency.as_nanos().min(u64::MAX as u128) as u64)
            .map_err(|e| e.to_string())?;
        self.calls += 1;
        Ok(())
    }
    pub fn record_attempt(&mut self, operation: Operation, latency: Duration) -> Result<()> {
        self.record_index(operation as usize, latency)
    }
    pub fn record_commit(&mut self, latency: Duration) -> Result<()> {
        self.record_index(8, latency)
    }
    pub fn publish(&mut self, elapsed: Duration, entries: u64, missing: u64, bytes: u64) {
        self.entries += entries;
        self.missing += missing;
        self.processed_bytes += bytes;
        let second = elapsed.as_secs() as usize;
        if second >= self.timeline.capacity() {
            self.timeline_buffer_growths += 1;
        }
        self.timeline.resize(self.timeline.len().max(second + 1), 0);
        self.timeline[second] += entries;
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.elapsed = self.elapsed.max(other.elapsed);
        self.calls += other.calls;
        self.entries += other.entries;
        self.missing += other.missing;
        self.failed += other.failed;
        self.aborted += other.aborted;
        self.corrupted += other.corrupted;
        self.processed_bytes += other.processed_bytes;
        for (existing, histogram) in self.histograms.iter_mut().zip(other.histograms) {
            if let Some(histogram) = histogram {
                if let Some(existing) = existing {
                    existing.add(&histogram).map_err(|e| e.to_string())?;
                } else {
                    *existing = Some(histogram);
                }
            }
        }
        self.timeline_buffer_growths += other.timeline_buffer_growths;
        self.timeline.resize(self.timeline.len().max(other.timeline.len()), 0);
        for (second, count) in other.timeline.into_iter().enumerate() {
            self.timeline[second] += count;
        }
        if self.error.is_none() {
            self.error = other.error;
        }
        if let Some(sample) = other.hardware_counters {
            let total = self.hardware_counters.get_or_insert_with(Default::default);
            total.cycles += sample.cycles;
            total.instructions += sample.instructions;
            total.cache_misses += sample.cache_misses;
            total.branch_misses += sample.branch_misses;
        }
        Ok(())
    }

    pub fn latencies(&self) -> BTreeMap<String, LatencySummary> {
        const NAMES: [&str; 9] = [
            "insert",
            "read",
            "update",
            "delete",
            "range-read",
            "read-modify-write",
            "bulk-load",
            "full-scan",
            "commit",
        ];
        self.histograms
            .iter()
            .enumerate()
            .filter_map(|(index, h)| {
                h.as_ref().filter(|h| !h.is_empty()).map(|h| {
                    (
                        NAMES[index].to_string(),
                        LatencySummary {
                            count: h.len(),
                            p50_ns: h.value_at_quantile(0.5),
                            p90_ns: h.value_at_quantile(0.9),
                            p99_ns: h.value_at_quantile(0.99),
                            p999_ns: h.value_at_quantile(0.999),
                            max_ns: h.max(),
                        },
                    )
                })
            })
            .collect()
    }
}

#[derive(Default, Serialize)]
pub struct ResourceUsage {
    pub cpu_seconds: f64,
    pub cpu_avg_percent: f64,
    pub cpu_max_percent: f32,
    pub rss_avg_bytes: u64,
    pub rss_max_bytes: u64,
    pub virtual_memory_avg_bytes: u64,
    pub virtual_memory_max_bytes: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
}

pub struct ResourceSampler {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<ResourceUsage>,
}

impl ResourceSampler {
    pub fn start() -> Self {
        let (stop, receiver) = mpsc::channel();
        let (ready, initialized) = mpsc::sync_channel(0);
        let handle = std::thread::spawn(move || {
            let pid = sysinfo::get_current_pid().unwrap();
            let mut system = ProcessSystem::new();
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
            let initial = system.process(pid).map(|p| (p.accumulated_cpu_time(), p.disk_usage()));
            let start = Instant::now();
            let _ = ready.send(());
            let mut usage = ResourceUsage::default();
            let mut samples = 0;
            let mut rss_sum = 0u128;
            let mut virtual_sum = 0u128;
            loop {
                let stopped = !matches!(
                    receiver.recv_timeout(Duration::from_millis(100)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                );
                system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
                if let Some(process) = system.process(pid) {
                    samples += 1;
                    rss_sum += process.memory() as u128;
                    virtual_sum += process.virtual_memory() as u128;
                    usage.rss_max_bytes = usage.rss_max_bytes.max(process.memory());
                    usage.virtual_memory_max_bytes = usage.virtual_memory_max_bytes.max(process.virtual_memory());
                    usage.cpu_max_percent = usage.cpu_max_percent.max(process.cpu_usage());
                    if let Some((cpu, disk)) = &initial {
                        usage.cpu_seconds = process.accumulated_cpu_time().saturating_sub(*cpu) as f64 / 1000.0;
                        usage.read_bytes = process
                            .disk_usage()
                            .total_read_bytes
                            .saturating_sub(disk.total_read_bytes);
                        usage.written_bytes = process
                            .disk_usage()
                            .total_written_bytes
                            .saturating_sub(disk.total_written_bytes);
                    }
                }
                if stopped {
                    break;
                }
            }
            usage.cpu_avg_percent = usage.cpu_seconds / start.elapsed().as_secs_f64() * 100.0;
            usage.rss_avg_bytes = (rss_sum / samples.max(1) as u128) as u64;
            usage.virtual_memory_avg_bytes = (virtual_sum / samples.max(1) as u128) as u64;
            usage
        });
        let _ = initialized.recv();
        Self { stop, handle }
    }

    pub fn stop(self) -> Result<ResourceUsage> {
        let _ = self.stop.send(());
        self.handle.join().map_err(|_| "resource sampler panicked".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_measurements_reuse_histograms_and_timeline() {
        let mut measurements = WorkloadMeasurements::new([Operation::Read], true, 3).unwrap();
        crate::backend::tests::assert_no_global_allocations(|| {
            for _ in 0..100 {
                measurements
                    .record_attempt(Operation::Read, Duration::from_micros(10))
                    .unwrap();
                measurements.record_commit(Duration::from_micros(20)).unwrap();
                measurements.publish(Duration::from_secs(1), 1, 0, 32);
            }
        });
        assert_eq!(measurements.calls, 200);
        assert_eq!(measurements.entries, 100);
        assert_eq!(measurements.timeline_buffer_growths, 0);
        assert_eq!(measurements.timeline.as_slice(), &[0, 100]);
    }
}
