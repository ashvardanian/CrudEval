//! Configuration reports, machine information, and atomic JSON output.

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::Serialize;
use serde_json::Value;

use crate::{
    measure::{LatencySummary, ResourceUsage},
    BackendCapabilities, CommonArgs, Result,
};

#[derive(Serialize)]
pub struct WorkloadReport {
    pub workload: String,
    pub status: String,
    pub elapsed_seconds: f64,
    pub flush_seconds: f64,
    pub calls: u64,
    pub entries: u64,
    pub throughput: f64,
    pub missing: u64,
    pub failed: u64,
    pub aborted: u64,
    pub corrupted: u64,
    pub processed_bytes: u64,
    pub latency: BTreeMap<String, LatencySummary>,
    #[serde(serialize_with = "serialize_timeline")]
    pub timeline_entries: Vec<u64, System>,
    pub timeline_buffer_growths: u64,
    pub client_usage: ResourceUsage,
    pub server_usage: Option<Value>,
    pub hardware_counters: Option<crate::perf_counters::CounterSample>,
    pub disk_bytes: u64,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct MachineInfo {
    pub cpu_model: String,
    pub logical_cores: usize,
    pub physical_cores: Option<usize>,
    pub ram_bytes: u64,
    pub kernel: Option<String>,
    pub os: Option<String>,
    pub architecture: &'static str,
    pub disks: Vec<DiskInfo>,
}

#[derive(Serialize)]
pub struct DiskInfo {
    pub name: String,
    pub mount_point: PathBuf,
    pub file_system: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Serialize)]
pub struct ConfigReport {
    pub schema_version: u32,
    pub started_unix_seconds: u64,
    pub machine: MachineInfo,
    pub workload: CommonArgs,
    #[serde(serialize_with = "serialize_config")]
    pub config: BTreeMap<String, Value>,
    pub capabilities: BackendCapabilities,
    pub phases: Vec<WorkloadReport>,
}

pub fn collect_machine_info() -> MachineInfo {
    let system = sysinfo::System::new_all();
    let disks = sysinfo::Disks::new_with_refreshed_list();
    MachineInfo {
        cpu_model: system.cpus().first().map(|c| c.brand().to_owned()).unwrap_or_default(),
        logical_cores: system.cpus().len(),
        physical_cores: sysinfo::System::physical_core_count(),
        ram_bytes: system.total_memory(),
        kernel: sysinfo::System::kernel_version(),
        os: sysinfo::System::long_os_version(),
        architecture: std::env::consts::ARCH,
        disks: disks
            .iter()
            .map(|disk| DiskInfo {
                name: disk.name().to_string_lossy().into_owned(),
                mount_point: disk.mount_point().to_path_buf(),
                file_system: disk.file_system().to_string_lossy().into_owned(),
                total_bytes: disk.total_space(),
                available_bytes: disk.available_space(),
            })
            .collect(),
    }
}

pub fn config_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash = (hash ^ byte as u64).wrapping_mul(0x100000001b3);
    }
    Ok(format!("{hash:016x}"))
}

pub fn write_report(directory: &Path, report: &ConfigReport) -> Result<PathBuf> {
    std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
    let backend = report
        .config
        .get("backend")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let hash = config_hash(&(
        &report.workload,
        report.config.iter().collect::<std::collections::BTreeMap<_, _>>(),
    ))?;
    let path = directory.join(format!("{backend}-{hash}.json"));
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let file = std::fs::File::create(&temporary).map_err(|e| e.to_string())?;
    serde_json::to_writer_pretty(&file, report).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(&temporary, &path).map_err(|e| e.to_string())?;
    Ok(path)
}

fn serialize_config<S: serde::Serializer>(
    value: &BTreeMap<String, Value>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (key, value) in value {
        map.serialize_entry(key, value)?;
    }
    map.end()
}

fn serialize_timeline<S: serde::Serializer>(
    value: &Vec<u64, System>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    value.as_slice().serialize(serializer)
}
