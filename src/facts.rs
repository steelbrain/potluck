//! Detecting this machine's facts: OS and version, architecture, kernel, CPU model and count, RAM,
//! GPUs, whether a display is up. GPUs: on Linux from /sys/class/drm/renderD*/device (vendor,
//! device, PCI slot, driver, name from the PCI ids or nvidia-smi), plus each NVIDIA GPU in
//! /proc/driver/nvidia/gpus that has no render node (nvidia-drm not loaded); on macOS `mac0` for
//! Apple silicon, each GPU in turn on an Intel Mac. The node config's `[[gpu]]` entries add
//! aliases. Facts are read again periodically, since a GPU mux can change which GPUs exist.
//!
//! Detection is cheap: files under /sys and /proc, sysctl, and on Linux at most one run of
//! nvidia-smi and, for each GPU still unnamed, one of lspci (once in a process), or on Intel Macs
//! one of system_profiler. A tool that takes longer than 20 s is killed, and detection goes on
//! without it. One the kill can't end (a wedged GPU driver) isn't started again until it has
//! ended.
//!
//! - Linux GPU names: the NVIDIA driver's `/proc/driver/nvidia/gpus/<slot>/information` (read
//!   without waking a sleeping GPU), else `nvidia-smi`, else `lspci` of that GPU's slot alone
//!   (listing every device would wake a sleeping GPU too), else `vendor:device`. Render nodes
//!   without a PCI device behind them are not listed.
//! - Linux display: the session logind elects as the user's display (`loginctl show-user -p
//!   Display`) is graphical (x11, wayland or mir; not a tty, such as an ssh login), or a Wayland
//!   socket exists in the user's runtime directory.
//! - macOS display: the user owns `/dev/console`, i.e. is logged in at the screen.
//!
//! On Windows nothing is run: the version, build and CPU model come from the registry, RAM from
//! `GlobalMemoryStatusEx`, the architecture from `IsWow64Process2` (the machine's own, also for
//! an x86_64 build emulated on ARM64), and the GPUs from the display adapters PnP lists
//! (SetupAPI, which reads what the system recorded and wakes no GPU): each PCI one, its vendor
//! and device from its hardware id (`PCI\VEN_10DE&DEV_2C02&...`), its slot from its PCI bus,
//! device and function (`0000:01:00.0`, domain 0), so its id reads like Linux's
//! (`10de:2c02@0000:01:00.0`), its name the device description and its driver the driver's
//! service name (`nvlddmkm`, `amdkmdag`, `BasicDisplay`). Display: an active session (at the
//! console or over Remote Desktop) of this user.

use crate::config::{NodeConfig, Paths};
use crate::types::{Facts, Gpu, gpu_matches};
use std::collections::BTreeMap;
#[cfg(unix)]
use std::process::{Command, Stdio};

/// The Windows build number (`26100`), from the registry.
#[cfg(windows)]
pub(crate) fn windows_build() -> Option<u32> {
    platform::build()
}

pub fn detect(paths: &Paths, config: &NodeConfig) -> anyhow::Result<Facts> {
    let mut facts = platform::detect()?;
    facts.node = node_name(paths, config);
    facts.version = crate::VERSION.to_string();
    apply_gpu_config(&mut facts.gpus, config);
    Ok(facts)
}

/// This machine's name in the cluster: the config's `name`, else the name this machine's records
/// give it (its daemon has run), else the short hostname. The OS may change the hostname under a
/// running machine (DHCP, a name taken on the network); the machine keeps its name.
pub fn node_name(paths: &Paths, config: &NodeConfig) -> String {
    if let Some(n) = config.name.as_deref().filter(|n| !n.is_empty()) {
        return n.to_string();
    }
    if let Some((had, _)) = crate::mesh::recorded_name(&paths.keys_dir()) {
        return had;
    }
    let host = crate::sys::hostname();
    let short = host.split('.').next().unwrap_or("").trim();
    if short.is_empty() {
        "localhost".into()
    } else {
        short.to_string()
    }
}

/// Sets each GPU's alias from the `[[gpu]]` entries whose selector names exactly that one GPU.
pub fn apply_gpu_config(gpus: &mut [Gpu], config: &NodeConfig) {
    for entry in &config.gpu {
        let Some(alias) = &entry.alias else { continue };
        let hits: Vec<usize> = (0..gpus.len())
            .filter(|&i| gpu_matches(&entry.matches, &gpus[i]))
            .collect();
        if let [i] = hits[..] {
            gpus[i].alias = Some(alias.clone());
        }
    }
}

/// Facts a daemon before a restart in place handed over, made to say what this binary and
/// `config` (node.toml as it is now) say, as [`detect`] would: the machine's name, the version
/// and the GPUs' aliases. The rest is detected again in the background.
pub fn as_configured(facts: &mut Facts, paths: &Paths, config: &NodeConfig) {
    facts.node = node_name(paths, config);
    facts.version = crate::VERSION.to_string();
    for g in &mut facts.gpus {
        g.alias = None;
    }
    apply_gpu_config(&mut facts.gpus, config);
}

/// `[[gpu]]` entries that don't name exactly one of `gpus`, in plain words (for the daemon's log;
/// a GPU mux can hide a GPU, so this is not an error).
pub fn gpu_config_problems(gpus: &[Gpu], config: &NodeConfig) -> Vec<String> {
    config
        .gpu
        .iter()
        .filter_map(|entry| {
            let n = gpus
                .iter()
                .filter(|g| gpu_matches(&entry.matches, g))
                .count();
            match n {
                1 => None,
                0 => Some(format!(
                    "[[gpu]] match = {:?} names no GPU here",
                    entry.matches
                )),
                n => Some(format!(
                    "[[gpu]] match = {:?} names {n} GPUs here; it must name one",
                    entry.matches
                )),
            }
        })
        .collect()
}

/// Runs a command and returns its stdout when it succeeded; `None` when it is missing or failed.
#[cfg(unix)]
fn output(prog: &str, args: &[&str]) -> Option<String> {
    output_within(prog, args, TOOL_TIME_LIMIT)
}

/// How long [`output`] waits for a tool. nvidia-smi can take seconds to wake a GPU and
/// system_profiler a few more on a busy Mac, but one that hangs (a wedged GPU driver) would hold
/// up the daemon's start, every later refresh and a nested dispatch: at the limit it is killed and
/// detection goes on without it.
#[cfg(unix)]
const TOOL_TIME_LIMIT: std::time::Duration = std::time::Duration::from_secs(20);

/// Tools, each with its arguments, whose run [`output_within`] gave up on and that hasn't ended:
/// its process unreaped (SIGKILL can't end a process in an uninterruptible wait, such as
/// nvidia-smi on a wedged GPU) or its output still open. Such a tool isn't started again until
/// that run ends, so it leaves one process and two threads behind, not more at every refresh.
#[cfg(unix)]
static GIVEN_UP: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

#[cfg(unix)]
fn given_up() -> std::sync::MutexGuard<'static, std::collections::BTreeSet<String>> {
    GIVEN_UP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// [`output`] with a time limit: past `limit` the tool is killed, reaped on a thread of its own,
/// and `None` returned. Its stdout is read on another thread, so a full pipe can't stall it. A tool
/// whose earlier run was given up on and hasn't ended yet isn't started: `None` at once.
#[cfg(unix)]
fn output_within(prog: &str, args: &[&str], limit: std::time::Duration) -> Option<String> {
    use std::io::Read;
    let key = std::iter::once(prog)
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join("\0");
    if given_up().contains(&key) {
        return None;
    }
    let mut child = Command::new(prog)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let read = stdout.read_to_end(&mut buf).map(|_| buf);
        let _ = tx.send(read);
    });
    // The run is given up on: until the tool is reaped and its output closed, it isn't started
    // again.
    let give_up = |child: Option<std::process::Child>, reader: std::thread::JoinHandle<()>| {
        given_up().insert(key.clone());
        let key = key.clone();
        std::thread::spawn(move || {
            if let Some(mut child) = child {
                let _ = child.wait();
            }
            let _ = reader.join();
            given_up().remove(&key);
        });
    };
    let deadline = std::time::Instant::now() + limit;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            _ => {
                // Not reaped yet, so the pid is still this child's.
                let _ = child.kill();
                give_up(Some(child), reader);
                return None;
            }
        }
    };
    // The tool has ended; what it started may still hold the pipe open.
    let left = deadline.saturating_duration_since(std::time::Instant::now());
    let bytes = match rx.recv_timeout(left) {
        Ok(read) => read.ok()?,
        Err(_) => {
            give_up(None, reader);
            return None;
        }
    };
    status
        .success()
        .then(|| String::from_utf8_lossy(&bytes).into_owned())
}

/// `uname`'s release and machine, the machine as `x86_64` or `aarch64`.
#[cfg(unix)]
fn uname() -> (String, String) {
    // SAFETY: utsname is plain old data; uname fills it with NUL-terminated strings.
    let mut u: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut u) } != 0 {
        return (String::new(), std::env::consts::ARCH.to_string());
    }
    let field = |f: &[libc::c_char]| {
        let bytes: Vec<u8> = f
            .iter()
            .take_while(|&&c| c != 0)
            // c_char is i8 on some targets and u8 on others (aarch64 Linux): `as u8` is a
            // needless cast there.
            .map(|&c| c.to_ne_bytes()[0])
            .collect();
        String::from_utf8_lossy(&bytes).into_owned()
    };
    (
        field(&u.release[..]),
        normalize_arch(&field(&u.machine[..])),
    )
}

pub fn normalize_arch(m: &str) -> String {
    match m {
        "arm64" | "aarch64" => "aarch64".into(),
        "amd64" | "x86_64" => "x86_64".into(),
        other => other.to_string(),
    }
}

/// `/etc/os-release` as a map, quotes removed.
pub fn parse_os_release(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| {
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(v);
            (k.trim().to_string(), v.replace("\\\"", "\""))
        })
        .collect()
}

/// The distribution and version from os-release: PRETTY_NAME, else NAME and VERSION_ID.
pub fn os_version_from(release: &BTreeMap<String, String>) -> String {
    if let Some(p) = release.get("PRETTY_NAME").filter(|p| !p.is_empty()) {
        return p.clone();
    }
    let name = release.get("NAME").map(String::as_str).unwrap_or("Linux");
    match release.get("VERSION_ID") {
        Some(v) => format!("{name} {v}"),
        None => name.to_string(),
    }
}

/// MemTotal from /proc/meminfo, in MiB.
pub fn parse_meminfo_mib(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024)
}

/// The CPU model and logical CPU count from /proc/cpuinfo.
pub fn parse_cpuinfo(text: &str) -> (String, u32) {
    let value = |key: &str| {
        text.lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim() == key)
            .map(|(_, v)| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let model = value("model name")
        .or_else(|| value("Hardware"))
        .or_else(|| value("Model"))
        .or_else(|| value("cpu model"))
        .unwrap_or_else(|| "unknown".into());
    let count = text
        .lines()
        .filter(|l| l.split(':').next().is_some_and(|k| k.trim() == "processor"))
        .count() as u32;
    (model, count)
}

/// A PCI bus id as `0000:01:00.0` (nvidia-smi prints an 8-digit domain). Lengths count
/// characters, so text that is no bus id (a tool's output can hold anything) is kept, not a panic.
pub fn normalize_slot(s: &str) -> String {
    let s = s.trim().to_ascii_lowercase();
    match s.split_once(':') {
        Some((domain, rest)) if domain.chars().count() > 4 => {
            let skip = domain.chars().count() - 4;
            let tail: String = domain.chars().skip(skip).collect();
            format!("{tail}:{rest}")
        }
        Some((domain, rest)) if domain.chars().count() < 4 && rest.contains(':') => {
            format!("{domain:0>4}:{rest}")
        }
        _ if s.matches(':').count() == 1 => format!("0000:{s}"),
        _ => s,
    }
}

/// The `Model:` line of the NVIDIA driver's per-GPU information file.
pub fn parse_nvidia_information(text: &str) -> Option<String> {
    text.lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim() == "Model")
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// `nvidia-smi --query-gpu=pci.bus_id,name --format=csv,noheader`: slot -> name.
pub fn parse_nvidia_smi(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| l.split_once(','))
        .map(|(bus, name)| (normalize_slot(bus), name.trim().to_string()))
        .collect()
}

/// `lspci -mm -D`: slot -> "vendor device" names.
pub fn parse_lspci(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        let mut fields = Vec::new();
        let mut rest = line.trim();
        while !rest.is_empty() {
            if let Some(r) = rest.strip_prefix('"') {
                let end = r.find('"').unwrap_or(r.len());
                fields.push(r[..end].to_string());
                rest = r.get(end + 1..).unwrap_or("").trim_start();
            } else {
                let end = rest.find(' ').unwrap_or(rest.len());
                fields.push(rest[..end].to_string());
                rest = rest[end..].trim_start();
            }
        }
        if fields.len() >= 4 {
            map.insert(
                normalize_slot(&fields[0]),
                format!("{} {}", fields[2], fields[3]),
            );
        }
    }
    map
}

/// GPUs from `system_profiler SPDisplaysDataType -json` (Intel Macs): `mac0`, `mac1`, ...
pub fn parse_system_profiler(text: &str) -> Vec<Gpu> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    let Some(items) = v.get("SPDisplaysDataType").and_then(|a| a.as_array()) else {
        return Vec::new();
    };
    let s = |item: &serde_json::Value, k: &str| {
        item.get(k)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .unwrap_or_default()
    };
    let hex = |t: &str| {
        let t = t.trim().to_ascii_lowercase();
        let i = t.find("0x")?;
        let h: String = t[i + 2..]
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .collect();
        (!h.is_empty()).then_some(h)
    };
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let vendor_text = s(item, "spdisplays_vendor");
            let lower = vendor_text.to_ascii_lowercase();
            let vendor = hex(&s(item, "spdisplays_vendor-id"))
                .or_else(|| hex(&vendor_text))
                .unwrap_or_else(|| {
                    if lower.contains("amd") || lower.contains("ati") {
                        "1002".into()
                    } else if lower.contains("intel") {
                        "8086".into()
                    } else if lower.contains("nvidia") {
                        "10de".into()
                    } else if lower.contains("apple") {
                        "apple".into()
                    } else {
                        lower.clone()
                    }
                });
            let name = Some(s(item, "sppci_model"))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| s(item, "_name"));
            Gpu {
                id: format!("mac{i}"),
                vendor,
                device: hex(&s(item, "spdisplays_device-id")).unwrap_or_default(),
                name,
                driver: None,
                alias: None,
                render_node: None,
            }
        })
        .collect()
}

/// The Windows version as `os_version` gives it, from the registry's `ProductName`,
/// `InstallationType`, build number and `DisplayVersion` (or `ReleaseId` before 20H2): `11 24H2`,
/// `10 22H2`, `Server 2025 24H2`, `Server 2019 1809`. The marketing version comes from the build
/// (22000 and later is Windows 11), since Windows 11's ProductName still says Windows 10; a
/// server's year from its ProductName, else from its build.
pub fn windows_version(product: &str, installation: &str, build: u32, release: &str) -> String {
    let server = installation.to_ascii_lowercase().contains("server")
        || product.to_ascii_lowercase().contains("server");
    let name = if server {
        let year = product
            .split(|c: char| !c.is_ascii_digit())
            .find(|w| w.len() == 4 && w.starts_with("20"))
            .map(str::to_string)
            .or_else(|| {
                let year = match build {
                    26100.. => "2025",
                    20348.. => "2022",
                    17763.. => "2019",
                    14393.. => "2016",
                    _ => return None,
                };
                Some(year.to_string())
            });
        match year {
            Some(y) => format!("Server {y}"),
            None => "Server".to_string(),
        }
    } else if build >= 22000 {
        "11".to_string()
    } else {
        "10".to_string()
    };
    let release = release.trim();
    if release.is_empty() {
        name
    } else {
        format!("{name} {release}")
    }
}

/// The vendor and device of a PCI hardware id (`PCI\VEN_10DE&DEV_2C02&SUBSYS_...`), lowercase hex:
/// `("10de", "2c02")`. `None` for any other bus (a Remote Desktop or indirect display adapter).
pub fn parse_pci_hardware_id(id: &str) -> Option<(String, String)> {
    let rest = id
        .get(..4)
        .filter(|p| p.eq_ignore_ascii_case("pci\\"))
        .map(|_| &id[4..])?;
    let field = |key: &str| {
        rest.split('&')
            .find_map(|part| {
                part.get(..key.len())
                    .filter(|k| k.eq_ignore_ascii_case(key))
                    .map(|_| &part[key.len()..])
            })
            .filter(|v| !v.is_empty() && v.chars().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_ascii_lowercase)
    };
    Some((field("VEN_")?, field("DEV_")?))
}

/// A PCI device's slot as Linux names it, `0000:01:00.0`, from its bus number and its address
/// as PnP gives it (device number in the high word, function in the low). The domain is 0.
pub fn pci_slot(bus: u32, address: u32) -> String {
    format!(
        "0000:{:02x}:{:02x}.{:x}",
        bus,
        address >> 16,
        address & 0xffff
    )
}

/// The slot from PnP's location text, `PCI bus 1, device 0, function 0`, when bus and address
/// can't be read on their own.
pub fn parse_pci_location(text: &str) -> Option<String> {
    let mut nums = [None; 3];
    for part in text.split(',') {
        let mut words = part.split_whitespace().rev();
        let (Some(n), Some(what)) = (words.next(), words.next()) else {
            continue;
        };
        let i = match what.to_ascii_lowercase().as_str() {
            "bus" => 0,
            "device" => 1,
            "function" => 2,
            _ => continue,
        };
        nums[i] = n.parse::<u32>().ok();
    }
    let [Some(bus), Some(dev), Some(func)] = nums else {
        return None;
    };
    Some(pci_slot(bus, (dev << 16) | func))
}

/// The GPU of a display adapter PnP lists: its hardware ids (the first PCI one counts), PCI slot,
/// device description and driver service. `None` for one that isn't on PCI.
pub fn windows_gpu(
    hardware_ids: &[String],
    slot: Option<String>,
    description: Option<String>,
    service: Option<String>,
) -> Option<Gpu> {
    let (vendor, device) = hardware_ids
        .iter()
        .find_map(|id| parse_pci_hardware_id(id))?;
    let slot = slot?;
    let name = description
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
        .unwrap_or_else(|| format!("{vendor}:{device}"));
    Some(Gpu {
        id: format!("{vendor}:{device}@{slot}"),
        vendor,
        device,
        name,
        driver: service
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        alias: None,
        render_node: None,
    })
}

#[cfg(windows)]
mod platform {
    use super::*;
    use crate::sys::registry::{hklm_dword, hklm_string};
    use crate::sys::win::from_wide;

    const CURRENT_VERSION: &str = r"SOFTWARE\Microsoft\Windows NT\CurrentVersion";

    /// The Windows build number (`26100`), from the registry.
    pub fn build() -> Option<u32> {
        hklm_string(CURRENT_VERSION, "CurrentBuildNumber")
            .or_else(|| hklm_string(CURRENT_VERSION, "CurrentBuild"))?
            .parse()
            .ok()
    }

    pub fn detect() -> anyhow::Result<Facts> {
        let build = build().unwrap_or(0);
        let text = |name: &str| hklm_string(CURRENT_VERSION, name).unwrap_or_default();
        let release = hklm_string(CURRENT_VERSION, "DisplayVersion")
            .or_else(|| hklm_string(CURRENT_VERSION, "ReleaseId"))
            .unwrap_or_default();
        let os_version = windows_version(
            &text("ProductName"),
            &text("InstallationType"),
            build,
            &release,
        );
        let kernel = format!(
            "{}.{}.{build}.{}",
            hklm_dword(CURRENT_VERSION, "CurrentMajorVersionNumber").unwrap_or(10),
            hklm_dword(CURRENT_VERSION, "CurrentMinorVersionNumber").unwrap_or(0),
            hklm_dword(CURRENT_VERSION, "UBR").unwrap_or(0)
        );
        let cpu_model = hklm_string(
            r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            "ProcessorNameString",
        )
        .unwrap_or_else(|| "unknown".into());
        use windows_sys::Win32::System::Threading::{
            ALL_PROCESSOR_GROUPS, GetActiveProcessorCount,
        };
        // SAFETY: plain call; it counts the processors of every group (more than 64 too).
        let mut cpus = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) };
        if cpus == 0 {
            cpus = std::thread::available_parallelism().map_or(1, |n| n.get() as u32);
        }
        Ok(Facts {
            node: String::new(),
            os: "windows".into(),
            os_version,
            arch: arch(),
            kernel,
            cpu_model,
            cpus,
            ram_mib: ram_mib()?,
            gpus: gpus(),
            display: display(),
            version: String::new(),
        })
    }

    /// The machine's architecture, also for a build emulated on another (x86_64 on ARM64).
    fn arch() -> String {
        use windows_sys::Win32::System::SystemInformation::{
            IMAGE_FILE_MACHINE_AMD64, IMAGE_FILE_MACHINE_ARM64, IMAGE_FILE_MACHINE_I386,
        };
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, IsWow64Process2};
        let (mut process, mut native) = (0u16, 0u16);
        // SAFETY: the pseudo handle of this process; both outputs are locals.
        if unsafe { IsWow64Process2(GetCurrentProcess(), &mut process, &mut native) } != 0 {
            match native {
                IMAGE_FILE_MACHINE_AMD64 => return "x86_64".into(),
                IMAGE_FILE_MACHINE_ARM64 => return "aarch64".into(),
                IMAGE_FILE_MACHINE_I386 => return "x86".into(),
                _ => {}
            }
        }
        normalize_arch(std::env::consts::ARCH)
    }

    fn ram_mib() -> anyhow::Result<u64> {
        use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
        let mut m = MEMORYSTATUSEX {
            dwLength: std::mem::size_of::<MEMORYSTATUSEX>() as u32,
            ..Default::default()
        };
        // SAFETY: m is plain data with dwLength set, as the call requires.
        if unsafe { GlobalMemoryStatusEx(&mut m) } == 0 {
            anyhow::bail!(
                "can't read the RAM size (GlobalMemoryStatusEx): {}",
                std::io::Error::last_os_error()
            );
        }
        Ok(m.ullTotalPhys / (1024 * 1024))
    }

    /// The PCI display adapters PnP lists as present, by slot.
    pub fn gpus() -> Vec<Gpu> {
        use windows_sys::Win32::Devices::DeviceAndDriverInstallation::{
            DIGCF_PRESENT, GUID_DEVCLASS_DISPLAY, SP_DEVINFO_DATA, SPDRP_ADDRESS, SPDRP_BUSNUMBER,
            SPDRP_DEVICEDESC, SPDRP_HARDWAREID, SPDRP_LOCATION_INFORMATION, SPDRP_SERVICE,
            SetupDiDestroyDeviceInfoList, SetupDiEnumDeviceInfo, SetupDiGetClassDevsW,
        };
        // SAFETY: lists the present devices of the display class; destroyed below.
        let set = unsafe {
            SetupDiGetClassDevsW(
                &GUID_DEVCLASS_DISPLAY,
                std::ptr::null(),
                std::ptr::null_mut(),
                DIGCF_PRESENT,
            )
        };
        if set == -1 {
            return Vec::new();
        }
        let mut gpus = Vec::new();
        for i in 0.. {
            let mut dev = SP_DEVINFO_DATA {
                cbSize: std::mem::size_of::<SP_DEVINFO_DATA>() as u32,
                ..Default::default()
            };
            // SAFETY: a valid set; dev is plain data with cbSize set.
            if unsafe { SetupDiEnumDeviceInfo(set, i, &mut dev) } == 0 {
                break;
            }
            let prop = |p| property(set, &dev, p);
            let text = |p| prop(p).map(|b| from_wide(&b));
            let dword = |p| {
                prop(p)
                    .filter(|b| b.len() >= 2)
                    .map(|b| u32::from(b[0]) | (u32::from(b[1]) << 16))
            };
            let ids: Vec<String> = prop(SPDRP_HARDWAREID)
                .map(|b| {
                    b.split(|&c| c == 0)
                        .filter(|s| !s.is_empty())
                        .map(String::from_utf16_lossy)
                        .collect()
                })
                .unwrap_or_default();
            let slot = match (dword(SPDRP_BUSNUMBER), dword(SPDRP_ADDRESS)) {
                (Some(bus), Some(address)) => Some(pci_slot(bus, address)),
                _ => text(SPDRP_LOCATION_INFORMATION).and_then(|t| parse_pci_location(&t)),
            };
            gpus.extend(windows_gpu(
                &ids,
                slot,
                text(SPDRP_DEVICEDESC),
                text(SPDRP_SERVICE),
            ));
        }
        // SAFETY: the set made above, not used after this.
        unsafe { SetupDiDestroyDeviceInfoList(set) };
        gpus.sort_by(|a, b| a.id.split('@').nth(1).cmp(&b.id.split('@').nth(1)));
        gpus
    }

    /// A device's registry property as UTF-16 units (DWORDs come as two), `None` if it has none.
    fn property(
        set: windows_sys::Win32::Devices::DeviceAndDriverInstallation::HDEVINFO,
        dev: &windows_sys::Win32::Devices::DeviceAndDriverInstallation::SP_DEVINFO_DATA,
        prop: u32,
    ) -> Option<Vec<u16>> {
        use windows_sys::Win32::Devices::DeviceAndDriverInstallation::SetupDiGetDeviceRegistryPropertyW;
        let mut need = 0u32;
        // SAFETY: a size query: no buffer, need receives the bytes the value takes.
        unsafe {
            SetupDiGetDeviceRegistryPropertyW(
                set,
                dev,
                prop,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
                &mut need,
            )
        };
        if need == 0 {
            return None;
        }
        // u16s, with room for a NUL the value may lack.
        let mut buf = vec![0u16; (need as usize).div_ceil(2) + 1];
        // SAFETY: buf has at least `need` bytes.
        let ok = unsafe {
            SetupDiGetDeviceRegistryPropertyW(
                set,
                dev,
                prop,
                std::ptr::null_mut(),
                buf.as_mut_ptr().cast(),
                need,
                std::ptr::null_mut(),
            )
        };
        (ok != 0).then_some(buf)
    }

    /// Whether this user has an active session: signed in at the console or over Remote Desktop,
    /// connected (a disconnected one doesn't count; a locked one does, as on a Mac).
    fn display() -> bool {
        use windows_sys::Win32::System::RemoteDesktop::{
            WTS_CURRENT_SERVER_HANDLE, WTS_SESSION_INFOW, WTSActive, WTSEnumerateSessionsW,
            WTSFreeMemory, WTSQuerySessionInformationW, WTSUserName,
        };
        let Some(me) = crate::sys::user_name() else {
            return false;
        };
        let mut info: *mut WTS_SESSION_INFOW = std::ptr::null_mut();
        let mut count = 0u32;
        // SAFETY: the local server; info receives an array of count entries, freed below.
        if unsafe { WTSEnumerateSessionsW(WTS_CURRENT_SERVER_HANDLE, 0, 1, &mut info, &mut count) }
            == 0
        {
            return false;
        }
        // SAFETY: WTSEnumerateSessionsW returned count entries at info.
        let sessions = unsafe { std::slice::from_raw_parts(info, count as usize) };
        let mine = sessions.iter().filter(|s| s.State == WTSActive).any(|s| {
            let mut name: windows_sys::core::PWSTR = std::ptr::null_mut();
            let mut bytes = 0u32;
            // SAFETY: name receives a string the call allocates, freed below.
            if unsafe {
                WTSQuerySessionInformationW(
                    WTS_CURRENT_SERVER_HANDLE,
                    s.SessionId,
                    WTSUserName,
                    &mut name,
                    &mut bytes,
                )
            } == 0
            {
                return false;
            }
            // SAFETY: name holds `bytes` bytes of a NUL-terminated string; freed after copying.
            let user = unsafe {
                let units = std::slice::from_raw_parts(name, bytes as usize / 2);
                let user = from_wide(units);
                WTSFreeMemory(name.cast());
                user
            };
            user.eq_ignore_ascii_case(&me)
        });
        // SAFETY: the array WTSEnumerateSessionsW allocated, not used after this.
        unsafe { WTSFreeMemory(info.cast()) };
        mine
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use std::path::Path;

    fn read(p: impl AsRef<Path>) -> Option<String> {
        std::fs::read_to_string(p).ok()
    }

    pub fn detect() -> anyhow::Result<Facts> {
        let (kernel, arch) = uname();
        let (cpu_model, mut cpus) = read("/proc/cpuinfo")
            .map(|t| parse_cpuinfo(&t))
            .unwrap_or_else(|| ("unknown".into(), 0));
        if cpus == 0 {
            cpus = std::thread::available_parallelism().map_or(1, |n| n.get() as u32);
        }
        let ram_mib = read("/proc/meminfo")
            .and_then(|t| parse_meminfo_mib(&t))
            .ok_or_else(|| anyhow::anyhow!("can't read MemTotal from /proc/meminfo"))?;
        let os_version = read("/etc/os-release")
            .or_else(|| read("/usr/lib/os-release"))
            .map(|t| os_version_from(&parse_os_release(&t)))
            .unwrap_or_else(|| "Linux".into());
        Ok(Facts {
            node: String::new(),
            os: "linux".into(),
            os_version,
            arch,
            kernel,
            cpu_model,
            cpus,
            ram_mib,
            gpus: gpus(),
            display: display(),
            version: String::new(),
        })
    }

    fn hex_file(p: &Path) -> Option<String> {
        let t = read(p)?;
        let t = t.trim().to_ascii_lowercase();
        Some(t.strip_prefix("0x").unwrap_or(&t).to_string())
    }

    pub fn gpus() -> Vec<Gpu> {
        gpus_at(Path::new("/"))
    }

    /// The GPU on the PCI device `dev` (a directory with `vendor`, `device` and `driver`), keyed
    /// by its slot.
    fn pci_gpu(dev: &Path, render_node: Option<String>) -> Option<(String, Gpu)> {
        let (Some(vendor), Some(device)) =
            (hex_file(&dev.join("vendor")), hex_file(&dev.join("device")))
        else {
            return None;
        };
        let slot = std::fs::canonicalize(dev)
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))?;
        let driver = std::fs::read_link(dev.join("driver"))
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()));
        Some((
            slot.clone(),
            Gpu {
                id: format!("{vendor}:{device}@{slot}"),
                vendor,
                device,
                name: String::new(),
                driver,
                alias: None,
                render_node,
            },
        ))
    }

    /// The GPUs under `root` (`/`, or a fake tree in tests).
    pub(super) fn gpus_at(root: &Path) -> Vec<Gpu> {
        let mut gpus: Vec<(String, Gpu)> = Vec::new();
        for e in std::fs::read_dir(root.join("sys/class/drm"))
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.starts_with("renderD") {
                continue;
            }
            gpus.extend(pci_gpu(
                &e.path().join("device"),
                Some(format!("/dev/dri/{name}")),
            ));
        }
        // An NVIDIA GPU has no render node while nvidia-drm isn't loaded (often so on a machine
        // without a display); the driver itself lists each GPU it drives.
        for e in std::fs::read_dir(root.join("proc/driver/nvidia/gpus"))
            .into_iter()
            .flatten()
            .flatten()
        {
            let slot = e.file_name().to_string_lossy().into_owned();
            if !gpus.iter().any(|(s, _)| *s == slot) {
                gpus.extend(pci_gpu(&root.join("sys/bus/pci/devices").join(&slot), None));
            }
        }
        gpus.sort_by(|a, b| a.0.cmp(&b.0));
        // Names: the NVIDIA driver's own file, then nvidia-smi once and lspci if needed.
        for (slot, g) in gpus.iter_mut() {
            if g.vendor == "10de"
                && let Some(n) =
                    read(root.join(format!("proc/driver/nvidia/gpus/{slot}/information")))
                        .and_then(|t| parse_nvidia_information(&t))
            {
                g.name = n;
            }
        }
        if gpus
            .iter()
            .any(|(_, g)| g.name.is_empty() && g.vendor == "10de")
        {
            let names = output(
                "nvidia-smi",
                &["--query-gpu=pci.bus_id,name", "--format=csv,noheader"],
            )
            .map(|t| parse_nvidia_smi(&t))
            .unwrap_or_default();
            for (slot, g) in gpus.iter_mut().filter(|(_, g)| g.name.is_empty()) {
                if let Some(n) = names.get(slot.as_str()) {
                    g.name = n.clone();
                }
            }
        }
        // lspci reads the configuration of each device it lists, which wakes a GPU the kernel has
        // powered off (D3cold): it lists only the GPU to name, and once in this process.
        for (slot, g) in gpus.iter_mut().filter(|(_, g)| g.name.is_empty()) {
            let known = lspci_names().get(&g.id).cloned();
            let named = known.or_else(|| {
                let listed = output("lspci", &["-mm", "-D", "-s", slot])?;
                let n = parse_lspci(&listed)
                    .remove(slot.as_str())
                    .unwrap_or_else(|| format!("{}:{}", g.vendor, g.device));
                lspci_names().insert(g.id.clone(), n.clone());
                Some(n)
            });
            g.name = named.unwrap_or_else(|| format!("{}:{}", g.vendor, g.device));
        }
        gpus.into_iter().map(|(_, g)| g).collect()
    }

    /// The names lspci gave GPUs in this process, by GPU id (which has the PCI slot).
    static LSPCI_NAMES: std::sync::Mutex<BTreeMap<String, String>> =
        std::sync::Mutex::new(BTreeMap::new());

    fn lspci_names() -> std::sync::MutexGuard<'static, BTreeMap<String, String>> {
        LSPCI_NAMES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn display() -> bool {
        // SAFETY: getuid has no preconditions.
        let uid = unsafe { libc::getuid() };
        let uid_s = uid.to_string();
        let args = ["show-user", &uid_s, "--property=Display", "--value"];
        // logind elects any session of the user's as its display, a graphical one first: with
        // none, an ssh login is it. Only a graphical one counts.
        if let Some(id) = output("loginctl", &args).filter(|out| !out.trim().is_empty())
            && let Some(kind) = output(
                "loginctl",
                &["show-session", id.trim(), "--property=Type", "--value"],
            )
            && matches!(kind.trim(), "x11" | "wayland" | "mir")
        {
            return true;
        }
        let runtime =
            std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| format!("/run/user/{uid}"));
        std::fs::read_dir(runtime).is_ok_and(|mut d| {
            d.any(|e| {
                e.is_ok_and(|e| {
                    let n = e.file_name().to_string_lossy().into_owned();
                    n.starts_with("wayland-") && !n.ends_with(".lock")
                })
            })
        })
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::ffi::CString;

    fn sysctl_raw(name: &str) -> Option<Vec<u8>> {
        let cname = CString::new(name).ok()?;
        let mut len: libc::size_t = 0;
        // SAFETY: a null buffer asks sysctlbyname for the size only.
        let rc = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                std::ptr::null_mut(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 || len == 0 {
            return None;
        }
        let mut buf = vec![0u8; len];
        // SAFETY: buf has room for len bytes, and sysctlbyname writes at most len.
        let rc = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                buf.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            return None;
        }
        buf.truncate(len);
        Some(buf)
    }

    fn sysctl_string(name: &str) -> Option<String> {
        let b = sysctl_raw(name)?;
        let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
        Some(String::from_utf8_lossy(&b[..end]).trim().to_string()).filter(|s| !s.is_empty())
    }

    fn sysctl_int(name: &str) -> Option<u64> {
        let b = sysctl_raw(name)?;
        match b.len() {
            4 => Some(u32::from_ne_bytes(b[..4].try_into().ok()?) as u64),
            8 => Some(u64::from_ne_bytes(b[..8].try_into().ok()?)),
            _ => None,
        }
    }

    pub fn detect() -> anyhow::Result<Facts> {
        let (kernel, mut arch) = uname();
        // An x86_64 build under Rosetta still runs on Apple silicon.
        let apple_silicon = sysctl_int("hw.optional.arm64") == Some(1);
        if apple_silicon {
            arch = "aarch64".into();
        }
        let cpu_model =
            sysctl_string("machdep.cpu.brand_string").unwrap_or_else(|| "unknown".into());
        let cpus = sysctl_int("hw.ncpu").unwrap_or(1) as u32;
        let ram_mib = sysctl_int("hw.memsize")
            .ok_or_else(|| anyhow::anyhow!("can't read hw.memsize"))?
            / (1024 * 1024);
        let os_version = sysctl_string("kern.osproductversion")
            .or_else(|| output("sw_vers", &["-productVersion"]).map(|s| s.trim().to_string()))
            .unwrap_or_default();
        let gpus = if apple_silicon {
            vec![Gpu {
                id: "mac0".into(),
                vendor: "apple".into(),
                device: String::new(),
                name: cpu_model.clone(),
                driver: None,
                alias: None,
                render_node: None,
            }]
        } else {
            output("system_profiler", &["SPDisplaysDataType", "-json"])
                .map(|t| parse_system_profiler(&t))
                .unwrap_or_default()
        };
        // SAFETY: getuid has no preconditions.
        let uid = unsafe { libc::getuid() };
        let display = std::fs::metadata("/dev/console")
            .is_ok_and(|m| std::os::unix::fs::MetadataExt::uid(&m) == uid);
        Ok(Facts {
            node: String::new(),
            os: "macos".into(),
            os_version,
            arch,
            kernel,
            cpu_model,
            cpus,
            ram_mib,
            gpus,
            display,
            version: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GpuConfig;

    /// Without `name` in node.toml, a machine whose daemon has run is named by its records,
    /// whatever its hostname is now; a `name` in node.toml still names it (and the daemon then
    /// refuses a name other than its records').
    #[test]
    fn the_records_name_a_machine_node_toml_does_not_name() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        let unnamed = NodeConfig::default();
        assert_ne!(node_name(&paths, &unnamed), "first");
        let keys = paths.keys_dir();
        let id = crate::mesh::identity::Identity::load_or_create(&keys).unwrap();
        crate::mesh::trust::Trust::load_or_found(&keys, &id, "first", "first:1").unwrap();
        assert_eq!(node_name(&paths, &unnamed), "first");
        let named = NodeConfig {
            name: Some("other".into()),
            ..Default::default()
        };
        assert_eq!(node_name(&paths, &named), "other");
    }

    /// A restart in place starts from facts handed over by the binary before: this binary's
    /// version and node.toml as it is now name the machine and its GPUs, as a fresh detection
    /// would.
    #[test]
    fn handed_over_facts_say_this_version_and_what_node_toml_says_now() {
        let gpu = |id: &str, alias: Option<&str>| Gpu {
            id: id.into(),
            vendor: "10de".into(),
            alias: alias.map(str::to_string),
            ..Default::default()
        };
        let mut facts = Facts {
            node: "before".into(),
            version: "0.0.9".into(),
            gpus: vec![gpu("10de:1@a", Some("old")), gpu("10de:2@b", Some("gone"))],
            ..Default::default()
        };
        let config = NodeConfig {
            name: Some("now".into()),
            gpu: vec![GpuConfig {
                matches: "10de:1@a".into(),
                alias: Some("new".into()),
                env: Default::default(),
            }],
            ..Default::default()
        };
        let nowhere = Paths {
            home: "/nonexistent".into(),
            config: "/nonexistent".into(),
        };
        as_configured(&mut facts, &nowhere, &config);
        assert_eq!(
            (facts.node.as_str(), facts.version.as_str()),
            ("now", crate::VERSION)
        );
        let aliases: Vec<_> = facts.gpus.iter().map(|g| g.alias.as_deref()).collect();
        assert_eq!(aliases, [Some("new"), None]);
    }

    #[test]
    fn parsers() {
        let rel = parse_os_release("NAME=\"CachyOS Linux\"\nPRETTY_NAME=\"CachyOS\"\nID=cachyos\n");
        assert_eq!(os_version_from(&rel), "CachyOS");
        let rel = parse_os_release("NAME=\"Ubuntu\"\nVERSION_ID=\"24.04\"\n");
        assert_eq!(os_version_from(&rel), "Ubuntu 24.04");
        assert_eq!(
            parse_meminfo_mib("MemTotal:       98509892 kB\nMemFree: 1 kB\n"),
            Some(96201)
        );
        let (model, n) = parse_cpuinfo(
            "processor\t: 0\nmodel name\t: Intel(R) Core(TM) Ultra 9 275HX\n\nprocessor\t: 1\nmodel name\t: Intel(R) Core(TM) Ultra 9 275HX\n",
        );
        assert_eq!((model.as_str(), n), ("Intel(R) Core(TM) Ultra 9 275HX", 2));
        assert_eq!(normalize_slot("00000000:01:00.0"), "0000:01:00.0");
        assert_eq!(normalize_slot("01:00.0"), "0000:01:00.0");
        assert_eq!(normalize_slot("0000:03:00.0"), "0000:03:00.0");
        assert_eq!(
            parse_nvidia_information("Model: \t\t NVIDIA GeForce RTX 5080 Laptop GPU\nIRQ: 16\n")
                .as_deref(),
            Some("NVIDIA GeForce RTX 5080 Laptop GPU")
        );
        let smi = parse_nvidia_smi("00000000:01:00.0, NVIDIA GeForce RTX 5080 Laptop GPU\n");
        assert_eq!(smi["0000:01:00.0"], "NVIDIA GeForce RTX 5080 Laptop GPU");
        let lspci = parse_lspci(
            "0000:01:00.0 \"VGA compatible controller\" \"NVIDIA Corporation\" \"GB203M / GN22 [GeForce RTX 5080 Max-Q / Mobile]\" -ra1 -p00 \"Micro-Star International Co., Ltd. [MSI]\" \"Device 1474\"\n",
        );
        assert_eq!(
            lspci["0000:01:00.0"],
            "NVIDIA Corporation GB203M / GN22 [GeForce RTX 5080 Max-Q / Mobile]"
        );
        let sp = parse_system_profiler(
            r#"{"SPDisplaysDataType":[{"_name":"Intel UHD Graphics 630","sppci_model":"Intel UHD Graphics 630","spdisplays_vendor":"Intel","spdisplays_device-id":"0x3e9b"},{"_name":"AMD Radeon Pro 5500M","sppci_model":"AMD Radeon Pro 5500M","spdisplays_vendor":"sppci_vendor_amd","spdisplays_device-id":"0x7340"}]}"#,
        );
        assert_eq!(sp.len(), 2);
        assert_eq!(
            (
                sp[0].id.as_str(),
                sp[0].vendor.as_str(),
                sp[0].device.as_str()
            ),
            ("mac0", "8086", "3e9b")
        );
        assert_eq!((sp[1].id.as_str(), sp[1].vendor.as_str()), ("mac1", "1002"));
        assert!(gpu_matches("amd", &sp[1]) && gpu_matches("1002:7340", &sp[1]));
        assert_eq!(normalize_arch("arm64"), "aarch64");
    }

    /// A tool that hangs (nvidia-smi on a wedged GPU) is given up on at the time limit, and
    /// killed: detection goes on without what it would have said.
    #[cfg(unix)]
    #[test]
    fn a_hung_tool_is_given_up_on() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("pid");
        let script = format!("echo $$ > {}; exec sleep 30", pid_file.display());
        let started = std::time::Instant::now();
        let got = output_within(
            "sh",
            &["-c", &script],
            std::time::Duration::from_millis(500),
        );
        assert_eq!(got, None);
        let took = started.elapsed();
        assert!(took < std::time::Duration::from_secs(5), "{took:?}");
        // It was killed (and reaped off-thread): its pid is soon gone.
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        // SAFETY: signal 0 only checks that the process exists.
        while unsafe { libc::kill(pid, 0) } == 0 {
            assert!(std::time::Instant::now() < deadline, "{pid} still runs");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // A quick tool still answers, also with more output than a pipe holds.
        let out = output_within(
            "sh",
            &["-c", "head -c 300000 /dev/zero"],
            std::time::Duration::from_secs(20),
        )
        .unwrap();
        assert_eq!(out.len(), 300_000);
        assert_eq!(
            output_within("sh", &["-c", "exit 3"], std::time::Duration::from_secs(20)),
            None
        );
        assert_eq!(
            output_within("/nonexistent/tool", &[], std::time::Duration::from_secs(20)),
            None
        );
    }

    /// A tool given up on whose run can't end isn't started again until it does: nvidia-smi on a
    /// wedged GPU sits in an uninterruptible wait, where SIGKILL can't end it, and each refresh
    /// (every minute) would leave one more such process and two more threads in the daemon. The
    /// stand-in: a tool whose output stays open in a process the kill doesn't reach.
    #[cfg(unix)]
    #[test]
    fn a_tool_whose_run_does_not_end_is_not_started_again() {
        let tmp = tempfile::tempdir().unwrap();
        let pids = tmp.path().join("pids");
        let script = format!("sleep 30 & echo $! >> {}; exec sleep 30", pids.display());
        let started = || -> Vec<i32> {
            std::fs::read_to_string(&pids)
                .unwrap_or_default()
                .lines()
                .map(|l| l.trim().parse().unwrap())
                .collect()
        };
        let limit = std::time::Duration::from_millis(300);
        for _ in 0..3 {
            assert_eq!(output_within("sh", &["-c", &script], limit), None);
        }
        let first = started();
        for &pid in &first {
            // SAFETY: kill has no memory preconditions; the pid is a process this test started,
            // kept from being reused by the open pipe its parent's reader waits on.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        assert_eq!(first.len(), 1, "started {} times", first.len());
        // Once that run has ended, the tool runs again.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while started().len() < 2 {
            assert!(std::time::Instant::now() < deadline, "never started again");
            assert_eq!(output_within("sh", &["-c", &script], limit), None);
        }
        for &pid in &started()[1..] {
            // SAFETY: as above.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }

    /// nvidia-smi's and lspci's output is read as it comes: a bus id with non-ASCII characters
    /// is kept as it is (it names no GPU here), not a panic in the facts refresh.
    #[test]
    fn slots_with_non_ascii_are_not_a_panic() {
        for bad in [
            "0\u{e9}000:01:00.0",
            "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}:01:00.0",
            "\u{e9}:01:00.0",
            "0000:\u{e9}:00.0",
            "\u{1f600}",
        ] {
            let got = std::panic::catch_unwind(|| normalize_slot(bad));
            assert!(got.is_ok(), "{bad:?}");
        }
        // The last four characters of a long domain, as for ASCII ones.
        assert_eq!(normalize_slot("0\u{e9}000:01:00.0"), "\u{e9}000:01:00.0");
        assert_eq!(normalize_slot("\u{e9}:01:00.0"), "000\u{e9}:01:00.0");
        let smi = parse_nvidia_smi("0\u{e9}000:01:00.0, X\n00000000:01:00.0, Y\n");
        assert_eq!(smi["0000:01:00.0"], "Y");
    }

    #[test]
    fn gpu_config_adds_aliases() {
        let mut gpus = vec![
            Gpu {
                id: "10de:2c19@0000:01:00.0".into(),
                vendor: "10de".into(),
                device: "2c19".into(),
                ..Default::default()
            },
            Gpu {
                id: "8086:7d67@0000:00:02.0".into(),
                vendor: "8086".into(),
                device: "7d67".into(),
                ..Default::default()
            },
        ];
        let config = NodeConfig {
            gpu: vec![
                GpuConfig {
                    matches: "nvidia".into(),
                    alias: Some("5080".into()),
                    ..Default::default()
                },
                GpuConfig {
                    matches: "any".into(),
                    alias: Some("ambiguous".into()),
                    ..Default::default()
                },
                GpuConfig {
                    matches: "amd".into(),
                    alias: Some("missing".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        apply_gpu_config(&mut gpus, &config);
        assert_eq!(gpus[0].alias.as_deref(), Some("5080"));
        assert_eq!(gpus[1].alias, None);
        let problems = gpu_config_problems(&gpus, &config);
        assert_eq!(problems.len(), 2, "{problems:?}");
    }

    /// A fake /sys and /proc: an NVIDIA GPU with a render node, one without (nvidia-drm not
    /// loaded, as on a machine without a display), and one handed to vfio-pci for a VM.
    #[cfg(target_os = "linux")]
    #[test]
    fn nvidia_gpus_without_a_render_node_are_listed() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let devices = r.join("sys/bus/pci/devices");
        std::fs::create_dir_all(&devices).unwrap();
        for (slot, device, driver, render, model) in [
            (
                "0000:01:00.0",
                "0x2c58",
                "nvidia",
                Some("renderD128"),
                Some("RTX A"),
            ),
            ("0000:02:00.0", "0x2330", "nvidia", None, Some("H B")),
            ("0000:03:00.0", "0x2684", "vfio-pci", None, None),
        ] {
            let dev = r.join("sys/devices/pci0000:00").join(slot);
            std::fs::create_dir_all(&dev).unwrap();
            std::fs::write(dev.join("vendor"), "0x10de\n").unwrap();
            std::fs::write(dev.join("device"), format!("{device}\n")).unwrap();
            let drv = r.join("sys/bus/pci/drivers").join(driver);
            std::fs::create_dir_all(&drv).unwrap();
            symlink(&drv, dev.join("driver")).unwrap();
            symlink(&dev, devices.join(slot)).unwrap();
            if let Some(render) = render {
                let node = r.join("sys/class/drm").join(render);
                std::fs::create_dir_all(&node).unwrap();
                symlink(&dev, node.join("device")).unwrap();
            }
            if let Some(model) = model {
                let d = r.join("proc/driver/nvidia/gpus").join(slot);
                std::fs::create_dir_all(&d).unwrap();
                let info = format!("Model: \t\t {model}\nIRQ:   \t\t 180\n");
                std::fs::write(d.join("information"), info).unwrap();
            }
        }
        let gpus = platform::gpus_at(r);
        let seen: Vec<_> = gpus
            .iter()
            .map(|g| {
                let driver = g.driver.as_deref();
                (
                    g.id.as_str(),
                    g.name.as_str(),
                    driver,
                    g.render_node.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                (
                    "10de:2c58@0000:01:00.0",
                    "RTX A",
                    Some("nvidia"),
                    Some("/dev/dri/renderD128")
                ),
                ("10de:2330@0000:02:00.0", "H B", Some("nvidia"), None),
            ]
        );
    }

    /// A GPU the NVIDIA driver doesn't name (here an Intel one) is named from lspci, the same at
    /// every detection.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_gpu_named_by_lspci_keeps_its_name() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let dev = r.join("sys/devices/pci0000:00/0000:00:02.0");
        std::fs::create_dir_all(&dev).unwrap();
        std::fs::write(dev.join("vendor"), "0x8086\n").unwrap();
        std::fs::write(dev.join("device"), "0x7d67\n").unwrap();
        let node = r.join("sys/class/drm/renderD129");
        std::fs::create_dir_all(&node).unwrap();
        symlink(&dev, node.join("device")).unwrap();
        let first = platform::gpus_at(r);
        assert_eq!(first.len(), 1, "{first:?}");
        assert_eq!(first[0].id, "8086:7d67@0000:00:02.0");
        assert!(!first[0].name.is_empty(), "{first:?}");
        assert_eq!(platform::gpus_at(r), first);
    }

    /// Structural checks on the real machine; run with --nocapture to see what it found.
    #[test]
    fn detects_this_machine() {
        let started = std::time::Instant::now();
        let nowhere = Paths {
            home: "/nonexistent".into(),
            config: "/nonexistent".into(),
        };
        let f = detect(&nowhere, &NodeConfig::default()).unwrap();
        let took = started.elapsed();
        println!(
            "facts ({took:?}): {}",
            serde_json::to_string_pretty(&f).unwrap()
        );
        assert!(!f.node.is_empty() && !f.kernel.is_empty() && !f.cpu_model.is_empty());
        assert_eq!(
            f.os,
            if cfg!(target_os = "macos") {
                "macos"
            } else if cfg!(windows) {
                "windows"
            } else {
                "linux"
            }
        );
        assert!(f.arch == "x86_64" || f.arch == "aarch64", "{}", f.arch);
        assert!(f.cpus >= 1 && f.ram_mib >= 512);
        assert_eq!(f.version, crate::VERSION);
        for g in &f.gpus {
            assert!(
                !g.id.is_empty() && !g.vendor.is_empty() && !g.name.is_empty(),
                "{g:?}"
            );
            if f.os == "linux" || f.os == "windows" {
                assert_eq!(
                    g.id,
                    format!(
                        "{}:{}@{}",
                        g.vendor,
                        g.device,
                        g.id.split('@').nth(1).unwrap()
                    )
                );
                assert!(match g.render_node.as_deref() {
                    Some(r) => r.starts_with("/dev/dri/renderD"),
                    None => g.vendor == "10de" || f.os == "windows",
                });
            }
        }
        if f.os == "macos" && f.arch == "aarch64" {
            assert_eq!(f.gpus.len(), 1);
            assert_eq!(
                (f.gpus[0].id.as_str(), f.gpus[0].vendor.as_str()),
                ("mac0", "apple")
            );
        }
        if f.os == "windows" {
            // 10.0.BUILD.UBR, and a version naming Windows 10 or 11 or a server.
            let parts: Vec<&str> = f.kernel.split('.').collect();
            assert_eq!(parts.len(), 4, "{}", f.kernel);
            assert!(parts[2].parse::<u32>().unwrap() >= 17763, "{}", f.kernel);
            assert!(
                ["10", "11", "Server"]
                    .iter()
                    .any(|v| f.os_version.split(' ').next() == Some(v)),
                "{}",
                f.os_version
            );
            #[cfg(windows)]
            assert_eq!(windows_build(), Some(parts[2].parse().unwrap()));
        }
        let ids: std::collections::BTreeSet<_> = f.gpus.iter().map(|g| &g.id).collect();
        assert_eq!(ids.len(), f.gpus.len(), "GPU ids are unique");
    }

    #[test]
    fn windows_versions() {
        let cases = [
            // Windows 11's ProductName still says Windows 10.
            ("Windows 10 Pro", "Client", 26100, "24H2", "11 24H2"),
            ("Windows 10 Pro", "Client", 19045, "22H2", "10 22H2"),
            ("Windows 10 Enterprise", "Client", 17763, "1809", "10 1809"),
            ("Windows 11 Home", "Client", 22631, "", "11"),
            (
                "Windows Server 2025 Datacenter",
                "Server",
                26100,
                "24H2",
                "Server 2025 24H2",
            ),
            (
                "Windows Server 2019 Standard",
                "Server Core",
                17763,
                "1809",
                "Server 2019 1809",
            ),
            (
                "Windows Server Datacenter",
                "Server",
                20348,
                "",
                "Server 2022",
            ),
            ("Windows Server", "Server", 100, "", "Server"),
        ];
        for (product, installation, build, release, want) in cases {
            assert_eq!(
                windows_version(product, installation, build, release),
                want,
                "{product} {build}"
            );
        }
        // How --on reads them.
        let facts = |v: &str| Facts {
            os: "windows".into(),
            os_version: v.into(),
            ..Default::default()
        };
        assert!(facts("11 24H2").matches("windows", "11-24h2"));
        assert!(facts("Server 2025 24H2").matches("windows", "server-2025"));
    }

    #[test]
    fn windows_gpus() {
        assert_eq!(
            parse_pci_hardware_id(r"PCI\VEN_10DE&DEV_2C02&SUBSYS_205E1458&REV_A1"),
            Some(("10de".into(), "2c02".into()))
        );
        assert_eq!(
            parse_pci_hardware_id(r"pci\ven_8086&dev_7D67"),
            Some(("8086".into(), "7d67".into()))
        );
        for not_pci in [
            r"SWD\RemoteDisplayEnum\RdpIdd_IndirectDisplay",
            r"ROOT\BasicRender",
            r"PCI\VEN_10DE",
            r"PCI\VEN_XYZ&DEV_1",
            "PC",
            "",
            "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}",
        ] {
            assert_eq!(parse_pci_hardware_id(not_pci), None, "{not_pci}");
        }
        assert_eq!(pci_slot(1, 0), "0000:01:00.0");
        assert_eq!(pci_slot(0x3a, (0x1f << 16) | 3), "0000:3a:1f.3");
        assert_eq!(
            parse_pci_location("PCI bus 1, device 0, function 0").as_deref(),
            Some("0000:01:00.0")
        );
        assert_eq!(
            parse_pci_location("PCI bus 0, device 2, function 1").as_deref(),
            Some("0000:00:02.1")
        );
        assert_eq!(parse_pci_location("on Intel(R) Xe Graphics"), None);
        let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let g = windows_gpu(
            &ids(&[
                r"PCI\VEN_10DE&DEV_2C02&SUBSYS_205E1458&REV_A1",
                r"PCI\VEN_10DE&DEV_2C02",
            ]),
            Some(pci_slot(1, 0)),
            Some("NVIDIA GeForce RTX 5080".into()),
            Some("nvlddmkm".into()),
        )
        .unwrap();
        assert_eq!(g.id, "10de:2c02@0000:01:00.0");
        assert_eq!(
            (g.vendor.as_str(), g.device.as_str(), g.name.as_str()),
            ("10de", "2c02", "NVIDIA GeForce RTX 5080")
        );
        assert_eq!(g.driver.as_deref(), Some("nvlddmkm"));
        assert!(gpu_matches("nvidia", &g) && gpu_matches("10de:2c02", &g));
        // Named by its ids when PnP has no description; not on PCI: no GPU.
        let g = windows_gpu(
            &ids(&[r"PCI\VEN_1234&DEV_1111"]),
            Some(pci_slot(0, 1 << 16)),
            None,
            None,
        )
        .unwrap();
        assert_eq!((g.name.as_str(), g.driver), ("1234:1111", None));
        assert_eq!(
            windows_gpu(
                &ids(&[r"SWD\RemoteDisplayEnum\RdpIdd_IndirectDisplay"]),
                Some(pci_slot(0, 0)),
                Some("Microsoft Remote Display Adapter".into()),
                Some("RdpIdd".into())
            ),
            None
        );
        assert_eq!(
            windows_gpu(&ids(&[r"PCI\VEN_10DE&DEV_2C02"]), None, None, None),
            None
        );
    }
}
