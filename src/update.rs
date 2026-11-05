//! `potluck self-update` (phase 3): pick the release binary for this OS and architecture, verify its
//! SHA-256 against the release's `SHA256SUMS`, write it beside the running binary, remove
//! `com.apple.quarantine` on macOS, fsync, check that it runs, and rename it over the old one (never
//! rewrite a running or signed binary in place). Then the daemon restarts in place on the new
//! binary ([`crate::daemon::handover`]): the daemon updating itself does so at once; `self-update
//! --here` asks the running daemon to (`potluck restart`), and restarts its service as before when
//! no daemon answers or the one that does is too old to know the request. Running jobs survive
//! either.
//!
//! Releases are plain binaries named `potluck-<target>` plus `SHA256SUMS` (`sha256sum` format), on
//! GitHub at `https://github.com/<GITHUB_REPO>/releases/download/v<version>/<file>`. "Latest" is
//! first resolved to its tag (`.../releases/latest` redirects to `.../releases/tag/v<version>`), so
//! the binary and `SHA256SUMS` come from one release even if another is published meanwhile.
//! `POTLUCK_GITHUB_URL` replaces `https://github.com` (for tests); `POTLUCK_RELEASES_URL` replaces
//! all of it with a directory URL holding one release's binaries and `SHA256SUMS` (plain http is
//! allowed). With a directory, a requested version is checked against the version the downloaded
//! binary reports.
//!
//! Versions only go up: toward the latest release, an update never installs a version lower than
//! the one on disk, the one this process runs, or the one the daemon runs. A lower version is
//! installed only when asked for directly ([`update_here_to`] with `allow_lower`).
//!
//! HTTP: ureq with rustls on the ring provider and the webpki root certificates compiled in, so the
//! static musl binary needs no OpenSSL and no system certificate store.

use anyhow::{Context, Result, anyhow, bail};
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

mod restart;
#[cfg(test)]
mod tests;

pub(crate) use restart::restart_command;
pub use restart::{Restart, restart_daemon};

/// The GitHub repository the releases come from, as `owner/name`.
pub const GITHUB_REPO: &str = "steelbrain/potluck";
/// Replaces `https://github.com` in release URLs (for tests).
pub const GITHUB_URL_ENV: &str = "POTLUCK_GITHUB_URL";
/// A directory URL holding one release's binaries and `SHA256SUMS`, used instead of GitHub.
pub const RELEASES_URL_ENV: &str = "POTLUCK_RELEASES_URL";
/// `1`: leave the daemon running after an update (as install.sh does with it).
pub const NO_RESTART_ENV: &str = "POTLUCK_NO_RESTART";
/// The checksum file of every release, in `sha256sum` format.
pub const SUMS_FILE: &str = "SHA256SUMS";

const MAX_BINARY_BYTES: u64 = 512 << 20;
const MAX_SUMS_BYTES: u64 = 1 << 20;
/// How long the new binary gets to answer `--version`.
const VERSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Updates this machine only to the latest release: replaces the binary and restarts the daemon.
/// Never installs a version lower than the one here; it says so and changes nothing. `daemon` is
/// the version the local daemon runs, if one answered (the daemon passes its own `VERSION`): when
/// the binary already is the release but the daemon runs another version, the daemon is restarted.
/// Returns what happened.
pub fn self_update_here(daemon: Option<&str>) -> Result<String> {
    update_here(None, false, daemon)
}

/// Updates this machine only to release `version` (`0.2.0`; a leading `v` is accepted), as the
/// newest member asks the others to. A version lower than the one here is refused unless
/// `allow_lower`, which only a direct request from the user may set: the mesh's version gating
/// always moves up. Fails, changing nothing, if the downloaded binary reports another version.
/// `daemon` is as for [`self_update_here`]. When the daemon calls this for itself, the restart at
/// the end stops the daemon, so it should answer its peer first.
pub fn update_here_to(version: &str, allow_lower: bool, daemon: Option<&str>) -> Result<String> {
    update_here(Some(version), allow_lower, daemon)
}

/// What an update the daemon ran on itself did ([`update_daemon`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Updated {
    /// What happened, in plain words.
    pub text: String,
    /// The daemon must restart in place on the new binary: it was replaced, or the daemon runs an
    /// older version than it. Not with `POTLUCK_NO_RESTART=1`.
    pub restart_here: bool,
}

impl From<String> for Updated {
    fn from(text: String) -> Updated {
        Updated {
            text,
            restart_here: false,
        }
    }
}

/// For the daemon updating itself: to release `version` as a newer member asks (never lower), or
/// to the latest release (`None`, for `potluck self-update`), with the daemon's own version as the
/// floor. Says whether the daemon must then restart in place on the new binary (it does that
/// itself, rather than have a service manager start another process).
pub fn update_daemon(version: Option<&str>) -> Result<Updated> {
    let outcome = std::cell::RefCell::new(None);
    let text = update_here_with(version, false, Some(crate::VERSION), Some(&outcome))?;
    let restart_here = matches!(outcome.into_inner(), Some(Restart::InPlaceNow));
    Ok(Updated { text, restart_here })
}

fn update_here(version: Option<&str>, allow_lower: bool, daemon: Option<&str>) -> Result<String> {
    update_here_with(version, allow_lower, daemon, None)
}

/// [`update_here`]. With `outcome`, the daemon updates itself: it restarts in place next, and
/// `outcome` notes whether it should; without, the running daemon is asked to restart in place
/// (its service restarted when that can't be done).
fn update_here_with(
    version: Option<&str>,
    allow_lower: bool,
    daemon: Option<&str>,
    outcome: Option<&std::cell::RefCell<Option<Restart>>>,
) -> Result<String> {
    let version = version.map(normalize_version).transpose()?;
    let var = |k: &str| std::env::var(k).ok();
    let source = Source::from_env(
        var(RELEASES_URL_ENV).as_deref(),
        var(GITHUB_URL_ENV).as_deref(),
    );
    let exe = running_binary()?;
    let plan = Plan {
        source: &source,
        target: &exe,
        version: version.as_deref(),
        allow_lower,
        daemon,
    };
    let skip = || {
        Ok(Restart::Skipped {
            service: restart::service_installed(),
        })
    };
    // A service installed from another potluck would run that one again: say so instead.
    let restart_ours = || match crate::service::installed_program() {
        Some(service) if !crate::service::same_file(&service, &exe) => Ok(Restart::OtherBinary {
            service,
            updated: exe.clone(),
        }),
        _ => match ask_daemon_to_restart(&exe) {
            Some(answer) => answer.map(Restart::InPlace),
            None => restart_daemon(),
        },
    };
    let itself = || Ok(Restart::InPlaceNow);
    let restart: &dyn Fn() -> Result<Restart> = if var(NO_RESTART_ENV).as_deref() == Some("1") {
        &skip
    } else if outcome.is_some() {
        &itself
    } else {
        &restart_ours
    };
    let noted = || {
        let r = restart();
        if let (Some(o), Ok(r)) = (outcome, &r) {
            *o.borrow_mut() = Some(r.clone());
        }
        r
    };
    update_with(&plan, Some(&noted))
}

/// Asks the daemon on this machine's socket to restart in place (`potluck restart`) on `exe`, the
/// binary just updated: its answer, or why it didn't (one running another binary refuses). None
/// when no daemon answers, or the one that does is too old to know the request: then its service
/// is restarted as before.
fn ask_daemon_to_restart(exe: &Path) -> Option<Result<String>> {
    use crate::proto::{Request, Response};
    let paths = crate::config::Paths::from_env().ok()?;
    let mut c = crate::client::Client::connect(&paths, Default::default()).ok()?;
    c.set_read_timeout(RESTART_ANSWER_TIMEOUT).ok()?;
    match c.call(Request::Restart {
        binary: Some(exe.to_path_buf()),
    }) {
        Ok(Response::Text { text }) => Some(Ok(text)),
        Ok(other) => Some(Err(anyhow!(
            "the daemon answered the restart with {other:?}"
        ))),
        Err(e) if crate::client::not_understood(&e) => None,
        Err(e) => Some(Err(e)),
    }
}

/// How long a restart in place may take to answer: the check of the new binary, the drain and
/// the handover.
const RESTART_ANSWER_TIMEOUT: Duration = Duration::from_secs(180);

/// Where releases come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// A repository's GitHub releases, `https://github.com/<repo>/releases` (or a URL laid out
    /// like it): `<root>/latest` redirects to `<root>/tag/v<version>`, and a release's files are
    /// at `<root>/download/v<version>/<file>`.
    Releases(String),
    /// A directory URL holding one release's binaries and `SHA256SUMS`.
    Directory(String),
}

impl Source {
    /// The directory in `releases_url` (`POTLUCK_RELEASES_URL`) when set, otherwise the releases
    /// of [`GITHUB_REPO`] on `github_url` (`POTLUCK_GITHUB_URL`, default `https://github.com`).
    pub fn from_env(releases_url: Option<&str>, github_url: Option<&str>) -> Source {
        let set = |v: Option<&str>| {
            v.map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|v| v.trim_end_matches('/').to_string())
        };
        if let Some(dir) = set(releases_url) {
            return Source::Directory(dir);
        }
        let github = set(github_url).unwrap_or_else(|| "https://github.com".to_string());
        Source::Releases(format!("{github}/{GITHUB_REPO}/releases"))
    }
}

/// One update, for [`update`].
pub struct Plan<'a> {
    pub source: &'a Source,
    /// The binary to replace; symlinks are resolved.
    pub target: &'a Path,
    /// A given version instead of the latest; the new binary must report it.
    pub version: Option<&'a str>,
    /// May install a version lower than the one here (only when asked for directly).
    pub allow_lower: bool,
    /// The version the daemon runs, if known.
    pub daemon: Option<&'a str>,
}

/// The release binary's name for an OS and architecture as Rust names them
/// (`std::env::consts::{OS, ARCH}`), if potluck publishes one.
pub fn asset_for(os: &str, arch: &str) -> Option<&'static str> {
    Some(match (os, arch) {
        ("macos", "aarch64") => "potluck-aarch64-apple-darwin",
        ("macos", "x86_64") => "potluck-x86_64-apple-darwin",
        ("linux", "x86_64") => "potluck-x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "potluck-aarch64-unknown-linux-musl",
        _ => return None,
    })
}

/// The architecture to install for: the one this binary was built for, except that an x86_64
/// binary translated by Rosetta on Apple silicon gets the native arm64 binary, as install.sh does.
fn host_arch(os: &str, arch: &'static str, translated: bool) -> &'static str {
    if os == "macos" && arch == "x86_64" && translated {
        "aarch64"
    } else {
        arch
    }
}

/// The release binary for this machine.
pub fn asset_name() -> Result<&'static str> {
    let os = std::env::consts::OS;
    let arch = host_arch(os, std::env::consts::ARCH, rosetta_translated());
    asset_for(os, arch).ok_or_else(|| {
        anyhow!("potluck publishes no release binary for {os} on {arch}; build it with cargo")
    })
}

/// Whether this process runs translated by Rosetta (`sysctl.proc_translated` is 1).
#[cfg(target_os = "macos")]
fn rosetta_translated() -> bool {
    let mut value: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated and `value` is a c_int of `size` bytes. On Intel Macs the
    // name doesn't exist and the call fails with ENOENT: not translated.
    let rc = unsafe {
        libc::sysctlbyname(
            c"sysctl.proc_translated".as_ptr(),
            (&mut value as *mut libc::c_int).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    rc == 0 && value == 1
}

/// Only macOS has Rosetta.
#[cfg(not(target_os = "macos"))]
fn rosetta_translated() -> bool {
    false
}

/// `v0.2.0` or `0.2.0` to `0.2.0`; refuses anything that isn't a plain version string.
pub fn normalize_version(v: &str) -> Result<String> {
    let v = v.trim();
    let v = v.strip_prefix('v').unwrap_or(v);
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+');
    if v.is_empty() || !v.starts_with(|c: char| c.is_ascii_digit()) || !v.chars().all(plain) {
        bail!("not a version: {v:?} (expected something like 0.2.0)");
    }
    Ok(v.to_string())
}

/// Orders two versions as semver does (`1.2.3`, `1.2.3-rc.1` before `1.2.3`, build metadata after
/// `+` ignored, a leading `v` accepted). `None` if either isn't such a version.
pub fn compare_versions(a: &str, b: &str) -> Option<Ordering> {
    fn parse(v: &str) -> Option<(Vec<u64>, Vec<&str>)> {
        let v = v.strip_prefix('v').unwrap_or(v);
        let v = v.split('+').next()?;
        let (core, pre) = match v.split_once('-') {
            Some((core, pre)) => (core, pre.split('.').collect()),
            None => (v, Vec::new()),
        };
        let nums = core
            .split('.')
            .map(|p| p.parse().ok())
            .collect::<Option<Vec<u64>>>()?;
        Some((nums, pre))
    }
    let (an, ap) = parse(a)?;
    let (bn, bp) = parse(b)?;
    for i in 0..an.len().max(bn.len()) {
        let (x, y) = (an.get(i).unwrap_or(&0), bn.get(i).unwrap_or(&0));
        if x != y {
            return Some(x.cmp(y));
        }
    }
    // A prerelease comes before its release; prereleases compare identifier by identifier.
    match (ap.is_empty(), bp.is_empty()) {
        (true, true) => return Some(Ordering::Equal),
        (true, false) => return Some(Ordering::Greater),
        (false, true) => return Some(Ordering::Less),
        (false, false) => {}
    }
    for (x, y) in ap.iter().zip(&bp) {
        let order = match (x.parse::<u64>(), y.parse::<u64>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            (Ok(_), Err(_)) => Ordering::Less,
            (Err(_), Ok(_)) => Ordering::Greater,
            (Err(_), Err(_)) => x.cmp(y),
        };
        if order != Ordering::Equal {
            return Some(order);
        }
    }
    Some(ap.len().cmp(&bp.len()))
}

/// The highest of `versions` that parse.
fn highest<'a>(versions: impl IntoIterator<Item = Option<&'a str>>) -> Option<&'a str> {
    let mut best: Option<&str> = None;
    for v in versions.into_iter().flatten() {
        match best {
            None if compare_versions(v, v).is_some() => best = Some(v),
            Some(b) if compare_versions(v, b) == Some(Ordering::Greater) => best = Some(v),
            _ => {}
        }
    }
    best
}

/// Replaces `plan.target` with this machine's binary from `plan.source`, and if `restart`, restarts
/// the daemon's service when it is installed and running, also when the binary already is the
/// release but the daemon runs another version. Nothing is written until the download matches its
/// checksum, and on any failure before the final rename the old binary is left as it was.
pub fn update(plan: &Plan, restart: bool) -> Result<String> {
    let restart_daemon: &dyn Fn() -> Result<Restart> = &restart_daemon;
    update_with(plan, restart.then_some(restart_daemon))
}

/// The release to download from: its directory, and the version it must hold, if known.
struct Release {
    base: String,
    version: Option<String>,
    /// From a tag: one release's files, never a mix of two.
    tagged: bool,
}

fn resolve_release(source: &Source, version: Option<&str>) -> Result<Release> {
    match source {
        Source::Directory(dir) => Ok(Release {
            base: dir.trim().trim_end_matches('/').to_string(),
            version: version.map(str::to_string),
            tagged: false,
        }),
        Source::Releases(root) => {
            let root = root.trim().trim_end_matches('/');
            let version = match version {
                Some(v) => v.to_string(),
                None => latest_version(root)?,
            };
            Ok(Release {
                base: format!("{root}/download/v{version}"),
                version: Some(version),
                tagged: true,
            })
        }
    }
}

/// The latest release's version: `<root>/latest` redirects to `<root>/tag/v<version>`.
fn latest_version(root: &str) -> Result<String> {
    let url = format!("{root}/latest");
    let resp = http_agent(0).get(&url).call().map_err(|e| match e {
        ureq::Error::StatusCode(code) => {
            anyhow!(
                "finding the latest release: {url}: HTTP {code} (is there a published release?)"
            )
        }
        e => anyhow!("finding the latest release: {url}: {e}"),
    })?;
    let location = resp
        .headers()
        .get("location")
        .and_then(|l| l.to_str().ok())
        .unwrap_or("");
    let tag = location
        .rsplit_once("/tag/")
        .map(|(_, tag)| tag.split(['?', '#']).next().unwrap_or(tag))
        .map(percent_decode);
    tag.and_then(|t| normalize_version(&t).ok()).ok_or_else(|| {
        anyhow!(
            "finding the latest release: {url} answered {} {location:?}, not a redirect to a release tag",
            resp.status()
        )
    })
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let escaped = s
            .get(i + 1..i + 3)
            .filter(|h| b[i] == b'%' && h.bytes().all(|c| c.is_ascii_hexdigit()))
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match escaped {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether `new` may be installed against `floor` (the highest version here). `Ok(None)`: go on.
/// `Ok(Some(why))`: toward the latest release, stop and say why. `Err`: a requested version that
/// would go down without `allow_lower`.
fn below_floor(plan: &Plan, new: &str, floor: Option<&str>) -> Result<Option<String>> {
    let Some(floor) = floor.filter(|_| !plan.allow_lower) else {
        return Ok(None);
    };
    match compare_versions(new, floor) {
        Some(Ordering::Less) => {}
        Some(_) => return Ok(None),
        None => bail!(
            "can't tell whether potluck {new} is lower than the potluck {floor} here; nothing was \
             changed"
        ),
    }
    match plan.version {
        None => Ok(Some(format!(
            "The latest release is potluck {new}, lower than the potluck {floor} here; nothing \
             was changed."
        ))),
        Some(_) => bail!(
            "potluck {new} is lower than the potluck {floor} here; a lower version is installed \
             only when asked for directly; nothing was changed"
        ),
    }
}

/// [`update`], with `restart` as the function that restarts the daemon, if any.
fn update_with(plan: &Plan, restart: Option<&dyn Fn() -> Result<Restart>>) -> Result<String> {
    let asset = asset_name()?;
    let target = resolve_target(plan.target)?;
    let old_version = binary_version(&target).ok();
    // Toward the latest release, never below what is on disk or running here.
    let floor = highest([Some(crate::VERSION), plan.daemon, old_version.as_deref()]);
    let release = resolve_release(plan.source, plan.version)?;
    if let Some(v) = &release.version
        && let Some(why) = below_floor(plan, v, floor)?
    {
        return Ok(why);
    }
    let base = &release.base;
    let agent = http_agent(10);

    let sums_url = format!("{base}/{SUMS_FILE}");
    let sums = fetch(&agent, &sums_url, MAX_SUMS_BYTES)?;
    let sums = String::from_utf8(sums).map_err(|_| anyhow!("{sums_url} is not a text file"))?;
    let expected = expected_sha256(&sums, asset).with_context(|| sums_url.clone())?;

    let asset_url = format!("{base}/{asset}");
    let binary = fetch(&agent, &asset_url, MAX_BINARY_BYTES)?;
    let actual = sha256(&binary);
    if actual != expected {
        // Without a tag, the two downloads may come from two releases.
        let race = if release.tagged {
            ""
        } else {
            " (if a release was published there between the two downloads, run it again)"
        };
        bail!(
            "{asset_url} has SHA-256 {}, but {SUMS_FILE} lists {}; refusing it, nothing was \
             changed{race}",
            hex(&actual),
            hex(&expected)
        );
    }
    let mismatch = |found: &str, want: &str| {
        let want = match plan.version {
            Some(_) => format!("the requested {want}"),
            None => format!("{want} as its tag says"),
        };
        anyhow!("{asset_url} is potluck {found}, not {want}; nothing was changed")
    };

    if sha256_file(&target).ok() == Some(expected) {
        let old = old_version.as_deref().unwrap_or("unknown");
        if let Some(want) = &release.version
            && !crate::mesh::same_version(old, want)
        {
            return Err(mismatch(old, want));
        }
        let unchanged = format!(
            "{} is already the release binary (potluck {old}); nothing was changed.",
            target.display()
        );
        // The daemon may still run an older binary: one replaced without a restart (install.sh
        // with POTLUCK_NO_RESTART=1) or by an update whose restart failed. It must then restart,
        // or it never runs the release.
        let (Some(restart), Some(daemon), Some(old)) = (restart, plan.daemon, &old_version) else {
            return Ok(unchanged);
        };
        if crate::mesh::same_version(daemon, old) {
            return Ok(unchanged);
        }
        if !plan.allow_lower && compare_versions(old, daemon) != Some(Ordering::Greater) {
            return Ok(format!(
                "{unchanged} The daemon runs potluck {daemon}, which is not older; it was left \
                 alone."
            ));
        }
        return restarted(
            format!("{unchanged} The daemon runs potluck {daemon}."),
            restart,
        );
    }

    let staged = Staged::write(&target, &binary)?;
    let new_version = binary_version(&staged.path).map_err(|e| {
        anyhow!("the downloaded {asset} doesn't run on this machine ({e:#}); nothing was changed")
    })?;
    // Build metadata (`+...`) doesn't make another version.
    if let Some(want) = &release.version
        && !crate::mesh::same_version(&new_version, want)
    {
        return Err(mismatch(&new_version, want));
    }
    if let Some(why) = below_floor(plan, &new_version, floor)? {
        return Ok(why);
    }
    staged.rename_over(&target)?;

    let updated = match &old_version {
        Some(old) => format!("Updated {} from {old} to {new_version}.", target.display()),
        None => format!("Updated {} to {new_version}.", target.display()),
    };
    let Some(restart) = restart else {
        return Ok(format!("{updated} The daemon was not restarted."));
    };
    restarted(updated, restart)
}

/// `done`, then the outcome of restarting the daemon with `restart`.
fn restarted(done: String, restart: &dyn Fn() -> Result<Restart>) -> Result<String> {
    match restart() {
        Ok(outcome) => Ok(format!("{done} {outcome}")),
        Err(e) => Err(anyhow!(
            "{done} Restarting the daemon failed: {e:#}. Restart it with `{}`.",
            restart::restart_command()
        )),
    }
}

/// The running executable, as a path that exists (Linux reports a replaced one as
/// `PATH (deleted)`).
pub fn running_binary() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("finding the running potluck binary")?;
    if !exe.exists()
        && let Some(s) = exe.to_str()
        && let Some(stripped) = s.strip_suffix(" (deleted)")
    {
        return Ok(PathBuf::from(stripped));
    }
    Ok(exe)
}

/// `target` with symlinks resolved; it must be a regular file.
fn resolve_target(target: &Path) -> Result<PathBuf> {
    let real = fs::canonicalize(target)
        .with_context(|| format!("finding the binary to replace, {}", target.display()))?;
    let meta = fs::metadata(&real).with_context(|| real.display().to_string())?;
    if !meta.is_file() {
        bail!("{} is not a regular file", real.display());
    }
    Ok(real)
}

/// An HTTP client following up to `redirects` redirects (0: return the redirect itself).
fn http_agent(redirects: u32) -> ureq::Agent {
    ureq::Agent::config_builder()
        .user_agent(format!("potluck/{}", crate::VERSION))
        .max_redirects(redirects)
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_per_read(Some(Duration::from_secs(60)))
        .timeout_global(Some(Duration::from_secs(30 * 60)))
        .build()
        .into()
}

fn fetch(agent: &ureq::Agent, url: &str, limit: u64) -> Result<Vec<u8>> {
    let mut resp = agent.get(url).call().map_err(|e| match e {
        ureq::Error::StatusCode(code) => anyhow!("downloading {url}: HTTP {code}"),
        e => anyhow!("downloading {url}: {e}"),
    })?;
    resp.body_mut()
        .with_config()
        .limit(limit)
        .read_to_vec()
        .map_err(|e| anyhow!("downloading {url}: {e}"))
}

/// The SHA-256 that `sums` (`sha256sum` output) lists for `asset`.
fn expected_sha256(sums: &str, asset: &str) -> Result<[u8; 32]> {
    let mut found: Option<[u8; 32]> = None;
    for line in sums.lines() {
        let line = line.trim_end_matches('\r');
        let Some((hash, rest)) = line.split_once(' ') else {
            continue;
        };
        // `<hash>  <name>` in text mode, `<hash> *<name>` in binary mode.
        let name = rest
            .strip_prefix(' ')
            .or_else(|| rest.strip_prefix('*'))
            .unwrap_or(rest);
        if name != asset {
            continue;
        }
        let hash = parse_hex32(hash)
            .ok_or_else(|| anyhow!("{SUMS_FILE} has a malformed hash for {asset}: {hash:?}"))?;
        if found.is_some_and(|f| f != hash) {
            bail!("{SUMS_FILE} lists {asset} twice with different hashes");
        }
        found = Some(hash);
    }
    found.ok_or_else(|| anyhow!("{SUMS_FILE} has no line for {asset}"))
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn sha256_file(path: &Path) -> std::io::Result<[u8; 32]> {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(ctx.finish().as_ref());
    Ok(out)
}

/// The version a potluck binary reports (`potluck 0.2.0` from `--version`).
pub(crate) fn binary_version(path: &Path) -> Result<String> {
    let mut child = spawn_retrying(Command::new(path).arg("--version"))
        .with_context(|| format!("running {} --version", path.display()))?;
    let deadline = Instant::now() + VERSION_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "{} --version didn't finish in {}s",
                path.display(),
                VERSION_TIMEOUT.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut out = String::new();
    if let Some(mut stdout) = child.stdout.take() {
        stdout.read_to_string(&mut out)?;
    }
    if !status.success() {
        bail!("{} --version failed ({status})", path.display());
    }
    out.lines()
        .next()
        .and_then(|l| l.trim().strip_prefix("potluck "))
        .and_then(|v| v.split_whitespace().next())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("{} --version printed {:?}", path.display(), out.trim()))
}

/// Spawns `cmd`, retrying while the file is busy: another thread forking while a freshly written
/// file was still open for writing leaves a copy of that descriptor in its child until the child
/// execs, and exec then fails with ETXTBSY.
fn spawn_retrying(cmd: &mut Command) -> std::io::Result<std::process::Child> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut tries = 0;
    loop {
        match cmd.spawn() {
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) && tries < 50 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(20));
            }
            other => return other,
        }
    }
}

/// The new binary, written beside the target under a temporary name. Removed on drop unless it
/// was renamed over the target.
struct Staged {
    path: PathBuf,
    done: bool,
}

impl Staged {
    fn write(target: &Path, data: &[u8]) -> Result<Staged> {
        let dir = target
            .parent()
            .ok_or_else(|| anyhow!("{} has no parent directory", target.display()))?;
        let name = target
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "potluck".into());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let path = dir.join(format!(".{name}.update-{}-{nanos}", std::process::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(&path)
            .with_context(|| {
                format!(
                    "can't write the new binary beside {} (creating {})",
                    target.display(),
                    path.display()
                )
            })?;
        let staged = Staged { path, done: false };
        let written = (|| -> std::io::Result<()> {
            file.write_all(data)?;
            file.set_permissions(fs::Permissions::from_mode(0o755))?;
            clear_quarantine(&staged.path)?;
            file.sync_all()
        })();
        drop(file);
        written.with_context(|| format!("writing {}", staged.path.display()))?;
        Ok(staged)
    }

    fn rename_over(mut self, target: &Path) -> Result<()> {
        fs::rename(&self.path, target).with_context(|| {
            format!("renaming {} over {}", self.path.display(), target.display())
        })?;
        self.done = true;
        if let Some(dir) = target.parent() {
            File::open(dir)
                .and_then(|d| d.sync_all())
                .with_context(|| format!("syncing {}", dir.display()))?;
        }
        Ok(())
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if !self.done {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Removes macOS's `com.apple.quarantine` attribute, if set, so Gatekeeper lets the binary run.
#[cfg(target_os = "macos")]
pub fn clear_quarantine(path: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let name = c"com.apple.quarantine";
    // SAFETY: both are valid NUL-terminated strings for the duration of the call.
    let rc = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr(), libc::XATTR_NOFOLLOW) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ENOATTR) {
        return Ok(());
    }
    Err(err)
}

/// Only macOS has a quarantine attribute.
#[cfg(not(target_os = "macos"))]
pub fn clear_quarantine(_path: &Path) -> std::io::Result<()> {
    Ok(())
}
