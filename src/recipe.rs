//! Named recipes from the repository's committed `potluck.toml` (at the repository root), so agents
//! run `potluck dispatch gpu-tests` instead of composing options. Options on the command line
//! override the recipe's; `[env]` applies to every job dispatched from the repository.
//!
//! ```toml
//! [env]
//! RUST_BACKTRACE = "1"
//!
//! [recipe.gpu-tests]
//! cmd = ["cargo", "nextest", "run", "--profile", "gpu"]
//! timeout = "15m"
//! priority = 4
//! gpu = ["nvidia"]
//! ram = "16G"
//! cpu = 4
//! on = { os = "linux" }
//! hold = "30m"
//! keep = ["target/nextest/default/junit.xml"]
//! env = { SOME_VAR = "1" }
//! ```
//!
//! `on` is a table, or a list in `--on`'s syntax, where `macos` and `linux` can stand alone (any
//! version): `on = ["macos", "gpu=apple"]`.

use crate::cli::DispatchArgs;
use crate::gitinfo::short;
use crate::types::{
    DEFAULT_PRIORITY, MAX_HOLD_MS, MIN_HOLD_MS, OS_KEYS, Request, check_on, fmt_ms,
    is_os_shorthand, parse_duration_ms, parse_ram_mib,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// The file's name at the repository root.
pub const FILE_NAME: &str = "potluck.toml";

/// The fact keys `--on` and a recipe's `on` accept (see [`crate::types::Facts::matches`]).
pub const FACT_KEYS: &[&str] = &[
    "macos",
    "linux",
    "gpu",
    "gpu_driver",
    "ram_gib",
    "node",
    "os",
    "os_version",
    "arch",
    "kernel",
    "cpu_model",
    "cpus",
    "display",
];

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectFile {
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub recipe: BTreeMap<String, Recipe>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    pub cmd: Vec<String>,
    pub timeout: Option<String>,
    pub priority: Option<u8>,
    #[serde(default)]
    pub gpu: Vec<String>,
    pub ram: Option<String>,
    pub cpu: Option<u32>,
    #[serde(default)]
    pub exclusive: bool,
    /// A table (`{ macos = "27" }`) or a list in `--on`'s syntax (`["macos", "gpu=apple"]`).
    #[serde(default, deserialize_with = "table_or_list")]
    pub on: BTreeMap<String, String>,
    pub hold: Option<String>,
    #[serde(default)]
    pub keep: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// A recipe's `on`: a table of facts, or a list of `--on` arguments split like
/// [`split_on`]. They are checked when the recipe is dispatched ([`check_facts`]).
fn table_or_list<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    struct OnVisitor;
    impl<'de> serde::de::Visitor<'de> for OnVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str(r#"a table of facts such as { macos = "27" }, or a list such as ["macos", "gpu=apple"]"#)
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut on = BTreeMap::new();
            while let Some((k, v)) = map.next_entry::<String, String>()? {
                on.insert(k, v);
            }
            Ok(on)
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut on: BTreeMap<String, String> = BTreeMap::new();
            while let Some(item) = seq.next_element::<String>()? {
                let (k, v) = split_on(&item);
                if let Some(had) = on.get(&k).filter(|had| **had != v) {
                    return Err(serde::de::Error::custom(format!(
                        "{item}: {k} is already required ({k}={had}); each fact takes one value"
                    )));
                }
                on.insert(k, v);
            }
            Ok(on)
        }
    }
    d.deserialize_any(OnVisitor)
}

/// Splits one `--on` argument: `KEY=VALUE`, or a bare `KEY` with an empty value (which only
/// `macos` and `linux` accept, see [`check_facts`]).
fn split_on(s: &str) -> (String, String) {
    match s.split_once('=') {
        Some((k, v)) => (k.trim().to_string(), v.to_string()),
        None => (s.trim().to_string(), String::new()),
    }
}

/// Refuses requirements `--on` and a recipe's `on` can't express: unknown facts, a bare key or
/// an empty value other than `macos` and `linux` (any version), and requirements that
/// contradict each other ([`check_on`]).
fn check_facts(on: &BTreeMap<String, String>) -> Result<(), String> {
    for (key, value) in on {
        if !FACT_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "unknown fact {key:?}: use one of {}",
                FACT_KEYS.join(", ")
            ));
        }
        if value.is_empty() && !is_os_shorthand(key) {
            return Err(format!(
                "{key} needs a value, {key}=VALUE (only macos and linux stand alone)"
            ));
        }
    }
    check_on(on)
}

/// Parses a potluck.toml. Unknown fields are refused, so a typo can't silently drop an option.
pub fn parse(text: &str) -> anyhow::Result<ProjectFile> {
    let file: ProjectFile =
        toml::from_str(text).map_err(|e| anyhow::anyhow!("{FILE_NAME}: {}", e.message()))?;
    for (name, r) in &file.recipe {
        if r.cmd.is_empty() {
            anyhow::bail!("{FILE_NAME}: recipe {name}: cmd is empty");
        }
    }
    Ok(file)
}

/// Reads the potluck.toml at the top of the repository as committed at `commit` (never the working
/// tree), whichever directory of the repository `repo_dir` is. `None` when that commit has no
/// potluck.toml; an error naming what it is when it is a directory, a symbolic link or a submodule
/// rather than a file.
pub fn load_at_commit(repo_dir: &Path, commit: &str) -> anyhow::Result<Option<ProjectFile>> {
    // gitinfo's command: git's own variables (GIT_DIR from a hook) must not name another
    // repository.
    let listed = crate::gitinfo::git(repo_dir)
        .args(["ls-tree", "--full-tree", "-z", commit, "--", FILE_NAME])
        .stderr(std::process::Stdio::null())
        .output()
        .map_err(|e| anyhow::anyhow!("could not run git: {e}"))?;
    // `<mode> <type> <id>\t<name>`, or nothing when the commit has no such entry.
    let entry = String::from_utf8_lossy(&listed.stdout).into_owned();
    let Some((meta, _)) = entry.split_once('\t').filter(|_| listed.status.success()) else {
        return Ok(None);
    };
    let mut meta = meta.split(' ');
    let (mode, id) = (meta.next().unwrap_or(""), meta.nth(1).unwrap_or(""));
    let not_a_file = match mode {
        "100644" | "100755" => None,
        "120000" => Some("a symbolic link"),
        "040000" => Some("a directory"),
        "160000" => Some("a submodule"),
        _ => Some("not a regular file"),
    };
    if let Some(what) = not_a_file {
        anyhow::bail!(
            "{FILE_NAME} at commit {} is {what}, not a file: potluck reads recipes only from a \
             file committed at the top of the repository",
            short(commit)
        );
    }
    let out = crate::gitinfo::git(repo_dir)
        .args(["cat-file", "blob", id])
        .output()
        .map_err(|e| anyhow::anyhow!("could not run git: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "git cat-file blob {id} ({FILE_NAME} at {}): {}",
            short(commit),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let text = String::from_utf8(out.stdout)
        .map_err(|_| anyhow::anyhow!("{FILE_NAME} at {commit} is not UTF-8"))?;
    parse(&text)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("{e} (as committed at {})", short(commit)))
}

/// A dispatch's options after applying the recipe and the command line, ready for a
/// [`crate::types::JobSpec`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub recipe: Option<String>,
    pub cmd: Vec<String>,
    /// The file's top-level env, then the recipe's, then `--env`.
    pub env: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub priority: u8,
    pub request: Request,
    pub on: BTreeMap<String, String>,
    pub hold_ms: Option<u64>,
    pub keep: Vec<String>,
    pub labels: BTreeMap<String, String>,
}

/// Applies `args` over the named recipe (if any) of `file`. Options given on the command line
/// replace the recipe's; `on` and `env` merge key by key, the command line last; `--exclusive`
/// can only add exclusivity (a recipe's `exclusive = true` stays).
pub fn resolve(file: &ProjectFile, args: &DispatchArgs) -> anyhow::Result<Resolved> {
    let empty = Recipe::default();
    let recipe = match &args.recipe {
        Some(name) => file.recipe.get(name).ok_or_else(|| {
            if file.recipe.is_empty() {
                anyhow::anyhow!("no recipe {name:?}: the committed {FILE_NAME} has no recipes")
            } else {
                let names: Vec<&str> = file.recipe.keys().map(String::as_str).collect();
                anyhow::anyhow!(
                    "no recipe {name:?} in the committed {FILE_NAME} (it has: {})",
                    names.join(", ")
                )
            }
        })?,
        None => &empty,
    };
    let what = |field: &str| match &args.recipe {
        Some(name) => format!("recipe {name}: {field}"),
        None => format!("--{field}"),
    };
    // Where a value came from, for its error: the option when given, else the recipe's field.
    let from = |given: bool, field: &str| {
        if given {
            format!("--{field}")
        } else {
            what(field)
        }
    };

    let cmd = if !args.cmd.is_empty() {
        args.cmd.clone()
    } else if !recipe.cmd.is_empty() {
        recipe.cmd.clone()
    } else {
        anyhow::bail!("no command: give one after --, or name a recipe from {FILE_NAME}");
    };

    let timeout = args.timeout.as_deref().or(recipe.timeout.as_deref());
    let timeout_ms = match timeout {
        Some(t) => parse_duration_ms(t)
            .map_err(|e| anyhow::anyhow!("{}: {e}", from(args.timeout.is_some(), "timeout")))?,
        None => anyhow::bail!(
            "--timeout is required (the run limit, with units: 600s, 5m, 2h5m10s), on the command line or in the recipe"
        ),
    };

    let priority = args
        .priority
        .or(recipe.priority)
        .unwrap_or(DEFAULT_PRIORITY);
    if !(1..=5).contains(&priority) {
        anyhow::bail!(
            "{}: priority {priority} is out of range: use 1 (lowest) to 5 (highest)",
            from(args.priority.is_some(), "priority")
        );
    }

    let mut request = Request::default();
    if let Some(ram) = args.ram.as_deref().or(recipe.ram.as_deref()) {
        request.ram_mib = parse_ram_mib(ram)
            .map_err(|e| anyhow::anyhow!("{}: {e}", from(args.ram.is_some(), "ram")))?;
    }
    request.cpu = args.cpu.or(recipe.cpu).unwrap_or(request.cpu);
    if request.cpu == 0 {
        anyhow::bail!(
            "{}: cpu must be at least 1 (it is a weight in cores)",
            from(args.cpu.is_some(), "cpu")
        );
    }
    request.gpus = if args.gpus.is_empty() {
        recipe.gpu.clone()
    } else {
        args.gpus.clone()
    };
    if let Some(bad) = request.gpus.iter().find(|g| g.trim().is_empty()) {
        anyhow::bail!("empty GPU selector {bad:?}");
    }
    request.exclusive = args.exclusive || recipe.exclusive;

    let on = resolve_on(&recipe.on, &args.on, args.recipe.as_deref())?;

    let mut env = file.env.clone();
    env.extend(recipe.env.clone());
    for kv in &args.env {
        let (k, v) = parse_kv(kv, "--env")?;
        env.insert(k, v);
    }

    let hold_ms = match args.hold.as_deref().or(recipe.hold.as_deref()) {
        Some(h) => Some(
            parse_hold_ms(h)
                .map_err(|e| anyhow::anyhow!("{}: {e}", from(args.hold.is_some(), "hold")))?
                .max(MIN_HOLD_MS),
        ),
        None => None,
    };

    let keep = if args.keep.is_empty() {
        recipe.keep.clone()
    } else {
        args.keep.clone()
    };

    let mut labels = BTreeMap::new();
    for kv in &args.labels {
        let (k, v) = parse_kv(kv, "--label")?;
        labels.insert(k, v);
    }

    Ok(Resolved {
        recipe: args.recipe.clone(),
        cmd,
        env,
        timeout_ms,
        priority,
        request,
        on,
        hold_ms,
        keep,
        labels,
    })
}

/// The facts a dispatch requires: the recipe's `on`, then each `--on`, key by key. The OS keys
/// (`macos`, `linux`, `os`) count as one: an `--on` naming the OS replaces the recipe's, so
/// `--on linux` runs a recipe that says `macos = "27"` on Linux. Each side is checked on its
/// own, then the two together.
fn resolve_on(
    recipe_on: &BTreeMap<String, String>,
    flags: &[String],
    recipe: Option<&str>,
) -> anyhow::Result<BTreeMap<String, String>> {
    if let Some(name) = recipe {
        check_facts(recipe_on).map_err(|e| anyhow::anyhow!("recipe {name}: on: {e}"))?;
    }
    let mut given: BTreeMap<String, String> = BTreeMap::new();
    for kv in flags {
        let (k, v) = split_on(kv);
        if k.is_empty() {
            anyhow::bail!("--on {kv:?}: expected KEY=VALUE, or macos or linux alone");
        }
        // All must hold, and one key keeps one value: the second would replace the first.
        if let Some(had) = given.get(&k).filter(|had| **had != v) {
            anyhow::bail!(
                "--on {kv}: {k} is already required ({k}={had}); each fact takes one value"
            );
        }
        given.insert(k, v);
    }
    check_facts(&given).map_err(|e| anyhow::anyhow!("--on: {e}"))?;
    let mut on = recipe_on.clone();
    if given.keys().any(|k| OS_KEYS.contains(&k.as_str())) {
        on.retain(|k, _| !OS_KEYS.contains(&k.as_str()));
    }
    on.extend(given);
    if let Some(name) = recipe {
        check_on(&on).map_err(|e| anyhow::anyhow!("--on with recipe {name}'s on: {e}"))?;
    }
    Ok(on)
}

/// Parses a hold duration: units required, at most 24 hours.
pub fn parse_hold_ms(s: &str) -> anyhow::Result<u64> {
    let ms = parse_duration_ms(s)?;
    if ms > MAX_HOLD_MS {
        anyhow::bail!("hold {s} is too long: at most {}", fmt_ms(MAX_HOLD_MS));
    }
    Ok(ms)
}

/// Splits `KEY=VALUE`; the key must not be empty.
pub fn parse_kv(s: &str, flag: &str) -> anyhow::Result<(String, String)> {
    match s.split_once('=') {
        Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.to_string())),
        _ => anyhow::bail!("{flag} {s:?}: expected KEY=VALUE"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn args(argv: &[&str]) -> DispatchArgs {
        let mut full = vec!["potluck", "dispatch"];
        full.extend_from_slice(argv);
        match crate::cli::Cli::try_parse_from(full).unwrap().command {
            crate::cli::Command::Dispatch(a) => a,
            _ => unreachable!(),
        }
    }

    const FILE: &str = r#"
[env]
RUST_BACKTRACE = "1"
SHARED = "file"

[recipe.gpu-tests]
cmd = ["cargo", "nextest", "run", "--profile", "gpu"]
timeout = "15m"
priority = 4
gpu = ["nvidia"]
ram = "16G"
cpu = 4
exclusive = true
on = { os = "linux" }
hold = "30m"
keep = ["target/nextest/default/junit.xml"]
env = { SHARED = "recipe" }
"#;

    #[test]
    fn the_documented_example_parses_and_resolves() {
        let file = parse(FILE).unwrap();
        let r = resolve(&file, &args(&["gpu-tests"])).unwrap();
        assert_eq!(r.cmd, ["cargo", "nextest", "run", "--profile", "gpu"]);
        assert_eq!((r.timeout_ms, r.priority), (15 * 60_000, 4));
        assert_eq!(
            r.request,
            Request {
                ram_mib: 16 * 1024,
                cpu: 4,
                gpus: vec!["nvidia".into()],
                exclusive: true
            }
        );
        assert_eq!(r.on.get("os").map(String::as_str), Some("linux"));
        assert_eq!(r.hold_ms, Some(30 * 60_000));
        assert_eq!(r.env.get("SHARED").map(String::as_str), Some("recipe"));
        assert_eq!(r.env.get("RUST_BACKTRACE").map(String::as_str), Some("1"));

        let r = resolve(
            &file,
            &args(&[
                "gpu-tests",
                "--timeout",
                "1h",
                "--priority",
                "1",
                "--gpu",
                "5080",
                "--ram",
                "2",
                "--env",
                "SHARED=flag",
                "--on",
                "os=macos",
                "--",
                "echo",
            ]),
        )
        .unwrap();
        assert_eq!(r.cmd, ["echo"]);
        assert_eq!(
            (r.timeout_ms, r.priority, r.request.ram_mib),
            (3_600_000, 1, 2048)
        );
        assert_eq!(r.request.gpus, ["5080"]);
        assert!(
            r.request.exclusive,
            "a flag can't turn the recipe's exclusive off"
        );
        assert_eq!(r.env.get("SHARED").map(String::as_str), Some("flag"));
        assert_eq!(r.on.get("os").map(String::as_str), Some("macos"));
    }

    #[test]
    fn bad_files_and_options_are_refused() {
        assert!(parse("[recipe.x]\ncmd = []\n").is_err());
        assert!(parse("[recipe.x]\ncmd = [\"a\"]\ngpus = [\"any\"]\n").is_err());
        assert!(parse("unknown = 1\n").is_err());
        let e = parse("[recipe.x]\ncmd = [\"a\"]\non = [\"gpu=nvidia\", \"gpu=amd\"]\n")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("gpu=amd: gpu is already required (gpu=nvidia)"),
            "{e}"
        );
        let file = parse("[recipe.x]\ncmd = [\"a\"]\ntimeout = \"600\"\npriority = 0\n").unwrap();
        let e = resolve(&file, &args(&["x"])).unwrap_err().to_string();
        assert!(
            e.contains("recipe x: timeout") && e.contains("use units"),
            "{e}"
        );
        let e = resolve(&file, &args(&["x", "--timeout", "1m"]))
            .unwrap_err()
            .to_string();
        assert!(e.contains("priority 0 is out of range"), "{e}");
        assert!(parse_hold_ms("24h").is_ok() && parse_hold_ms("24h1s").is_err());
        // Each error names the option, or the recipe's field, it is about.
        let plain = ProjectFile::default();
        for (argv, want) in [
            (&["--hold", "5"][..], "--hold: bad duration \"5\""),
            (&["--hold", "30h"], "--hold: hold 30h is too long"),
            (&["--ram", "x"], "--ram: bad RAM amount \"x\""),
            (
                &["--priority", "9"],
                "--priority: priority 9 is out of range",
            ),
            (&["--cpu", "0"], "--cpu: cpu must be at least 1"),
        ] {
            let e = err(&plain, &with(argv));
            assert!(e.starts_with(want), "{argv:?}: {e}");
        }
        let file = parse(
            "[recipe.x]\ncmd = [\"a\"]\ntimeout = \"1m\"\nhold = \"5\"\nram = \"x\"\npriority = 9\ncpu = 0\n",
        )
        .unwrap();
        let e = err(&file, &["x", "--ram", "1", "--priority", "1", "--cpu", "1"]);
        assert!(e.starts_with("recipe x: hold: bad duration \"5\""), "{e}");
        let e = err(&file, &["x", "--priority", "1", "--cpu", "1"]);
        assert!(e.starts_with("recipe x: ram: bad RAM amount"), "{e}");
        let e = err(&file, &["x", "--ram", "1", "--cpu", "1", "--hold", "5m"]);
        assert!(
            e.starts_with("recipe x: priority: priority 9 is out of range"),
            "{e}"
        );
        let e = err(
            &file,
            &["x", "--ram", "1", "--priority", "1", "--hold", "5m"],
        );
        assert!(
            e.starts_with("recipe x: cpu: cpu must be at least 1"),
            "{e}"
        );
        assert_eq!(
            parse_kv("A=b=c", "--env").unwrap(),
            ("A".into(), "b=c".into())
        );
        assert!(parse_kv("=x", "--env").is_err());
    }

    /// `dispatch --timeout 1m EXTRA -- true`.
    fn with<'a>(extra: &[&'a str]) -> Vec<&'a str> {
        let mut a = vec!["--timeout", "1m"];
        a.extend_from_slice(extra);
        a.extend_from_slice(&["--", "true"]);
        a
    }

    fn on(r: &Resolved) -> Vec<(&str, &str)> {
        r.on.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
    }

    fn err(file: &ProjectFile, argv: &[&str]) -> String {
        resolve(file, &args(argv)).unwrap_err().to_string()
    }

    #[test]
    fn os_shorthands_on_the_command_line() {
        let file = ProjectFile::default();
        let r = resolve(&file, &args(&with(&["--on", "macos"]))).unwrap();
        assert_eq!(on(&r), [("macos", "")]);
        let r = resolve(
            &file,
            &args(&with(&["--on", "macos=27", "--on", "arch=aarch64"])),
        )
        .unwrap();
        assert_eq!(on(&r), [("arch", "aarch64"), ("macos", "27")]);
        let r = resolve(
            &file,
            &args(&with(&["--on", "linux=7.2", "--on", "os_version=ubuntu"])),
        )
        .unwrap();
        assert_eq!(on(&r), [("linux", "7.2"), ("os_version", "ubuntu")]);
        // Compatible requirements on the same fact are kept.
        let r = resolve(
            &file,
            &args(&with(&["--on", "linux=7", "--on", "kernel=7.2.8"])),
        )
        .unwrap();
        assert_eq!(on(&r), [("kernel", "7.2.8"), ("linux", "7")]);
        // The same requirement twice is one.
        let r = resolve(
            &file,
            &args(&with(&["--on", "gpu=nvidia", "--on", "gpu=nvidia"])),
        )
        .unwrap();
        assert_eq!(on(&r), [("gpu", "nvidia")]);

        for (extra, want) in [
            (
                &["--on", "macos", "--on", "linux"][..],
                "--on: macos and linux contradict each other: a machine runs one OS",
            ),
            (
                &["--on", "linux=7.2", "--on", "os=macos"],
                "--on: linux=7.2 and os=macos contradict each other: a machine runs one OS",
            ),
            (
                &["--on", "macos=26", "--on", "os_version=27"],
                "--on: macos=26 and os_version=27 contradict each other: no os_version starts with both",
            ),
            (
                &["--on", "gpu"],
                "--on: gpu needs a value, gpu=VALUE (only macos and linux stand alone)",
            ),
            (
                &["--on", "kernel="],
                "--on: kernel needs a value, kernel=VALUE (only macos and linux stand alone)",
            ),
            (
                &["--on", "macos=."],
                "--on: macos=. gives no version: versions are compared by their parts, such as 27 or 7.2",
            ),
            (
                &["--on", "=27"],
                "--on \"=27\": expected KEY=VALUE, or macos or linux alone",
            ),
            (
                &["--on", "windows"],
                "--on: unknown fact \"windows\": use one of macos, linux, gpu,",
            ),
            (
                &["--on", "gpu=nvidia", "--on", "gpu=amd"],
                "--on gpu=amd: gpu is already required (gpu=nvidia); each fact takes one value",
            ),
        ] {
            let e = err(&file, &with(extra));
            assert!(e.starts_with(want), "{extra:?}: {e}");
        }
    }

    #[test]
    fn os_shorthands_in_recipes() {
        let file = parse(
            r#"
[recipe.mac]
cmd = ["true"]
timeout = "1m"
on = { macos = "27", gpu = "apple" }

[recipe.any-mac]
cmd = ["true"]
timeout = "1m"
on = ["macos", "gpu=apple"]

[recipe.kernel]
cmd = ["true"]
timeout = "1m"
on = ["linux=7.2", "os_version=cachyos"]

[recipe.old]
cmd = ["true"]
timeout = "1m"
on = { os = "macos", os_version = "26" }

[recipe.both]
cmd = ["true"]
timeout = "1m"
on = ["macos", "linux"]

[recipe.bare-gpu]
cmd = ["true"]
timeout = "1m"
on = ["gpu"]
"#,
        )
        .unwrap();
        let r = resolve(&file, &args(&["mac"])).unwrap();
        assert_eq!(on(&r), [("gpu", "apple"), ("macos", "27")]);
        let r = resolve(&file, &args(&["any-mac"])).unwrap();
        assert_eq!(on(&r), [("gpu", "apple"), ("macos", "")]);
        let r = resolve(&file, &args(&["kernel"])).unwrap();
        assert_eq!(on(&r), [("linux", "7.2"), ("os_version", "cachyos")]);

        // An --on naming the OS replaces the recipe's OS; the other facts stay.
        let r = resolve(&file, &args(&["mac", "--on", "linux"])).unwrap();
        assert_eq!(on(&r), [("gpu", "apple"), ("linux", "")]);
        let r = resolve(&file, &args(&["mac", "--on", "macos=26"])).unwrap();
        assert_eq!(on(&r), [("gpu", "apple"), ("macos", "26")]);
        let r = resolve(&file, &args(&["kernel", "--on", "os=linux"])).unwrap();
        assert_eq!(on(&r), [("os", "linux"), ("os_version", "cachyos")]);
        let r = resolve(&file, &args(&["mac", "--on", "gpu=any"])).unwrap();
        assert_eq!(on(&r), [("gpu", "any"), ("macos", "27")]);

        assert_eq!(
            err(&file, &["old", "--on", "macos=27"]),
            "--on with recipe old's on: macos=27 and os_version=26 contradict each other: no os_version starts with both"
        );
        assert_eq!(
            err(&file, &["both"]),
            "recipe both: on: macos and linux contradict each other: a machine runs one OS"
        );
        assert_eq!(
            err(&file, &["bare-gpu"]),
            "recipe bare-gpu: on: gpu needs a value, gpu=VALUE (only macos and linux stand alone)"
        );
        // A recipe that isn't dispatched doesn't stop others.
        assert!(resolve(&file, &args(&["--timeout", "1m", "--", "true"])).is_ok());

        let e = parse("[recipe.x]\ncmd = [\"a\"]\non = \"macos\"\n")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains(r#"a table of facts such as { macos = "27" }, or a list"#),
            "{e}"
        );
        let e = parse("[recipe.x]\ncmd = [\"a\"]\non = { macos = 27 }\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("expected a string"), "{e}");
    }
}
