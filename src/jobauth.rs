//! Signed jobs. The command line signs what it dispatches with its user key
//! ([`crate::mesh::user`]); in a cluster with users, a machine queues, grants and runs only what a
//! user it recognizes signed.
//!
//! The signature covers the spec as the command line sends it (its repository ids not salted yet,
//! `auth` None, `submitted_from` the local daemon's name), a nonce and when it was signed:
//! serde_json of `("potluck job v1", spec, nonce, issued_at)`. The spec's text travels with the
//! signature ([`JobAuth::signed_spec`]), so a machine checks the very bytes that were signed, on
//! any version, and then compares the spec it was given with the one it derives from them
//! ([`derived`]): the owner's salting of repository ids is the only change a machine makes.
//!
//! `potluck resize` signs a queued job as its owner holds it, with its new request: its ids
//! salted already, which a runner takes as they are ([`derived`]).

use crate::mesh::identity::{PublicKey, Signature, Signer};
use crate::types::{JobSpec, Millis};
use serde::{Deserialize, Serialize};

/// The tag the signed bytes start with.
pub const JOB_TAG: &str = "potluck job v1";
/// How far from the owner's clock a dispatch's signing time may be.
pub const MAX_SKEW_MS: u64 = 10 * crate::types::MINUTE_MS;
/// How long the owner remembers a dispatch's nonce: one seen before is a replay.
pub const NONCE_KEEP_MS: u64 = 24 * crate::types::HOUR_MS;

/// A user's signature over a dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAuth {
    /// The user's public key.
    pub key: PublicKey,
    /// Random, new for every dispatch ([`crate::mesh::trust::nonce`]).
    pub nonce: String,
    /// When the command line signed it (its clock, ms since the Unix epoch).
    pub issued_at: Millis,
    pub sig: Signature,
    /// The spec the command line signed, as the serde_json text it signed. Empty where the spec
    /// isn't the reader's to see (another member's report of a queued job): the signature can't
    /// be checked there, only who made it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signed_spec: String,
}

/// The bytes a dispatch's signature covers: serde_json of `(JOB_TAG, spec, nonce, issued_at)`,
/// with `spec_json` (the spec's serde_json text) as it is.
pub fn signed_bytes(spec_json: &str, nonce: &str, issued_at: Millis) -> Vec<u8> {
    let text = |v: &str| serde_json::to_string(v).expect("a string serializes");
    format!(
        "[{},{spec_json},{},{issued_at}]",
        text(JOB_TAG),
        text(nonce)
    )
    .into_bytes()
}

/// `spec` signed by `key` at `issued_at`, with a new nonce: what the command line sends.
pub fn sign(spec: &JobSpec, key: &dyn Signer, issued_at: Millis) -> JobSpec {
    sign_with_nonce(spec, key, crate::mesh::trust::nonce(), issued_at)
}

/// [`sign`] with the nonce given.
pub fn sign_with_nonce(
    spec: &JobSpec,
    key: &dyn Signer,
    nonce: String,
    issued_at: Millis,
) -> JobSpec {
    let unsigned = JobSpec {
        auth: None,
        ..spec.clone()
    };
    let signed_spec = serde_json::to_string(&unsigned).expect("a spec serializes");
    let sig = key.sign(&signed_bytes(&signed_spec, &nonce, issued_at));
    JobSpec {
        auth: Some(JobAuth {
            key: key.public(),
            nonce,
            issued_at,
            sig,
            signed_spec,
        }),
        ..unsigned
    }
}

/// The spec `auth` covers, when its signature holds.
pub fn open(auth: &JobAuth) -> Result<JobSpec, String> {
    if auth.signed_spec.is_empty() {
        return Err("its signature comes without the spec it covers".into());
    }
    let bytes = signed_bytes(&auth.signed_spec, &auth.nonce, auth.issued_at);
    if !auth.key.verify(&bytes, &auth.sig) {
        return Err(format!("its signature by key {} doesn't verify", auth.key));
    }
    let signed: JobSpec = serde_json::from_str(&auth.signed_spec)
        .map_err(|e| format!("the spec its signature covers can't be read: {e}"))?;
    if signed.auth.is_some() {
        return Err("the spec its signature covers holds a signature itself".into());
    }
    Ok(signed)
}

/// The spec as its owner holds it, made from the spec the command line sent: its repository ids
/// salted with `owner` ([`crate::gitinfo::owned_repo`]). `pre_salting`, for a dispatch into a
/// workspace made before ids were salted: the repository and submodule ids stay the command
/// line's (that workspace's mirrors and pins have them); carried repositories, in no workspace,
/// are salted all the same.
pub fn owned(owner: &str, spec: &JobSpec, pre_salting: bool) -> JobSpec {
    let mut spec = spec.clone();
    spec.repo = if pre_salting {
        crate::types::RepoRef {
            carried: crate::gitinfo::owned_carried(owner, &spec.repo.carried),
            ..spec.repo
        }
    } else {
        crate::gitinfo::owned_repo(owner, &spec.repo)
    };
    spec
}

/// Whether `effective` (a spec as its owner holds it, signature included) is the one the user
/// signed, `signed`: equal, field by field, to [`owned`] of `signed` with `signed`'s
/// `submitted_from` as the owner. A spec pinned to a workspace whose repository id is the signed,
/// unsalted one may be the [`owned`] form from before salting. A signed spec whose ids are salted
/// already ([`salted_already`]: signed as its owner held it, by `potluck resize`) must be
/// `effective` itself, salted no second time: the user signed that very spec for that owner.
pub fn derived(signed: &JobSpec, effective: &JobSpec) -> Result<(), String> {
    let effective = JobSpec {
        auth: None,
        ..effective.clone()
    };
    if salted_already(signed) {
        if effective == *signed {
            return Ok(());
        }
        return Err(format!(
            "its {} is not what its signature covers",
            differing(signed, &effective).join(", ")
        ));
    }
    let owner = &signed.submitted_from;
    let salted = owned(owner, signed, false);
    if effective == salted {
        return Ok(());
    }
    let pre_salting = effective.workspace.is_some()
        && crate::types::is_unsalted_repo_id(&signed.repo.id)
        && effective.repo.id == signed.repo.id;
    if pre_salting && effective == owned(owner, signed, true) {
        return Ok(());
    }
    Err(format!(
        "its {} is not what its signature covers",
        differing(&salted, &effective).join(", ")
    ))
}

/// Whether `spec`'s repository ids are salted already, as its owner holds them ([`owned`]): its
/// carried repositories' all, and its own repository's (its submodules' derive from it); or, for
/// a job pinned to a workspace from before salting, which keeps its own unsalted id, its carried
/// repositories', of which it has some. A spec the command line sends has none salted
/// ([`crate::types::repo_id`]).
pub fn salted_already(spec: &JobSpec) -> bool {
    use crate::types::{is_owned_repo_id, is_unsalted_repo_id};
    let carried = &spec.repo.carried;
    let top = &spec.repo.id;
    carried.iter().all(|c| is_owned_repo_id(&c.repo_id))
        && (is_owned_repo_id(top)
            || (spec.workspace.is_some() && is_unsalted_repo_id(top) && !carried.is_empty()))
}

/// The fields in which `a` and `b` differ, by their names in JSON.
fn differing(a: &JobSpec, b: &JobSpec) -> Vec<String> {
    let (a, b) = (
        serde_json::to_value(a).unwrap_or_default(),
        serde_json::to_value(b).unwrap_or_default(),
    );
    let empty = serde_json::Map::new();
    let (a, b) = (
        a.as_object().unwrap_or(&empty),
        b.as_object().unwrap_or(&empty),
    );
    let mut keys: Vec<String> = a
        .keys()
        .chain(b.keys())
        .filter(|k| a.get(*k) != b.get(*k))
        .cloned()
        .collect();
    keys.sort();
    keys.dedup();
    if keys.is_empty() {
        keys.push("spec".into());
    }
    keys
}

/// What a runner checks of a job granted by `owner`, before it runs it: a user `recognized` signed
/// it, the signature holds, it was signed for `owner`, and `spec` derives from the spec signed
/// ([`derived`]). Returns the spec signed.
pub fn verify_grant(
    spec: &JobSpec,
    owner: &str,
    recognized: impl Fn(&PublicKey) -> bool,
) -> Result<JobSpec, String> {
    let Some(auth) = &spec.auth else {
        return Err("it is unsigned".into());
    };
    if !recognized(&auth.key) {
        return Err(format!(
            "it is signed by key {}, which is no user of this cluster",
            auth.key
        ));
    }
    let signed = open(auth)?;
    if signed.submitted_from != owner {
        return Err(format!(
            "it was signed for {:?}, not for {owner}, which granted it",
            signed.submitted_from
        ));
    }
    derived(&signed, spec)?;
    Ok(signed)
}

/// What an owner checks of a signed dispatch made to it, `node`, at `now` (its clock): the
/// signature holds, covers this very spec, was made for this machine, and not more than
/// [`MAX_SKEW_MS`] from `now`. Who signed it, and whether its nonce was seen before, the caller
/// checks.
pub fn check_dispatch<'a>(
    spec: &'a JobSpec,
    node: &str,
    now: Millis,
) -> Result<&'a JobAuth, String> {
    let Some(auth) = &spec.auth else {
        return Err("it is unsigned".into());
    };
    let nonce_ok = (16..=64).contains(&auth.nonce.len())
        && auth
            .nonce
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !nonce_ok {
        return Err(format!(
            "its nonce {:?} is not one potluck makes",
            auth.nonce
        ));
    }
    let signed = open(auth)?;
    let sent = JobSpec {
        auth: None,
        ..spec.clone()
    };
    if signed != sent {
        return Err(format!(
            "its {} is not what its signature covers",
            differing(&signed, &sent).join(", ")
        ));
    }
    if signed.submitted_from != node {
        return Err(format!(
            "it was signed for machine {:?}, not this one, {node}",
            signed.submitted_from
        ));
    }
    if auth.issued_at.abs_diff(now) > MAX_SKEW_MS {
        return Err(format!(
            "it was signed at {}, more than {} from this machine's clock ({}): is a clock off?",
            crate::types::fmt_local_time(auth.issued_at),
            crate::types::fmt_ms(MAX_SKEW_MS),
            crate::types::fmt_local_time(now)
        ));
    }
    Ok(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::user::UserKey;
    use crate::types::{CarriedRef, RepoRef, Request, SubmoduleRef};
    use std::path::PathBuf;

    fn key(dir: &std::path::Path, name: &str) -> UserKey {
        UserKey::load_or_create(&dir.join(name)).unwrap().0
    }

    fn spec() -> JobSpec {
        JobSpec {
            key: Some("k".into()),
            labels: [("team".to_string(), "x".to_string())].into(),
            priority: 3,
            request: Request::default(),
            on: [("os".to_string(), "linux".to_string())].into(),
            repo: RepoRef {
                id: "app-0011aabb".into(),
                source: PathBuf::from("/src/app/.git"),
                commit: "0123456789abcdef0123456789abcdef01234567".into(),
                branch: Some("feature".into()),
                submodules: vec![SubmoduleRef {
                    path: "vendor/lib".into(),
                    repo_id: crate::gitinfo::submodule_repo_id("app-0011aabb", "vendor/lib"),
                    source: PathBuf::from("/src/app/.git/modules/vendor/lib"),
                    commit: "1".repeat(40),
                }],
                carried: vec![CarriedRef {
                    name: "dep".into(),
                    repo_id: "dep-0022ccdd".into(),
                    source: PathBuf::from("/src/dep/.git"),
                    commit: "2".repeat(40),
                }],
            },
            workspace: None,
            advance: false,
            cmd: vec!["cargo".into(), "test".into()],
            env: [("A".to_string(), "1".to_string())].into(),
            unset_env: ["HTTPS_PROXY".to_string()].into(),
            timeout_ms: 600_000,
            hold_ms: None,
            keep: vec!["out/*".into()],
            rerun_if_lost: true,
            kill_on_stall: false,
            recipe: None,
            submitted_from: "studio".into(),
            pools: vec!["cloud".into()],
            auth: None,
        }
    }

    /// The bytes signed are serde_json of the tuple, the spec's text as serde_json writes it.
    #[test]
    fn the_signed_bytes_are_the_tuple_as_serde_json_writes_it() {
        let s = spec();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            signed_bytes(&json, "n0nce-n0nce-n0nce", 42),
            serde_json::to_vec(&(JOB_TAG, &s, "n0nce-n0nce-n0nce", 42u64)).unwrap()
        );
        // A nonce with what JSON escapes is written as JSON writes it.
        assert_eq!(
            signed_bytes("{}", "a\"b", 1),
            serde_json::to_vec(&(JOB_TAG, serde_json::json!({}), "a\"b", 1u64)).unwrap()
        );
    }

    /// A spec's maps are ordered ones (BTreeMap, BTreeSet), so the same spec always serializes to
    /// the same bytes: what the command line signs can be made again from the spec. A HashMap
    /// anywhere in a spec breaks this test (its order changes from one map to the next).
    #[test]
    fn a_spec_serializes_to_the_same_bytes_every_time() {
        let mut s = spec();
        for i in 0..64 {
            s.labels.insert(format!("l{i}"), i.to_string());
            s.env.insert(format!("E{i}"), i.to_string());
            s.on.insert(format!("o{i}"), i.to_string());
            s.unset_env.insert(format!("U{i}"));
        }
        let first = serde_json::to_string(&s).unwrap();
        for _ in 0..16 {
            // A copy built afresh, its maps filled in another order.
            let mut t = s.clone();
            t.labels = s
                .labels
                .iter()
                .rev()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            t.env = s
                .env
                .iter()
                .rev()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            t.on =
                s.on.iter()
                    .rev()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
            t.unset_env = s.unset_env.iter().rev().cloned().collect();
            let back: JobSpec = serde_json::from_str(&first).unwrap();
            assert_eq!(serde_json::to_string(&t).unwrap(), first);
            assert_eq!(serde_json::to_string(&back).unwrap(), first);
        }
        // Each map's keys come out sorted.
        let v: serde_json::Value = serde_json::from_str(&first).unwrap();
        for field in ["labels", "env", "on"] {
            let keys: Vec<&String> = v[field].as_object().unwrap().keys().collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "{field}");
        }
        let unset: Vec<&str> = v["unset_env"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        let mut sorted = unset.clone();
        sorted.sort();
        assert_eq!(unset, sorted);
    }

    #[test]
    fn a_signed_spec_opens_and_any_change_to_what_was_signed_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "user.key");
        let signed = sign(&spec(), &k, 1_000);
        let auth = signed.auth.clone().unwrap();
        assert_eq!(auth.key, k.public());
        assert_eq!(open(&auth).unwrap(), spec());
        let other = key(tmp.path(), "other.key");

        let mut bad = auth.clone();
        bad.nonce.push('x');
        assert!(open(&bad).unwrap_err().contains("doesn't verify"));
        let mut bad = auth.clone();
        bad.issued_at += 1;
        assert!(open(&bad).unwrap_err().contains("doesn't verify"));
        let mut bad = auth.clone();
        bad.key = other.public();
        assert!(open(&bad).unwrap_err().contains("doesn't verify"));
        let mut bad = auth.clone();
        bad.sig = other.sign(&signed_bytes(&bad.signed_spec, &bad.nonce, bad.issued_at));
        assert!(open(&bad).unwrap_err().contains("doesn't verify"));
        let mut bad = auth.clone();
        bad.signed_spec.clear();
        assert!(open(&bad).unwrap_err().contains("without the spec"));
        // Each field of the spec signed.
        type Change = Box<dyn Fn(&mut JobSpec)>;
        let tampered: Vec<Change> = vec![
            Box::new(|s| s.key = None),
            Box::new(|s| {
                s.labels.insert("b".into(), "c".into());
            }),
            Box::new(|s| s.priority = 5),
            Box::new(|s| s.request.ram_mib += 1),
            Box::new(|s| s.request.gpus.push("any".into())),
            Box::new(|s| {
                s.on.remove("os");
            }),
            Box::new(|s| s.repo.id = "app-0011aabc".into()),
            Box::new(|s| s.repo.source = PathBuf::from("/elsewhere/.git")),
            Box::new(|s| s.repo.commit = "f".repeat(40)),
            Box::new(|s| s.repo.branch = None),
            Box::new(|s| s.repo.submodules[0].commit = "3".repeat(40)),
            Box::new(|s| s.repo.carried[0].repo_id = "dep-0022ccde".into()),
            Box::new(|s| s.workspace = Some("msi-w1".into())),
            Box::new(|s| s.advance = true),
            Box::new(|s| s.cmd.push("--all".into())),
            Box::new(|s| {
                s.env.insert("A".into(), "2".into());
            }),
            Box::new(|s| {
                s.unset_env.clear();
            }),
            Box::new(|s| s.timeout_ms += 1),
            Box::new(|s| s.hold_ms = Some(600_000)),
            Box::new(|s| s.keep.clear()),
            Box::new(|s| s.rerun_if_lost = false),
            Box::new(|s| s.kill_on_stall = true),
            Box::new(|s| s.recipe = Some("r".into())),
            Box::new(|s| s.submitted_from = "msi".into()),
            Box::new(|s| s.pools = vec!["default".into()]),
        ];
        for (i, change) in tampered.iter().enumerate() {
            let mut s: JobSpec = serde_json::from_str(&auth.signed_spec).unwrap();
            change(&mut s);
            let mut bad = auth.clone();
            bad.signed_spec = serde_json::to_string(&s).unwrap();
            assert!(
                open(&bad).unwrap_err().contains("doesn't verify"),
                "change {i}"
            );
            // The spec sent beside an intact signature: the owner sees it isn't what was signed.
            let sent = JobSpec {
                auth: Some(auth.clone()),
                ..s
            };
            assert!(
                check_dispatch(&sent, "studio", 1_000)
                    .unwrap_err()
                    .contains("is not what its signature covers"),
                "change {i}"
            );
        }
    }

    #[test]
    fn an_owner_takes_a_dispatch_signed_for_it_now() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "user.key");
        let signed = sign(&spec(), &k, 1_000_000);
        assert_eq!(
            check_dispatch(&signed, "studio", 1_000_000).unwrap().key,
            k.public()
        );
        let skew = MAX_SKEW_MS;
        assert!(check_dispatch(&signed, "studio", 1_000_000 + skew).is_ok());
        assert!(check_dispatch(&signed, "studio", 1_000_000 - skew).is_ok());
        for off in [1_000_000 + skew + 1, 1_000_000 - skew - 1] {
            let e = check_dispatch(&signed, "studio", off).unwrap_err();
            assert!(e.contains("more than 10m from this machine's clock"), "{e}");
        }
        let e = check_dispatch(&signed, "msi", 1_000_000).unwrap_err();
        assert!(
            e.contains("signed for machine \"studio\", not this one, msi"),
            "{e}"
        );
        let e = check_dispatch(&spec(), "studio", 1_000_000).unwrap_err();
        assert_eq!(e, "it is unsigned");
        let mut short = signed.clone();
        short.auth.as_mut().unwrap().nonce = "abc".into();
        let e = check_dispatch(&short, "studio", 1_000_000).unwrap_err();
        assert!(e.contains("is not one potluck makes"), "{e}");
        // Nonces differ from one signing to the next.
        let again = sign(&spec(), &k, 1_000_000);
        assert_ne!(again.auth.unwrap().nonce, signed.auth.unwrap().nonce);
    }

    /// A runner derives the spec it was granted from the one signed: the owner salted the ids
    /// with its name, or (into a workspace from before salting) kept the unsalted ones but the
    /// carried repositories'.
    #[test]
    fn a_granted_spec_derives_from_the_signed_one() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "user.key");
        let yes = |_: &PublicKey| true;
        let signed = sign(&spec(), &k, 1_000);
        let auth = signed.auth.clone();
        let effective = JobSpec {
            auth: auth.clone(),
            ..owned("studio", &spec(), false)
        };
        assert!(crate::types::is_owned_repo_id(&effective.repo.id));
        assert_ne!(
            effective.repo.submodules[0].repo_id,
            spec().repo.submodules[0].repo_id
        );
        assert_eq!(verify_grant(&effective, "studio", yes).unwrap(), spec());
        // Its signature checked as the runner gets it: through JSON.
        let through: JobSpec =
            serde_json::from_str(&serde_json::to_string(&effective).unwrap()).unwrap();
        assert!(verify_grant(&through, "studio", yes).is_ok());

        let e = verify_grant(&effective, "msi", yes).unwrap_err();
        assert!(e.contains("signed for \"studio\", not for msi"), "{e}");
        let e = verify_grant(&effective, "studio", |_| false).unwrap_err();
        assert!(e.contains("which is no user of this cluster"), "{e}");
        let e = verify_grant(&spec(), "studio", yes).unwrap_err();
        assert_eq!(e, "it is unsigned");
        // The spec unsalted, or salted with another owner's name, is not what the owner holds.
        let unsalted = JobSpec {
            auth: auth.clone(),
            ..spec()
        };
        let e = verify_grant(&unsalted, "studio", yes).unwrap_err();
        assert_eq!(e, "its repo is not what its signature covers");
        let elsewhere = JobSpec {
            auth: auth.clone(),
            ..owned("msi", &spec(), false)
        };
        assert!(verify_grant(&elsewhere, "studio", yes).is_err());
        // Each field changed after signing.
        let mut cmd = effective.clone();
        cmd.cmd = vec!["rm".into(), "-rf".into(), "/".into()];
        let e = verify_grant(&cmd, "studio", yes).unwrap_err();
        assert_eq!(e, "its cmd is not what its signature covers");
        let mut env = effective.clone();
        env.env.insert("LD_PRELOAD".into(), "x".into());
        env.pools.push("default".into());
        let e = verify_grant(&env, "studio", yes).unwrap_err();
        assert_eq!(e, "its env, pools is not what its signature covers");
        let mut key_gone = effective.clone();
        key_gone.key = None;
        assert!(verify_grant(&key_gone, "studio", yes).is_err());

        // Into a workspace from before salting: the unsalted ids stay, the carried ones are
        // salted.
        let pinned = JobSpec {
            workspace: Some("msi-w3".into()),
            ..spec()
        };
        let signed = sign(&pinned, &k, 1_000);
        let kept = JobSpec {
            auth: signed.auth.clone(),
            ..owned("studio", &pinned, true)
        };
        assert_eq!(kept.repo.id, pinned.repo.id);
        assert_ne!(kept.repo.carried[0].repo_id, pinned.repo.carried[0].repo_id);
        assert!(verify_grant(&kept, "studio", yes).is_ok());
        let salted = JobSpec {
            auth: signed.auth.clone(),
            ..owned("studio", &pinned, false)
        };
        assert!(verify_grant(&salted, "studio", yes).is_ok());
        // Not salted at all: the carried ids are wrong.
        let raw = JobSpec {
            auth: signed.auth.clone(),
            ..pinned.clone()
        };
        assert!(verify_grant(&raw, "studio", yes).is_err());
        // The pre-salting form only for a job pinned to a workspace.
        let loose = sign(&spec(), &k, 1_000);
        let kept = JobSpec {
            auth: loose.auth.clone(),
            ..owned("studio", &spec(), true)
        };
        assert!(verify_grant(&kept, "studio", yes).is_err());
    }

    /// `potluck resize` signs a job as its owner holds it, its ids salted already: a runner
    /// takes that spec as it is, and not salted a second time, nor unsalted. So too a job pinned to
    /// a workspace from before salting (its own ids unsalted, its carried ones salted).
    #[test]
    fn a_spec_signed_as_its_owner_holds_it_is_taken_as_it_is() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "user.key");
        let yes = |_: &PublicKey| true;
        let held = owned("studio", &spec(), false);
        assert!(salted_already(&held) && !salted_already(&spec()));
        let signed = sign(&held, &k, 1_000);
        assert_eq!(verify_grant(&signed, "studio", yes).unwrap(), held);
        let twice = JobSpec {
            auth: signed.auth.clone(),
            ..owned("studio", &held, false)
        };
        assert_ne!(twice.repo.id, held.repo.id);
        let e = verify_grant(&twice, "studio", yes).unwrap_err();
        assert_eq!(e, "its repo is not what its signature covers");
        let unsalted = JobSpec {
            auth: signed.auth.clone(),
            ..spec()
        };
        assert!(verify_grant(&unsalted, "studio", yes).is_err());
        // Any other change is one still.
        let mut cmd = signed.clone();
        cmd.cmd.push("--all".into());
        assert!(verify_grant(&cmd, "studio", yes).is_err());
        // For another owner than the one that holds it, no.
        assert!(verify_grant(&signed, "msi", yes).is_err());

        let pinned = JobSpec {
            workspace: Some("msi-w3".into()),
            ..spec()
        };
        let kept = owned("studio", &pinned, true);
        assert!(salted_already(&kept) && !salted_already(&pinned));
        let signed = sign(&kept, &k, 1_000);
        assert_eq!(verify_grant(&signed, "studio", yes).unwrap(), kept);
        let carried_twice = JobSpec {
            auth: signed.auth.clone(),
            ..owned("studio", &kept, true)
        };
        assert!(verify_grant(&carried_twice, "studio", yes).is_err());
        // Unpinned, an unsalted id is the command line's: salted by the owner, not taken as is.
        let loose = JobSpec {
            workspace: None,
            ..kept
        };
        assert!(!salted_already(&loose));
        let signed = sign(&loose, &k, 1_000);
        assert!(verify_grant(&signed, "studio", yes).is_err());
    }

    /// A spec in a record from before signed jobs, or signed, loads; and the signature's text
    /// survives the trip through JSON a report or a grant makes.
    #[test]
    fn a_signature_goes_through_json_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "user.key");
        let signed = sign(&spec(), &k, 7);
        let json = serde_json::to_string(&signed).unwrap();
        let back: JobSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, signed);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["auth"]["issued_at"], 7);
        assert_eq!(v["auth"]["key"], k.public().to_text());
        assert!(v["auth"]["signed_spec"].as_str().unwrap().starts_with('{'));
        // Unsigned and without pools: neither is written.
        let plain = JobSpec {
            pools: vec![],
            ..spec()
        };
        let v = serde_json::to_value(&plain).unwrap();
        assert!(v.get("auth").is_none() && v.get("pools").is_none(), "{v}");
    }
}
