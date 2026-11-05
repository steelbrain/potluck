//! Several meshes in one process, on 127.0.0.1 with free ports and temporary key directories.

use super::*;
use serde_json::json;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Instant;

const HEARTBEAT: Duration = Duration::from_millis(100);
const DOWN_AFTER: Duration = Duration::from_millis(800);
const WAIT: Duration = Duration::from_secs(15);

/// Echoes calls; streams of kind "echo" send every byte back.
struct Echo;

impl Handler for Echo {
    fn call(&self, from: &Peer, msg: Value) -> Result<Value, String> {
        if msg.get("fail").is_some() {
            return Err("asked to fail".into());
        }
        if let Some(ms) = msg.get("sleep_ms").and_then(Value::as_u64) {
            thread::sleep(Duration::from_millis(ms));
        }
        Ok(json!({ "from": from.name, "echo": msg }))
    }

    fn stream(&self, _from: &Peer, header: Value, mut stream: Stream) {
        if header["kind"] == "upper" {
            // A child process piped through the stream, as the daemon will run git upload-pack.
            let mut child = std::process::Command::new("tr")
                .args(["a-z", "A-Z"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let (stdin, stdout) = (child.stdin.take().unwrap(), child.stdout.take().unwrap());
            stream.splice(stdout, stdin).unwrap();
            assert!(child.wait().unwrap().success());
            return;
        }
        if header["kind"] != "echo" {
            return;
        }
        let mut buf = vec![0u8; 50_000];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if stream.write_all(&buf[..n]).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        let _ = stream.close_write();
    }
}

/// A mesh stopped when dropped, with its event receiver.
struct Node {
    mesh: Mesh,
    events: Receiver<Event>,
    dir: PathBuf,
}

impl std::ops::Deref for Node {
    type Target = Mesh;
    fn deref(&self) -> &Mesh {
        &self.mesh
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.mesh.stop();
    }
}

fn config(name: &str, dir: &Path, version: &str) -> MeshConfig {
    MeshConfig {
        name: name.to_string(),
        keys_dir: dir.join("keys"),
        listen: "127.0.0.1:0".parse().unwrap(),
        address: Some("127.0.0.1".into()),
        version: version.to_string(),
        heartbeat: HEARTBEAT,
        down_after: DOWN_AFTER,
        updating_for: Duration::from_secs(600),
        connect_timeout: Duration::from_secs(2),
        call_timeout: Duration::from_secs(10),
    }
}

fn node(mesh: Mesh, dir: &Path) -> Node {
    let events = mesh.subscribe();
    Node {
        mesh,
        events,
        dir: dir.to_path_buf(),
    }
}

fn start_version(name: &str, dir: &Path, version: &str) -> Node {
    node(
        Mesh::start(config(name, dir, version), Arc::new(Echo)).unwrap(),
        dir,
    )
}

fn start(name: &str, root: &Path) -> Node {
    start_version(name, &root.join(name), "0.1.0")
}

fn restart(node: Node, name: &str, version: &str) -> Node {
    let dir = node.dir.clone();
    drop(node);
    start_version(name, &dir, version)
}

fn pair(member: &Mesh, new: &Mesh) {
    let request = new.pair_request().unwrap();
    let reply = member.admit(&request).unwrap();
    new.pair_finish(&reply).unwrap();
}

fn names(m: &Mesh) -> Vec<String> {
    let mut n: Vec<String> = m.members().into_iter().map(|m| m.name).collect();
    n.sort();
    n
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < WAIT, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_event(node: &Node, what: &str, pred: impl Fn(&Event) -> bool) -> Event {
    wait_on(&node.events, what, pred)
}

fn wait_on(events: &Receiver<Event>, what: &str, pred: impl Fn(&Event) -> bool) -> Event {
    let start = Instant::now();
    loop {
        let left = WAIT.saturating_sub(start.elapsed());
        match events.recv_timeout(left) {
            Ok(e) if pred(&e) => return e,
            Ok(_) => {}
            Err(_) => panic!("no event: {what}"),
        }
    }
}

#[test]
fn pairing_lets_two_machines_talk() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    assert_eq!(names(&a), ["a"]);
    assert_ne!(a.cluster(), b.cluster());

    let request = b.pair_request().unwrap();
    // Asking again gives the same request until it is used.
    assert_eq!(b.pair_request().unwrap(), request);
    let reply = a.admit(&request).unwrap();
    let members = b.pair_finish(&reply).unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(b.cluster(), a.cluster());
    assert_eq!(names(&a), ["a", "b"]);
    assert_eq!(names(&b), ["a", "b"]);
    // Finishing twice is harmless.
    b.pair_finish(&reply).unwrap();

    let r = b.call("a", json!({ "n": 1 })).unwrap();
    assert_eq!(r, json!({ "from": "b", "echo": { "n": 1 } }));
    let r = a.call("b", json!({ "n": 2 })).unwrap();
    assert_eq!(r["from"], "a");
    match a.call("b", json!({ "fail": true })) {
        Err(MeshError::Remote { node, message }) => {
            assert_eq!((node.as_str(), message.as_str()), ("b", "asked to fail"));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        a.call("zed", json!({})),
        Err(MeshError::NotMember(_))
    ));
    wait_event(&a, "b up on a", |e| *e == Event::Up { node: "b".into() });
    wait_until("a sees b alive", || a.member("b").is_some_and(|m| m.alive));
    let mb = a.member("b").unwrap();
    assert_eq!(mb.admitted_by, "a");
    assert_eq!(mb.version.as_deref(), Some("0.1.0"));
    assert!(!mb.this_machine && a.member("a").unwrap().this_machine);

    // The key file and the records are private.
    use std::os::unix::fs::PermissionsExt;
    for f in ["keys/node.key", "keys/members.json"] {
        let mode = std::fs::metadata(b.dir.join(f))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "{f}");
    }
}

#[test]
fn pairing_mistakes_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    let c = start("c", tmp.path());
    let other_b = start_version("b", &tmp.path().join("b2"), "0.1.0");

    // Admitting one's own request, garbage, or a request for a name already taken.
    let own = a.pair_request().unwrap();
    assert!(matches!(a.admit(&own), Err(MeshError::Pairing(_))));
    assert!(matches!(a.admit("hello"), Err(MeshError::Pairing(_))));
    pair(&a, &b);
    let dup = other_b.pair_request().unwrap();
    assert!(matches!(a.admit(&dup), Err(MeshError::Pairing(_))));

    // A reply meant for another machine, or no request pending.
    let req_c = c.pair_request().unwrap();
    let reply_c = a.admit(&req_c).unwrap();
    assert!(matches!(
        other_b.pair_finish(&reply_c),
        Err(MeshError::Pairing(_))
    ));
    assert!(matches!(
        b.pair_finish(&reply_c),
        Err(MeshError::Pairing(_))
    ));
    // A member of a cluster can't pair elsewhere without being removed first.
    assert!(matches!(b.pair_request(), Err(MeshError::Invalid(_))));
    // Another version can't be admitted.
    let newer = start_version("d", &tmp.path().join("d"), "0.2.0");
    match a.admit(&newer.pair_request().unwrap()) {
        Err(MeshError::VersionMismatch { node, ours, theirs }) => {
            assert_eq!(
                (node.as_str(), ours.as_str(), theirs.as_str()),
                ("d", "0.1.0", "0.2.0")
            );
        }
        other => panic!("{other:?}"),
    }
    c.pair_finish(&reply_c).unwrap();
    assert_eq!(names(&c), ["a", "b", "c"]);
}

#[test]
fn chain_of_trust_lets_members_talk_directly() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    let c = start("c", tmp.path());
    pair(&a, &b);
    pair(&b, &c);
    // c learned a from b's reply; a learns c from b's announcement.
    assert_eq!(names(&c), ["a", "b", "c"]);
    wait_until("a knows c", || names(&a) == ["a", "b", "c"]);
    assert_eq!(a.member("c").unwrap().admitted_by, "b");
    assert_eq!(a.call("c", json!("hi")).unwrap()["from"], "a");
    assert_eq!(c.call("a", json!("hi")).unwrap()["from"], "c");
    wait_event(&a, "c joined on a", |e| {
        *e == Event::Joined {
            node: "c".into(),
            by: "b".into(),
        }
    });
}

#[test]
fn a_strangers_key_is_refused_in_the_handshake() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let s = start("s", tmp.path());

    // The stranger dials a member: the member refuses its key during the handshake.
    match s.dial_raw("a", a.key(), &a.address()) {
        Err(MeshError::Handshake { message, .. }) => {
            assert!(message.contains("alert"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let e = wait_event(&a, "refused on a", |e| matches!(e, Event::Refused { .. }));
    let Event::Refused { reason, .. } = e else {
        unreachable!()
    };
    assert!(reason.contains("TLS handshake"), "{reason}");

    // A member dialing the stranger's address as if it were b refuses the stranger's key.
    match a.dial_raw("b", b.key(), &s.address()) {
        Err(MeshError::Handshake { message, .. }) => {
            assert!(message.to_lowercase().contains("certificate"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    // Members still talk.
    a.call("b", json!(1)).unwrap();
}

#[test]
fn a_removed_member_is_refused_everywhere_even_by_members_that_were_offline() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    let c = start("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b knows c", || names(&b) == ["a", "b", "c"]);
    wait_until("b reaches c", || b.call("c", json!(0)).is_ok());
    let old_b_key = b.key();

    // b and c go offline; a removes b.
    let c_dir = c.dir.clone();
    let b_dir = b.dir.clone();
    drop(b);
    drop(c);
    a.remove("b").unwrap();
    assert_eq!(names(&a), ["a", "c"]);
    wait_event(&a, "b removed on a", |e| {
        *e == Event::Removed {
            node: "b".into(),
            by: Some("a".into()),
        }
    });
    // Removing it again does nothing; a can't remove itself.
    a.remove("b").unwrap();
    assert!(matches!(a.remove("a"), Err(MeshError::Invalid(_))));

    // c comes back still listing b, and learns of the removal from a.
    let c = start_version("c", &c_dir, "0.1.0");
    wait_until("c drops b", || names(&c) == ["a", "c"]);
    assert!(matches!(
        c.call("b", json!(0)),
        Err(MeshError::NotMember(_))
    ));
    assert!(matches!(
        a.call("b", json!(0)),
        Err(MeshError::NotMember(_))
    ));

    // b comes back not knowing; whoever it reaches tells it, with the signed removal.
    let b = start_version("b", &b_dir, "0.1.0");
    wait_until("b learns it was removed", || b.is_removed());
    wait_event(&b, "self removed", |e| {
        *e == Event::SelfRemoved { by: "a".into() }
    });
    for node in ["a", "c"] {
        assert!(matches!(
            b.call(node, json!(0)),
            Err(MeshError::Removed { .. })
        ));
        // Dialing directly with the old key is refused by each member too.
        let m = if node == "a" { &a } else { &c };
        assert!(matches!(
            b.dial_raw(node, m.key(), &m.address()),
            Err(MeshError::Removed { .. })
        ));
    }

    // To come back, b pairs again with a new key.
    let request = b.pair_request().unwrap();
    assert_ne!(b.key(), old_b_key);
    let reply = c.admit(&request).unwrap();
    b.pair_finish(&reply).unwrap();
    assert!(!b.is_removed());
    // a lets the new key in once c's announcement of it arrives.
    wait_until("a knows the new b", || names(&a) == ["a", "b", "c"]);
    assert_eq!(b.call("a", json!(0)).unwrap()["from"], "b");
}

#[test]
fn a_removed_member_online_is_told_at_once() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    a.remove("b").unwrap();
    wait_event(&b, "self removed", |e| {
        *e == Event::SelfRemoved { by: "a".into() }
    });
    assert!(b.is_removed());
    assert!(matches!(
        b.call("a", json!(0)),
        Err(MeshError::Removed { .. })
    ));
}

/// The member that removes a machine dials it, pinned to its key, to tell it. A version the
/// removed machine answers with is no member's: the remover doesn't report it, so its daemon
/// doesn't update to it. (Its daemon updated to whatever version a removed machine that still
/// has its key, running a modified potluck, named.)
#[test]
fn a_removed_machine_answering_on_another_version_is_not_a_member_on_it() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let x = start_fixed("x", tmp.path());
    pair(&a, &x);
    a.call("x", json!(0)).unwrap();
    let (dir, addr) = place(&x);
    let x_key = x.key();
    drop(x);
    // x again, with its key, answering every hello with a refusal on version 9.9.9 once a has
    // removed it. Connections a makes before that (heartbeats) wait until then too.
    let id = identity::Identity::load(&dir.join("keys")).unwrap();
    let cfg = tls::server_config(&id, Arc::new(|_: &PublicKey| true)).unwrap();
    let listener = std::net::TcpListener::bind(addr).unwrap();
    listener.set_nonblocking(true).unwrap();
    let removed = Arc::new(AtomicBool::new(false));
    let answered = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let serving = {
        let (removed, answered, done) = (removed.clone(), answered.clone(), done.clone());
        thread::spawn(move || {
            let mut answering = Vec::new();
            while !done.load(Ordering::SeqCst) {
                let Ok((sock, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                };
                sock.set_nonblocking(false).unwrap();
                let (cfg, removed, answered) = (cfg.clone(), removed.clone(), answered.clone());
                answering.push(thread::spawn(move || {
                    wait_until("a removes x", || removed.load(Ordering::SeqCst));
                    let Ok(d) = tls::Duplex::accept(sock, cfg) else {
                        return;
                    };
                    d.set_read_timeout(Some(WAIT)).unwrap();
                    let Ok(Some(line)) = node::read_line_raw(&d, node::MAX_HELLO) else {
                        return;
                    };
                    let mut hello =
                        serde_json::from_slice::<Value>(&line).unwrap()["hello"].clone();
                    hello["node"] = json!("x");
                    hello["key"] = serde_json::to_value(x_key).unwrap();
                    hello["version"] = json!("9.9.9");
                    hello["address"] = json!(addr.to_string());
                    let reply = json!({ "refused": { "reason": "version", "hello": hello } });
                    let mut line = serde_json::to_vec(&reply).unwrap();
                    line.push(b'\n');
                    let mut sent = 0;
                    while sent < line.len() {
                        let Ok(n) = d.write(&line[sent..]) else {
                            return;
                        };
                        sent += n;
                    }
                    // a closes the connection once it has taken the answer.
                    while matches!(d.read(&mut [0u8; 256]), Ok(n) if n > 0) {}
                    answered.fetch_add(1, Ordering::SeqCst);
                }));
            }
            for t in answering {
                t.join().unwrap();
            }
        })
    };
    a.remove("x").unwrap();
    removed.store(true, Ordering::SeqCst);
    wait_until("x answers a", || answered.load(Ordering::SeqCst) > 0);
    done.store(true, Ordering::SeqCst);
    serving.join().unwrap();
    let mismatches: Vec<Event> = a
        .events
        .try_iter()
        .filter(|e| matches!(e, Event::VersionMismatch { .. }))
        .collect();
    assert!(
        mismatches.is_empty(),
        "a reports the version of a machine it removed: {mismatches:?}"
    );
    assert!(a.member("x").is_none());
}

#[test]
fn a_version_mismatch_is_refused_with_both_versions() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    a.call("b", json!(0)).unwrap();

    // b is updated and restarts on a newer version (and, here, another port: a learns it from
    // b's refused hello). Until then a dials b's old port, where nothing listens or, once the
    // port is reused, another test's machine whose key is not b's.
    let b = restart(b, "b", "0.2.0");
    let mut result = a.call("b", json!(0));
    wait_until("a reaches b's new port", || {
        result = a.call("b", json!(0));
        !matches!(
            result,
            Err(MeshError::Unreachable { .. } | MeshError::Handshake { .. })
        )
    });
    match result {
        Err(MeshError::VersionMismatch { node, ours, theirs }) => {
            assert_eq!(
                (node.as_str(), ours.as_str(), theirs.as_str()),
                ("b", "0.1.0", "0.2.0")
            );
            assert_eq!(
                compare_versions(&theirs, &ours),
                std::cmp::Ordering::Greater
            );
        }
        other => panic!("{other:?}"),
    }
    wait_event(&b, "mismatch on b", |e| {
        *e == Event::VersionMismatch {
            node: "a".into(),
            ours: "0.2.0".into(),
            theirs: "0.1.0".into(),
        }
    });
    wait_event(&a, "mismatch on a", |e| {
        *e == Event::VersionMismatch {
            node: "b".into(),
            ours: "0.1.0".into(),
            theirs: "0.2.0".into(),
        }
    });
    // Streams are refused the same way.
    assert!(matches!(
        b.open_stream("a", json!({ "kind": "echo" })),
        Err(MeshError::VersionMismatch { .. })
    ));
}

/// Answers on another version count as hearing from a member for `updating_for` only: past that,
/// a machine that hears from no member on its own version is silent (it compares records again
/// once it does), as one hearing from nobody is.
#[test]
fn answers_on_another_version_past_the_updating_time_are_silence() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config("a", &tmp.path().join("a"), "0.1.0");
    cfg.updating_for = Duration::from_secs(1);
    let a = node(
        Mesh::start(cfg, Arc::new(Echo)).unwrap(),
        &tmp.path().join("a"),
    );
    let b = start("b", tmp.path());
    pair(&a, &b);
    wait_until("a deals with b", || a.caught_up_for("b"));
    let b = restart(b, "b", "0.2.0");
    wait_event(
        &a,
        "mismatch on a",
        |e| matches!(e, Event::VersionMismatch { node, .. } if node == "b"),
    );
    // Updating: b counts as heard from.
    assert!(a.member("b").unwrap().alive);
    wait_event(&a, "b down once the updating time is over", |e| {
        *e == Event::Down { node: "b".into() }
    });
    // b still answers every heartbeat, on its version: a has heard from no member it can compare
    // records with for the down time.
    thread::sleep(HEARTBEAT * 3);
    assert!(
        !a.caught_up_for("b"),
        "answers on another version past the updating time kept a from being silent"
    );
    drop(b);
}

/// Two builds of one version that differ only in build metadata (`+...`) are the same version:
/// neither is newer, so neither would update to the other, and they must talk.
#[test]
fn builds_that_differ_only_in_build_metadata_talk() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_version("a", &tmp.path().join("a"), "0.1.0+build.1");
    let b = start_version("b", &tmp.path().join("b"), "0.1.0+build.2");
    pair(&a, &b);
    a.call("b", json!(0)).unwrap();
    b.call("a", json!(0)).unwrap();
    // A prerelease is another version.
    let c = start_version("c", &tmp.path().join("c"), "0.1.0-rc.1+build.1");
    let request = c.pair_request().unwrap();
    assert!(matches!(
        a.admit(&request),
        Err(MeshError::VersionMismatch { .. })
    ));
}

#[test]
fn the_first_subscriber_gets_the_events_from_before_it() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let dir = tmp.path().join("b");
    let mesh = Mesh::start(config("b", &dir, "0.1.0"), Arc::new(Echo)).unwrap();
    // Joining emits Joined on b before anyone subscribes.
    pair(&a, &mesh);
    let b = node(mesh, &dir);
    let early: Vec<Event> = b.events.try_iter().collect();
    assert!(
        early.contains(&Event::Joined {
            node: "a".into(),
            by: "a".into()
        }),
        "{early:?}"
    );
    // Later subscribers get only what follows; every subscriber gets that.
    let later = b.subscribe();
    assert!(later.try_recv().is_err());
    a.remove("b").unwrap();
    let removed = Event::SelfRemoved { by: "a".into() };
    wait_event(&b, "self removed on the first", |e| *e == removed);
    wait_on(&later, "self removed on the later", |e| *e == removed);
}

#[test]
fn heartbeats_tell_down_and_up() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    wait_event(&a, "b up", |e| *e == Event::Up { node: "b".into() });
    wait_until("a sees b alive", || a.member("b").is_some_and(|m| m.alive));

    // a hears from b after `t`, then b stops: b can't be down on a before `t` + the down time,
    // however late the heartbeats run.
    let t = Instant::now();
    let seen = a.member("b").unwrap().last_seen;
    wait_until("a hears from b again", || {
        a.member("b").unwrap().last_seen != seen
    });
    let dir = b.dir.clone();
    drop(b);
    wait_event(&a, "b down", |e| *e == Event::Down { node: "b".into() });
    assert!(t.elapsed() >= DOWN_AFTER, "{:?}", t.elapsed());
    wait_until("a sees b down", || !a.member("b").unwrap().alive);

    let b = start_version("b", &dir, "0.1.0");
    wait_event(&a, "b up again", |e| *e == Event::Up { node: "b".into() });
    wait_event(&b, "a up on b", |e| *e == Event::Up { node: "a".into() });
    wait_until("a sees b alive again", || a.member("b").unwrap().alive);
}

#[test]
fn a_stream_carries_megabytes_both_ways_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);

    let len = 6 * 1024 * 1024 + 12_345;
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let data: Vec<u8> = (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let data = Arc::new(data);

    let mut stream = a.open_stream("b", json!({ "kind": "echo" })).unwrap();
    assert_eq!(stream.peer().name, "b");
    let mut writer = stream.try_clone().unwrap();
    let sent = data.clone();
    let w = thread::spawn(move || {
        // Uneven chunks, written while the other thread reads the echo.
        let mut off = 0;
        let mut size = 1;
        while off < sent.len() {
            let end = (off + size).min(sent.len());
            writer.write_all(&sent[off..end]).unwrap();
            off = end;
            size = (size * 7 + 13) % 200_000 + 1;
        }
        writer.close_write().unwrap();
    });
    let mut got = Vec::with_capacity(len);
    stream.read_to_end(&mut got).unwrap();
    w.join().unwrap();
    assert_eq!(got.len(), data.len());
    assert!(got == *data, "stream corrupted");
}

#[test]
fn concurrent_calls_both_ways() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let mut threads = Vec::new();
    for i in 0..24 {
        let (from, to) = if i % 3 == 0 {
            (b.mesh.clone(), "a")
        } else {
            (a.mesh.clone(), "b")
        };
        threads.push(thread::spawn(move || {
            for j in 0..20 {
                let msg = json!({ "i": i, "j": j, "sleep_ms": (i * j) % 7 });
                let r = from.call(to, msg.clone()).unwrap();
                assert_eq!(r["echo"], msg);
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
}

/// A stopped mesh no longer listens, though the Mesh value lives on (in a daemon, until its last
/// user lets go): its port can be bound again at once. Its listener stays open only when it is
/// handed over ([`Mesh::hand_over`]).
#[test]
fn a_stopped_mesh_frees_its_port_at_once() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let addr = a.local_addr().unwrap();
    a.stop();
    std::net::TcpListener::bind(addr).expect("the port is free once stop returns");
}

#[test]
fn a_restarted_member_keeps_its_identity_and_membership() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let (key, cluster, old_addr) = (a.key(), a.cluster(), a.address());

    // Restarted, a listens on another port: b learns the new address from a's hello.
    let a = restart(a, "a", "0.1.0");
    assert_eq!(a.key(), key);
    assert_eq!(a.cluster(), cluster);
    assert_ne!(a.address(), old_addr);
    assert_eq!(names(&a), ["a", "b"]);
    assert_eq!(a.call("b", json!(0)).unwrap()["from"], "a");
    wait_until("b reaches the restarted a", || {
        b.call("a", json!(0)).is_ok()
    });
    assert_eq!(b.member("a").unwrap().address, a.address());
}

#[test]
fn a_dial_only_mesh_uses_the_daemons_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let mut cfg = config("a", &a.dir, "0.1.0");
    cfg.listen = a.local_addr().unwrap();
    let client = Mesh::connect_only(cfg).unwrap();
    assert_eq!(client.key(), a.key());
    assert_eq!(client.call("b", json!(5)).unwrap()["from"], "a");
    let mut s = client.open_stream("b", json!({ "kind": "echo" })).unwrap();
    s.write_all(b"ping").unwrap();
    s.close_write().unwrap();
    let mut back = String::new();
    s.read_to_string(&mut back).unwrap();
    assert_eq!(back, "ping");
    assert!(matches!(client.pair_request(), Err(MeshError::Invalid(_))));
    client.stop();
    assert!(matches!(
        client.call("b", json!(0)),
        Err(MeshError::Stopped)
    ));
}

#[test]
fn a_stream_pipes_a_process_both_ways() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let text: String = (0..40_000).map(|i| format!("line {i} of text\n")).collect();
    let stream = a.open_stream("b", json!({ "kind": "upper" })).unwrap();
    let mut out = Vec::new();
    let (sent, got) = stream.splice(text.as_bytes(), &mut out).unwrap();
    assert_eq!((sent, got), (text.len() as u64, text.len() as u64));
    assert!(String::from_utf8(out).unwrap() == text.to_uppercase());
}

/// A manual smoke test between two machines; does nothing unless SMOKE_STEP is set. Each run
/// starts a member from SMOKE_DIR listening on 127.0.0.1:SMOKE_PORT (reached from the other
/// machine through a forwarded port of the same number), performs SMOKE_STEP (`request`,
/// `admit` or `finish`, reading SMOKE_IN and writing SMOKE_OUT), then for SMOKE_SECS seconds
/// calls every other member once a second and sends each an 8 MiB echo stream once.
#[test]
#[ignore]
fn smoke_between_machines() {
    let Ok(step) = std::env::var("SMOKE_STEP") else {
        return;
    };
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
    let name = env("SMOKE_NAME");
    let port: u16 = env("SMOKE_PORT").parse().unwrap();
    let secs: u64 = std::env::var("SMOKE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut cfg = config(&name, Path::new(&env("SMOKE_DIR")), "0.1.0");
    cfg.listen = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    cfg.address = Some(format!("127.0.0.1:{port}"));
    cfg.heartbeat = Duration::from_secs(1);
    cfg.down_after = Duration::from_secs(5);
    let mesh = Mesh::start(cfg, Arc::new(Echo)).unwrap();
    let events = mesh.subscribe();
    match step.as_str() {
        "request" => std::fs::write(env("SMOKE_OUT"), mesh.pair_request().unwrap()).unwrap(),
        "admit" => {
            let request = std::fs::read_to_string(env("SMOKE_IN")).unwrap();
            std::fs::write(env("SMOKE_OUT"), mesh.admit(&request).unwrap()).unwrap();
        }
        "finish" => {
            let reply = std::fs::read_to_string(env("SMOKE_IN")).unwrap();
            mesh.pair_finish(&reply).unwrap();
        }
        _ => {}
    }
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut streamed = std::collections::HashSet::new();
    while Instant::now() < deadline {
        for e in events.try_iter() {
            println!("{name}: event {e:?}");
        }
        let peers: Vec<String> = mesh
            .members()
            .into_iter()
            .filter(|m| !m.this_machine)
            .map(|m| m.name)
            .collect();
        for p in peers {
            let t = Instant::now();
            match mesh.call(&p, json!({ "from_smoke": name })) {
                Ok(v) => println!("{name}: call {p} ok in {:?}: {v}", t.elapsed()),
                Err(e) => println!("{name}: call {p}: {e}"),
            }
            if streamed.contains(&p) {
                continue;
            }
            let Ok(stream) = mesh.open_stream(&p, json!({ "kind": "echo" })) else {
                continue;
            };
            let data: Vec<u8> = (0..8 << 20)
                .map(|i: u32| (i * 31 + i / 4099) as u8)
                .collect();
            let mut back = Vec::new();
            let t = Instant::now();
            let r = stream.splice(&data[..], &mut back);
            let ok = r.is_ok() && back == data;
            println!(
                "{name}: 8 MiB echo stream to {p}: {} in {:?}",
                if ok { "intact" } else { "BROKEN" },
                t.elapsed()
            );
            streamed.insert(p);
        }
        thread::sleep(Duration::from_secs(1));
    }
    for e in events.try_iter() {
        println!("{name}: event {e:?}");
    }
    for m in mesh.members() {
        println!(
            "{name}: member {} alive={} version={:?} address={}",
            m.name, m.alive, m.version, m.address
        );
    }
    mesh.stop();
}

#[test]
fn a_handshake_that_trickles_is_cut_off() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let limit = node::HANDSHAKE_TIMEOUT + Duration::from_secs(3);
    let mut s = std::net::TcpStream::connect(a.local_addr().unwrap()).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    // The start of a TLS record carrying a ClientHello, then a byte a second: every read on the
    // listening side returns well within its timeout, but the handshake never completes.
    s.write_all(&[0x16, 0x03, 0x01, 0x01, 0x00, 0x01, 0x00, 0x00, 0xfc])
        .unwrap();
    let started = Instant::now();
    loop {
        let mut b = [0u8; 64];
        match s.read(&mut b) {
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            // Closed, or an alert before closing.
            _ => break,
        }
        assert!(
            started.elapsed() < limit,
            "the listener still waits for the handshake after {:?}",
            started.elapsed()
        );
        if s.write_all(&[0x03]).is_err() {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// A hello is read a byte at a time (bytes after its line belong to a stream). A read zeroed a
/// 16 KiB buffer for the socket even when rustls had the byte already: 24 ms of a listener's time
/// for a 64 KiB hello from a key it knows, against 1 ms now (release build, M3 Max).
#[test]
fn a_hello_read_a_byte_at_a_time_zeroes_a_buffer_only_to_read_the_socket() {
    let tmp = tempfile::tempdir().unwrap();
    let (da, db) = (tmp.path().join("a"), tmp.path().join("b"));
    std::fs::create_dir_all(&da).unwrap();
    std::fs::create_dir_all(&db).unwrap();
    let a = identity::Identity::create(&da).unwrap();
    let b = identity::Identity::create(&db).unwrap();
    let a_key = a.public();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let dialer = thread::spawn(move || {
        let sock = std::net::TcpStream::connect(addr).unwrap();
        let d = tls::Duplex::connect(sock, &b, a_key).unwrap();
        let mut line = vec![b'x'; node::MAX_HELLO as usize];
        line.push(b'\n');
        let mut sent = 0;
        while sent < line.len() {
            sent += d.write(&line[sent..]).unwrap();
        }
        let mut done = [0u8; 1];
        let _ = d.read(&mut done);
    });
    let (sock, _) = listener.accept().unwrap();
    let cfg = tls::server_config(&a, Arc::new(|_: &PublicKey| true)).unwrap();
    let d = tls::Duplex::accept(sock, cfg).unwrap();
    let line = node::read_line_raw(&d, node::MAX_HELLO).unwrap().unwrap();
    assert_eq!(line.len() as u64, node::MAX_HELLO);
    let buffers = d.buffers();
    d.write(b"k").unwrap();
    dialer.join().unwrap();
    // One for each read of the socket (a few dozen), not one for each byte.
    assert!(
        buffers < 1_000,
        "{buffers} buffers zeroed for a {} byte line",
        line.len()
    );
}

/// Starts a member on a port to restart it on with [`again`], as a real member's port is fixed.
/// The port is one [`free_port`] hands out, claimed for this process: while its member is down,
/// it goes to no other test here or in another test process. (A port picked at random, as
/// before, went to another test process's `free_port` meanwhile.)
fn start_fixed(name: &str, root: &Path) -> Node {
    let dir = root.join(name);
    for _ in 0..100 {
        let mut cfg = config(name, &dir, "0.1.0");
        cfg.listen = std::net::SocketAddr::from(([127, 0, 0, 1], free_port()));
        match Mesh::start(cfg, Arc::new(Echo)) {
            Ok(mesh) => return node(mesh, &dir),
            // Taken between the claim and the bind by a process that doesn't claim ports.
            Err(MeshError::Invalid(m)) if m.starts_with("can't listen") => {}
            Err(e) => panic!("{e}"),
        }
    }
    panic!("no free port for {name}");
}

/// Restarts a stopped member from its directory, on the port it had.
fn again(name: &str, dir: &Path, addr: std::net::SocketAddr) -> Node {
    let mut cfg = config(name, dir, "0.1.0");
    cfg.listen = addr;
    node(Mesh::start(cfg, Arc::new(Echo)).unwrap(), dir)
}

/// A member's directory and listening address, to restart it with [`again`] (it must have been
/// started with [`start_fixed`]).
fn place(n: &Node) -> (PathBuf, std::net::SocketAddr) {
    let addr = n.local_addr().unwrap();
    assert!(
        addr.port() < 32_000,
        "{} was not started with start_fixed",
        n.name()
    );
    (n.dir.clone(), addr)
}

/// While a member started on a fixed port is down, the port stays claimed for this process (its
/// UDP twin is held), so another test process's `free_port` skips it.
#[test]
fn a_fixed_port_stays_claimed_while_its_member_is_down() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let (_, addr) = place(&a);
    drop(a);
    assert!(
        std::net::UdpSocket::bind(addr).is_err(),
        "{addr} is free for another process to take"
    );
}

/// A child process that another thread is starting holds a copy of every descriptor until it
/// execs: the listener of a member stopped meanwhile listened on in it, and the member could not
/// start again on its port ("Address already in use"). On msi, tests that restart a member in
/// their own process failed so while other tests started processes. Here the child holds its
/// copies for half a second; the port is free once the member has stopped all the same.
#[test]
fn a_stopped_members_port_is_free_while_a_child_being_started_holds_its_listener() {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let (dir, addr) = place(&a);
    let (forked, tell) = std::os::unix::net::UnixStream::pair().unwrap();
    let starting = thread::spawn(move || {
        let tell = tell.as_raw_fd();
        let mut child = std::process::Command::new("true");
        // SAFETY: only write and nanosleep between fork and exec.
        unsafe {
            child.pre_exec(move || {
                libc::write(tell, b"f".as_ptr().cast(), 1);
                let half = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 500_000_000,
                };
                libc::nanosleep(&half, std::ptr::null_mut());
                Ok(())
            });
        }
        child.status().unwrap()
    });
    // Forked: the child holds a's listener.
    (&forked).read_exact(&mut [0u8; 1]).unwrap();
    drop(a);
    let a = again("a", &dir, addr);
    assert_eq!(a.local_addr(), Some(addr));
    assert!(starting.join().unwrap().success());
}

#[test]
fn unauthenticated_connections_are_capped() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    // Calls at once leave b a few connections to a in its pool: b's call below takes one even
    // while a heartbeat ping holds another.
    let bm: &Mesh = &b;
    thread::scope(|s| {
        for _ in 0..3 {
            s.spawn(|| bm.call("a", json!({ "sleep_ms": 200 })).unwrap());
        }
    });
    let addr = a.local_addr().unwrap();
    // Twelve connections that never start their handshake: eight fill this address's slots and
    // the rest are closed at once. (b, on the same address, may hold a slot for a moment while it
    // dials a: then one more is closed.)
    let extra = 4;
    let conns: Vec<std::net::TcpStream> = (0..node::MAX_UNAUTHENTICATED_PER_IP + extra)
        .map(|_| {
            let s = std::net::TcpStream::connect(addr).unwrap();
            s.set_nonblocking(true).unwrap();
            s
        })
        .collect();
    let closed = |conns: &[std::net::TcpStream]| {
        conns
            .iter()
            .filter(|s| {
                let mut buf = [0u8; 1];
                match (&mut &**s).read(&mut buf) {
                    Ok(0) => true,
                    Err(e) => e.kind() != io::ErrorKind::WouldBlock,
                    Ok(_) => false,
                }
            })
            .count()
    };
    let started = Instant::now();
    while closed(&conns) < extra {
        assert!(
            started.elapsed() < WAIT,
            "only {} of the connections past the cap were closed",
            closed(&conns)
        );
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(300));
    let n = closed(&conns);
    assert!(n <= extra + 1, "{n} closed: the cap let fewer than 7 in");
    let e = wait_event(
        &a,
        "refused for the cap",
        |e| matches!(e, Event::Refused { reason, .. } if reason.contains("too many")),
    );
    let Event::Refused { address, .. } = e else {
        unreachable!()
    };
    assert!(address.starts_with("127.0.0.1:"), "{address}");
    // Pooled connections are already authenticated: b still reaches a.
    b.call("a", json!(1)).unwrap();
    // Once those connections go, new ones are let in again.
    drop(conns);
    wait_until("a new connection is let in", || {
        a.dial_raw("b", b.key(), &b.address()).is_ok()
            && b.dial_raw("a", a.key(), &a.address()).is_ok()
    });
}

#[test]
fn the_unauthenticated_cap_counts_per_address_and_in_all() {
    let mut p = node::PreAuth::default();
    let ip = |i: u8| std::net::IpAddr::from([10, 0, 0, i]);
    for _ in 0..3 {
        assert!(p.try_take(ip(1), 5, 3));
    }
    assert!(!p.try_take(ip(1), 5, 3));
    assert!(p.try_take(ip(2), 5, 3) && p.try_take(ip(2), 5, 3));
    // Five in all: a third address is refused too.
    assert!(!p.try_take(ip(3), 5, 3));
    p.release(ip(1));
    assert!(p.try_take(ip(3), 5, 3));
    assert!(!p.try_take(ip(1), 5, 3));
}

#[test]
fn a_removed_members_records_count_nowhere_even_through_an_offline_member() {
    // a removes b while b and d are offline. b, not knowing, admits x through d. When a is back,
    // everyone learns: b's admission of x is void, and x, dropped, is told.
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let d = start_fixed("d", tmp.path());
    pair(&a, &b);
    pair(&a, &d);
    wait_until("b knows d", || names(&b) == ["a", "b", "d"]);
    let (pa, pb, pd) = (place(&a), place(&b), place(&d));
    drop(b);
    drop(d);
    a.remove("b").unwrap();
    drop(a);

    let b = again("b", &pb.0, pb.1);
    let d = again("d", &pd.0, pd.1);
    let x = start("x", tmp.path());
    pair(&b, &x);
    wait_until("d accepts b's admission of x", || {
        names(&d) == ["a", "b", "d", "x"]
    });
    assert_eq!(x.call("d", json!(0)).unwrap()["from"], "x");

    let a = again("a", &pa.0, pa.1);
    wait_until("d learns b's removal", || names(&d) == ["a", "d"]);
    wait_until("a stays as it was", || names(&a) == ["a", "d"]);
    // x is told it is out, with why, and nobody talks to it.
    let e = wait_event(&x, "x dropped", |e| matches!(e, Event::Dropped { .. }));
    let Event::Dropped { reason } = e else {
        unreachable!()
    };
    assert!(reason.contains("b admitted it"), "{reason}");
    assert!(x.is_removed());
    assert!(matches!(
        x.call("d", json!(0)),
        Err(MeshError::Dropped { .. })
    ));
    wait_until("b learns it was removed", || b.is_removed());
    assert!(!a.member("x").is_some_and(|m| m.alive));
    // x can pair again, with the same key (it was never removed).
    let key = x.key();
    let reply = a.admit(&x.pair_request().unwrap()).unwrap();
    x.pair_finish(&reply).unwrap();
    assert_eq!(x.key(), key);
    assert!(!x.is_removed());
    wait_until("d knows x again", || names(&d) == ["a", "d", "x"]);
}

#[test]
fn removals_that_cross_go_to_the_member_admitted_first_on_every_member() {
    // b removes c while c is offline; c, not knowing, removes b and tells d (offline when b
    // removed c). Once everyone has both removals, b's counts (b was admitted before c) and
    // c's is void, on every member.
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    let d = start_fixed("d", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    pair(&a, &d);
    for m in [&b, &c, &d] {
        wait_until("all know each other", || names(m) == ["a", "b", "c", "d"]);
    }
    let places: Vec<(PathBuf, std::net::SocketAddr)> =
        [&a, &b, &c, &d].iter().map(|n| place(n)).collect();
    drop(c);
    drop(d);
    b.remove("c").unwrap();
    wait_until("a learns b removed c", || names(&a) == ["a", "b", "d"]);
    drop(a);
    drop(b);

    let c = again("c", &places[2].0, places[2].1);
    let d = again("d", &places[3].0, places[3].1);
    c.remove("b").unwrap();
    wait_until("d takes c's removal of b", || names(&d) == ["a", "c", "d"]);
    drop(c);

    let a = again("a", &places[0].0, places[0].1);
    let b = again("b", &places[1].0, places[1].1);
    for m in [&a, &b, &d] {
        wait_until("every member settles on b's removal", || {
            names(m) == ["a", "b", "d"]
        });
    }
    // c, back, learns it lost.
    let c = again("c", &places[2].0, places[2].1);
    wait_until("c learns it was removed", || c.is_removed());
    assert!(matches!(
        c.call("a", json!(0)),
        Err(MeshError::Removed { by }) if by == "b"
    ));
    assert_eq!(b.call("d", json!(0)).unwrap()["from"], "b");
}

// Audit repro (round 7 trust): s admits k (k ranks before q) while q removes s, not knowing of
// it, and k removes q, not knowing of q's removal: k's removal counts, and with it s's admission
// of k. t held s's admission of k when q's removal of s arrived, dropped it (void then) and
// remembered it as turned away; k's removal on its own waits for k's admission. Every member
// then sent t both together, and t took neither, for good: s stayed removed and q a member on t
// while every other member had it the other way round.
#[test]
fn a_record_turned_away_is_taken_with_the_record_that_makes_it_count() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let s = start_fixed("s", tmp.path());
    let t = start_fixed("t", tmp.path());
    pair(&a, &s);
    pair(&a, &t);
    wait_until("s knows t", || names(&s) == ["a", "s", "t"]);
    // s admits k, at the clock s has now; nobody hears of it yet.
    let k_dir = tmp.path().join("k");
    let k = identity::Identity::create(&k_dir.join("keys")).unwrap();
    let s_keys = s.dir.join("keys");
    let s_admits_k = {
        let id = identity::Identity::load(&s_keys).unwrap();
        let st = trust::Trust::load_read_only(&s_keys).unwrap();
        Record::Admission(Admission::sign(
            &id,
            "s",
            "k",
            k.public(),
            "127.0.0.1:1",
            "",
            st.next_clock(),
        ))
    };
    let v = start_fixed("v", tmp.path());
    let q = start_fixed("q", tmp.path());
    pair(&a, &v);
    pair(&a, &q);
    for m in [&s, &t, &v, &q] {
        wait_until("all know each other", || {
            names(m) == ["a", "q", "s", "t", "v"]
        });
    }
    let q_key = q.key();
    let (pt, pv) = (place(&t), place(&v));
    drop(s);
    drop(t);
    drop(v);
    // t holds s's admission of k; q, not knowing of it, removes s.
    hold(&pt.0, std::slice::from_ref(&s_admits_k));
    q.remove("s").unwrap();
    // k removes q, not knowing of q's removal of s; v holds both of the records q hasn't seen.
    let k_removes_q = Record::Removal(Removal::sign(&k, "k", "q", q_key, Vec::new(), 10));
    hold(&pv.0, &[s_admits_k.clone(), k_removes_q.clone()]);

    // t, back, hears of q's removal of s: s's admission of k is void there. A merge drops no
    // record held, so t keeps it until it starts again, and drops it then.
    let t = again("t", &pt.0, pt.1);
    wait_until("t learns q removed s", || names(&t) == ["a", "q", "t", "v"]);
    let held = |n: &Node, r: &Record| {
        trust::Trust::load_read_only(&n.dir.join("keys"))
            .unwrap()
            .records()
            .contains(r)
    };
    assert!(held(&t, &s_admits_k));
    drop(t);
    let t = again("t", &pt.0, pt.1);
    assert!(!held(&t, &s_admits_k));
    wait_until("t, started again, still has s removed", || {
        names(&t) == ["a", "q", "t", "v"]
    });
    // v, back, brings both records: k ranks before q, so k's removal of q counts everywhere.
    let v = again("v", &pv.0, pv.1);
    for m in [&a, &v] {
        wait_until("a and v settle on k's removal of q", || {
            names(m) == ["a", "k", "s", "t", "v"]
        });
    }
    wait_until("t settles on k's removal of q", || {
        names(&t) == ["a", "k", "s", "t", "v"]
    });
    assert!(held(&t, &s_admits_k) && held(&t, &k_removes_q));
    drop(q);
}

/// Makes `dir` read-only (true) or writable again (false) for its owner.
fn read_only(dir: &Path, yes: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mode = if yes { 0o500 } else { 0o700 };
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// A membership change whose records this machine can't save is undone, so the error the user
/// sees and what the cluster is told agree.
#[test]
fn a_membership_change_that_cant_be_saved_is_undone() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    let c = start("c", tmp.path());
    pair(&a, &b);
    let keys = a.dir.join("keys");
    let saved = std::fs::read(keys.join("members.json")).unwrap();
    let request = c.pair_request().unwrap();
    read_only(&keys, true);
    let admitted = a.admit(&request);
    let removed = a.remove("b");
    read_only(&keys, false);
    assert!(
        admitted.is_err() && removed.is_err(),
        "{admitted:?} {removed:?}"
    );
    assert_eq!(names(&a), ["a", "b"]);
    assert_eq!(std::fs::read(keys.join("members.json")).unwrap(), saved);
    // Nothing was announced: b never hears of c, nor of its own removal.
    std::thread::sleep(HEARTBEAT * 3);
    assert_eq!(names(&b), ["a", "b"]);
    // Once it can save, the same request admits c.
    let reply = a.admit(&request).unwrap();
    c.pair_finish(&reply).unwrap();
    wait_until("b hears of c", || names(&b) == ["a", "b", "c"]);
}

/// Records a member sends that this machine couldn't save (a full or read-only disk) are saved
/// once it can, not only at the next change: a restart before that would forget them.
#[test]
fn records_that_couldnt_be_saved_are_saved_later() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    let c = start("c", tmp.path());
    pair(&a, &b);
    let keys = b.dir.join("keys");
    read_only(&keys, true);
    pair(&a, &c);
    wait_until("b hears of c", || names(&b) == ["a", "b", "c"]);
    let file = keys.join("members.json");
    let has_c = || {
        std::fs::read_to_string(&file)
            .unwrap()
            .contains("\"node\": \"c\"")
    };
    assert!(!has_c());
    read_only(&keys, false);
    wait_until("b saves c's admission", has_c);
}

/// A removed machine's `pair` makes a new key and starts over as a cluster of one. When the new
/// key is written but the records can't be (a disk with room for the small key file only; here
/// a directory in the records' place), it stays the removed machine, so the next `pair` makes a
/// request a member can admit.
#[test]
fn a_new_key_whose_records_cant_be_saved_leaves_the_removed_machine_as_it_was() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let old = b.key();
    a.remove("b").unwrap();
    wait_until("b learns it was removed", || b.is_removed());
    let file = b.dir.join("keys").join("members.json");
    let aside = b.dir.join("members.json.aside");
    std::fs::rename(&file, &aside).unwrap();
    std::fs::create_dir(&file).unwrap();
    std::fs::write(file.join("x"), "").unwrap();
    let failed = b.pair_request();
    std::fs::remove_file(file.join("x")).unwrap();
    std::fs::remove_dir(&file).unwrap();
    std::fs::rename(&aside, &file).unwrap();
    assert!(failed.is_err(), "{failed:?}");
    assert!(b.is_removed());
    let request = b.pair_request().unwrap();
    assert_ne!(b.key(), old);
    let reply = a.admit(&request).unwrap();
    b.pair_finish(&reply).unwrap();
    assert_eq!(names(&b), ["a", "b"]);
}

/// A port on 127.0.0.1, free when asked; the caller binds it later. It is below the ranges the
/// system hands out to listeners on port 0 and to outgoing connections (32768-60999 on Linux,
/// 49152-65535 on macOS), where it could meanwhile go to another test's connection. Each is handed
/// out once in this process.
pub(crate) fn free_port() -> u16 {
    use std::hash::{BuildHasher, Hasher};
    static NEXT: std::sync::Mutex<u16> = std::sync::Mutex::new(0);
    // Claimed for this process as tests/common's free_port does (the UDP twin bound), so other
    // test processes using either skip it.
    static CLAIMS: std::sync::Mutex<Vec<std::net::UdpSocket>> = std::sync::Mutex::new(Vec::new());
    let mut next = NEXT.lock().unwrap();
    if *next == 0 {
        // Each RandomState has keys of its own: a random start in 20000..30000.
        let r = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        *next = 20_000 + (r % 10_000) as u16;
    }
    for _ in 0..2_000 {
        let port = *next;
        *next = if port >= 31_999 { 20_000 } else { port + 1 };
        let Ok(claim) = std::net::UdpSocket::bind(("127.0.0.1", port)) else {
            continue;
        };
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            CLAIMS.lock().unwrap().push(claim);
            return port;
        }
    }
    panic!("no free port on 127.0.0.1 in 20000..32000");
}

/// Machines in no cluster need no listener: an unlistening mesh pairs on both sides, then each
/// starts listening on its configured port and they meet.
#[test]
fn pairing_without_listening_first() {
    // Each port is free when it is picked and bound only after the pairing: another test process
    // can take it meanwhile (they pick from the same range). Then it all starts again with others.
    for _ in 0..5 {
        if pairing_without_listening_first_on_free_ports() {
            return;
        }
    }
    panic!("the ports picked were taken before they were bound, five times");
}

/// [`pairing_without_listening_first`]; false when a port picked was taken by another process.
fn pairing_without_listening_first_on_free_ports() -> bool {
    let root = tempfile::tempdir().unwrap();
    let cfg = |name: &str| {
        let mut c = config(name, &root.path().join(name), "0.1.0");
        c.listen = SocketAddr::new(c.listen.ip(), free_port());
        c
    };
    let (ca, cb) = (cfg("a"), cfg("b"));
    let a = Mesh::start_unlistening(ca.clone(), Arc::new(Echo)).unwrap();
    let b = Mesh::start_unlistening(cb.clone(), Arc::new(Echo)).unwrap();
    assert_eq!((a.local_addr(), b.local_addr()), (None, None));
    assert_eq!(b.address(), format!("127.0.0.1:{}", cb.listen.port()));
    pair(&a, &b);
    assert_eq!(names(&a), ["a", "b"]);
    assert_eq!(names(&b), ["a", "b"]);
    a.stop();
    b.stop();
    let taken = |e: &MeshError| e.to_string().contains("already in use");
    let a = match Mesh::start(ca.clone(), Arc::new(Echo)) {
        Ok(a) => a,
        Err(e) if taken(&e) => return false,
        Err(e) => panic!("{e}"),
    };
    let b = match Mesh::start(cb.clone(), Arc::new(Echo)) {
        Ok(b) => b,
        Err(e) if taken(&e) => {
            a.stop();
            return false;
        }
        Err(e) => panic!("{e}"),
    };
    assert_eq!(a.local_addr(), Some(ca.listen));
    wait_until("a and b are up", || {
        a.member("b").is_some_and(|m| m.alive) && b.member("a").is_some_and(|m| m.alive)
    });
    assert_eq!(a.call("b", json!({"x": 1})).unwrap()["from"], "a");
    // A dial-only mesh of b (as `potluck _tunnel` runs) leaves b's address alone, whatever its
    // config says, and can't pair.
    let b_at = format!("127.0.0.1:{}", cb.listen.port());
    let mut cd = cb.clone();
    cd.address = Some("127.0.0.1:1".into());
    let d = Mesh::connect_only(cd).unwrap();
    assert_eq!(d.call("a", json!({"x": 2})).unwrap()["from"], "b");
    assert_eq!(a.member("b").unwrap().address, b_at);
    assert!(d.pair_request().is_err());
    a.stop();
    b.stop();
    true
}

// Audit repro (round 7 trust): a pairing reply carried every record that counts, and removed
// machines' records count for good: each machine admitted and removed in the cluster's life
// added about 950 bytes to the reply. Past about 140 of them the reply is over 128 KiB, the most
// one argument may be on Linux, so `potluck pair --finish REPLY` there failed to start
// ("Argument list too long"). The reply now carries what makes the members members.
#[test]
fn a_pairing_reply_stays_small_however_many_machines_were_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    pair(&a, &b);
    wait_until("b knows a", || names(&b) == ["a", "b"]);
    let (pa, pb) = (place(&a), place(&b));
    drop(a);
    drop(b);
    // a admitted and removed 300 machines in its time.
    let keys = pa.0.join("keys");
    let id = identity::Identity::load(&keys).unwrap();
    hold(&pa.0, &junk(&id, "a", 600, 7));
    let a = again("a", &pa.0, pa.1);
    let b = again("b", &pb.0, pb.1);
    assert_eq!(
        trust::Trust::load_read_only(&keys)
            .unwrap()
            .view
            .removed
            .len(),
        300
    );
    let c = start("c", tmp.path());
    let reply = a.admit(&c.pair_request().unwrap()).unwrap();
    assert!(reply.len() < 16 * 1024, "a reply of {} bytes", reply.len());
    c.pair_finish(&reply).unwrap();
    assert_eq!(names(&c), ["a", "b", "c"]);
    // c takes the rest from the members.
    let digest = |n: &Node| {
        trust::Trust::load_read_only(&n.dir.join("keys"))
            .unwrap()
            .digest()
    };
    wait_until("c holds a's records", || digest(&c) == digest(&a));
    wait_until("b knows c", || names(&b) == ["a", "b", "c"]);
}

// Owner's design (round 5): room for MAX_MEMBERS members. Admitting one more is refused, naming
// the limit, and nothing is signed; once a member is removed, the admission goes through.
#[test]
fn an_admission_past_the_limit_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let dir = a.dir.clone();
    drop(a);
    {
        let keys = dir.join("keys");
        let id = identity::Identity::load(&keys).unwrap();
        let mut t = trust::Trust::load_or_found(&keys, &id, "a", "127.0.0.1:1").unwrap();
        let first = t.next_clock();
        let fill: Vec<Record> = (0..trust::MAX_MEMBERS - 1)
            .map(|i| {
                let mut k = [7u8; 32];
                k[..8].copy_from_slice(&(i as u64).to_le_bytes());
                Record::Admission(Admission::sign(
                    &id,
                    "a",
                    &format!("x{i}"),
                    PublicKey(k),
                    "127.0.0.1:1",
                    "",
                    first + i as u64,
                ))
            })
            .collect();
        t.merge(fill);
        assert_eq!(t.view.members.len(), trust::MAX_MEMBERS);
        t.save().unwrap();
    }
    // No heartbeat while it runs: it would ping the 2,047 machines that don't exist.
    let mut cfg = config("a", &dir, "0.1.0");
    cfg.heartbeat = Duration::from_secs(600);
    let a = node(Mesh::start(cfg, Arc::new(Echo)).unwrap(), &dir);
    let c = start("c", tmp.path());
    let records = |n: &Node| trust::Trust::load_read_only(&n.dir.join("keys")).unwrap();
    let held = records(&a).records().len();
    let request = c.pair_request().unwrap();
    let err = a.admit(&request).unwrap_err().to_string();
    assert!(err.contains("2048 members, the most it can have"), "{err}");
    assert_eq!(records(&a).records().len(), held, "nothing is signed");
    assert_eq!(a.members().len(), trust::MAX_MEMBERS);
    a.remove("x5").unwrap();
    c.pair_finish(&a.admit(&request).unwrap()).unwrap();
    assert_eq!(a.members().len(), trust::MAX_MEMBERS);
    assert!(a.member("c").is_some() && a.member("x5").is_none());
}

/// Records of `by`'s, as many as `n`: admissions of keys that don't exist, each removed after.
fn junk(by: &identity::Identity, by_name: &str, n: usize, tag: u8) -> Vec<Record> {
    let mut out = Vec::new();
    let mut i = 0u32;
    while out.len() < n {
        let mut k = [0u8; 32];
        k[0] = tag;
        k[1..5].copy_from_slice(&i.to_le_bytes());
        let name = format!("x{tag}-{i}");
        let clock = 50 + 2 * u64::from(i);
        out.push(Record::Admission(Admission::sign(
            by,
            by_name,
            &name,
            PublicKey(k),
            "127.0.0.1:1",
            "",
            clock,
        )));
        out.push(Record::Removal(Removal::sign(
            by,
            by_name,
            &name,
            PublicKey(k),
            Vec::new(),
            clock + 1,
        )));
        i += 1;
    }
    out
}

/// Adds `records` to the stopped member's store in `dir` as they are, as if it had taken them.
fn hold(dir: &Path, records: &[Record]) {
    let file = dir.join("keys").join(trust::STATE_FILE);
    let mut state: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    let held = state["records"].as_array_mut().unwrap();
    held.extend(records.iter().map(|r| serde_json::to_value(r).unwrap()));
    std::fs::write(&file, serde_json::to_vec(&state).unwrap()).unwrap();
}

#[test]
fn a_removed_members_records_after_its_removal_are_not_kept() {
    // b, removed, signs records after its removal (keys it admits and removes): the store turns
    // them away, and a store holding them drops them when loaded: one an older potluck wrote
    // (it kept up to 4,096 records, those that count nowhere included), read by the new one
    // after an update. a still admits c with a small reply, and keeps the admission across a
    // restart.
    let tmp = tempfile::tempdir().unwrap();
    let a = start("a", tmp.path());
    let b = start("b", tmp.path());
    pair(&a, &b);
    let b_keys = b.dir.join("keys");
    drop(b);
    a.remove("b").unwrap();
    let dir = a.dir.clone();
    drop(a);
    let b_id = identity::Identity::load(&b_keys).unwrap();
    let flood = junk(&b_id, "b", 4000, 1);
    let keys = dir.join("keys");
    let held = {
        let id = identity::Identity::load(&keys).unwrap();
        let mut t = trust::Trust::load_or_found(&keys, &id, "a", "127.0.0.1:1").unwrap();
        let held = t.records().len();
        assert_eq!(t.merge(flood.clone()), trust::Changes::default());
        assert_eq!(t.records().len(), held);
        held
    };
    hold(&dir, &flood);
    assert_eq!(
        trust::Trust::load_read_only(&keys).unwrap().records().len(),
        held + flood.len(),
        "held on disk"
    );
    let a = start("a", tmp.path());
    let c = start("c", tmp.path());
    let reply = a.admit(&c.pair_request().unwrap()).unwrap();
    assert!(reply.len() < 16 * 1024, "a reply of {} bytes", reply.len());
    c.pair_finish(&reply).unwrap();
    assert_eq!(names(&a), ["a", "c"]);
    assert_eq!(names(&c), ["a", "c"]);
    let records = |n: &Node| trust::Trust::load_read_only(&n.dir.join("keys")).unwrap();
    assert_eq!(
        records(&a).records().len(),
        held + 1,
        "the flood is dropped"
    );
    wait_until("c holds a's records", || {
        records(&c).digest() == records(&a).digest()
    });
    drop(c);
    let a = restart(a, "a", "0.1.0");
    assert_eq!(names(&a), ["a", "c"]);
}

/// [`again`] with another down time.
fn again_after(name: &str, dir: &Path, addr: std::net::SocketAddr, down_after: Duration) -> Node {
    let mut cfg = config(name, dir, "0.1.0");
    cfg.listen = addr;
    cfg.down_after = down_after;
    node(Mesh::start(cfg, Arc::new(Echo)).unwrap(), dir)
}

#[test]
fn a_member_back_compares_records_with_the_others_before_it_deals_with_one() {
    // a removes b while b and c are away. b and c come back first, b with the records it had
    // (as a removed machine may keep them): c must not deal with b before it has compared
    // records with a, which knows of the removal.
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    for m in [&b, &c] {
        wait_until("all know each other", || names(m) == ["a", "b", "c"]);
    }
    let (pa, pb, pc) = (place(&a), place(&b), place(&c));
    drop(b);
    drop(c);
    a.remove("b").unwrap();
    drop(a);

    // A down time long enough that c doesn't give up on a before a is back.
    let down = Duration::from_secs(10);
    let b = again_after("b", &pb.0, pb.1, down);
    let c = again_after("c", &pc.0, pc.1, down);
    wait_until("c hears from b", || c.member("b").is_some_and(|m| m.alive));
    // Dealing with a (down) needs only b's records, which c compared with its own.
    wait_until("c compares records with b", || c.caught_up_for("a"));
    assert!(!c.caught_up_for("b"));
    assert!(!b.caught_up_for("c"));
    let a = again_after("a", &pa.0, pa.1, down);
    let start = Instant::now();
    while names(&c).iter().any(|n| n == "b") {
        assert!(
            !c.caught_up_for("b"),
            "c may deal with b before it heard of b's removal"
        );
        assert!(start.elapsed() < WAIT, "c never learns of b's removal");
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!c.caught_up_for("b"));
    wait_until("c deals with a", || c.caught_up_for("a"));
    drop(a);
}

#[test]
fn a_member_back_from_silence_waits_at_most_the_down_time_for_the_others() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    let pa = place(&a);
    let pb = place(&b);
    drop(a);
    let down = Duration::from_secs(3);
    let a = again_after("a", &pa.0, pa.1, down);
    wait_until("a compares records with b and c", || {
        a.caught_up_for("b") && a.caught_up_for("c")
    });
    // c leaves for good; b goes away for longer than the down time: a hears from nobody.
    drop(c);
    drop(b);
    thread::sleep(down + Duration::from_millis(500));
    assert!(!a.caught_up_for("b"));
    // b is back: a is back too, and waits for c (silent) for up to the down time.
    let b = again("b", &pb.0, pb.1);
    wait_until("a hears from b", || a.member("b").is_some_and(|m| m.alive));
    let back = Instant::now();
    assert!(!a.caught_up_for("b"));
    wait_until("a deals with b without c", || a.caught_up_for("b"));
    assert!(back.elapsed() + Duration::from_millis(500) >= down);
    drop(b);
}

#[test]
fn a_member_that_starts_alone_compares_records_once_it_hears_from_one() {
    // a removes b while b and c are away. c starts again while it can reach nobody, for longer
    // than the down time; then b (removed, with the records it had) is the first it hears from.
    // c heard from no member for the down time: it is back once it hears from b, and must not
    // deal with b before it has compared records with a or waited the down time for it.
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    for m in [&b, &c] {
        wait_until("all know each other", || names(m) == ["a", "b", "c"]);
    }
    let (pb, pc) = (place(&b), place(&c));
    drop(b);
    drop(c);
    a.remove("b").unwrap();
    drop(a);

    let down = Duration::from_secs(2);
    let c = again_after("c", &pc.0, pc.1, down);
    thread::sleep(down + Duration::from_millis(500));
    assert!(
        !c.caught_up_for("b"),
        "c has heard from no member since it started"
    );
    let b = again_after("b", &pb.0, pb.1, down);
    wait_until("c hears from b", || c.member("b").is_some_and(|m| m.alive));
    let heard = Instant::now();
    while heard.elapsed() < down / 2 {
        assert!(
            !c.caught_up_for("b"),
            "c deals with b {:?} after it first heard from anyone, without comparing records with a",
            heard.elapsed()
        );
        thread::sleep(Duration::from_millis(20));
    }
    drop(b);
}

// Audit repro (round 4 security): a removed machine that reached a member away at its removal
// before any member that knew of it could fill that member's store with records that count there
// (keys it admits and removes). The removal then had no record counting nowhere to take the place
// of: the member turned it away for good and kept dealing with the removed one. Now there is no
// cap, and the removal voids them: the member keeps them until it next starts, and drops them
// then.
#[test]
fn a_removed_member_that_fills_an_away_members_store_is_still_removed_there() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    for m in [&b, &c] {
        wait_until("all know each other", || names(m) == ["a", "b", "c"]);
    }
    let (pa, pb, pc) = (place(&a), place(&b), place(&c));
    drop(b);
    drop(c);
    a.remove("b").unwrap();
    drop(a);

    // b, removed, signs records in bulk: in its own view (and c's), every one counts.
    let b_id = identity::Identity::load(&pb.0.join("keys")).unwrap();
    let flood = junk(&b_id, "b", 4000, 1);
    {
        let keys = pb.0.join("keys");
        let mut t = trust::Trust::load_or_found(&keys, &b_id, "b", "127.0.0.1:1").unwrap();
        t.merge(flood);
        t.save().unwrap();
    }
    let records = |dir: &Path| {
        trust::Trust::load_read_only(&dir.join("keys"))
            .unwrap()
            .records()
            .len()
    };
    let before = records(&pc.0);
    let down = Duration::from_secs(10);
    let b = again_after("b", &pb.0, pb.1, down);
    let c = again_after("c", &pc.0, pc.1, down);
    wait_until("c takes b's records", || records(&pc.0) > before + 3000);
    let a = again_after("a", &pa.0, pa.1, down);
    wait_until("c learns b's removal", || names(&c) == ["a", "c"]);
    assert!(!c.caught_up_for("b"));
    drop(c);
    let c = again_after("c", &pc.0, pc.1, down);
    assert_eq!(
        records(&pc.0),
        before + 1,
        "c drops b's records as it starts"
    );
    wait_until("c, started again, still has b removed", || {
        names(&c) == ["a", "c"]
    });
    assert!(!c.caught_up_for("b"));
    assert_eq!(
        records(&pc.0),
        before + 1,
        "c takes none of b's records back"
    );
    drop(b);
    drop(a);
}

/// Signs, on the stopped member in `dir` (it is `name`), a removal of member `node` keeping
/// every record of `node`'s it holds, as `potluck remove` there would.
fn remove_offline(dir: &Path, name: &str, node: &str) {
    let keys = dir.join("keys");
    let id = identity::Identity::load(&keys).unwrap();
    let mut t = trust::Trust::load_or_found(&keys, &id, name, "127.0.0.1:1").unwrap();
    let key = t.view.by_name(node).unwrap().key;
    let rec = Removal::sign(&id, name, node, key, t.signed_by(&key), t.next_clock());
    t.merge(vec![Record::Removal(rec)]);
    assert!(t.view.removed.contains_key(&key));
    t.save().unwrap();
}

// Audit repro (round 5 trust): b removes c while a, away, removes b, not knowing of b's removal
// of c. a's removal voids b's (b's records a hadn't seen are void), so c is a member everywhere
// once the records go round; but c heard of b's removal first. A machine that sees itself out
// refused every member and dialed none, so it never heard of a's removal and stayed out for
// good, while the members saw it as a member they couldn't reach. (A removed b reaching a member
// away at its removal first could shut it out so.)
#[test]
fn a_machine_whose_removal_turns_out_void_is_a_member_again() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    for m in [&b, &c] {
        wait_until("all know each other", || names(m) == ["a", "b", "c"]);
    }
    let pa = place(&a);
    drop(a);
    b.remove("c").unwrap();
    wait_until("c learns b removed it", || c.is_removed());
    remove_offline(&pa.0, "a", "b");
    let a = again("a", &pa.0, pa.1);
    wait_until("c learns its removal is void", || !c.is_removed());
    assert_eq!(names(&c), ["a", "c"]);
    assert_eq!(c.call("a", json!(0)).unwrap()["from"], "c");
    assert_eq!(a.call("c", json!(0)).unwrap()["from"], "a");
    drop(b);
}

// Audit repro (round 9 trust): b removes the founder a while a, away, removes b, not knowing of
// b's removal. a was admitted first, so its removal counts and b's is void (README: "the
// founder's removal wins any such race"). But c and d heard of b's removal first: they refused
// a as removed before any record exchange, dialed it no more, and a and b, each removed for the
// other, never dialed each other. So a's removal reached nobody: c and d had the founder out and
// b in, for good, while a had b out. Now a member about to refuse a key as removed takes the
// records that count for it first.
#[test]
fn a_crossing_removal_that_counts_reaches_members_that_heard_the_other_first() {
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let b = start_fixed("b", tmp.path());
    let c = start_fixed("c", tmp.path());
    let d = start_fixed("d", tmp.path());
    pair(&a, &b);
    pair(&a, &c);
    pair(&a, &d);
    for m in [&b, &c, &d] {
        wait_until("all know each other", || names(m) == ["a", "b", "c", "d"]);
    }
    let pa = place(&a);
    drop(a);
    b.remove("a").unwrap();
    for m in [&c, &d] {
        wait_until("c and d take b's removal of a", || {
            names(m) == ["b", "c", "d"]
        });
    }
    remove_offline(&pa.0, "a", "b");
    let a = again("a", &pa.0, pa.1);
    for m in [&a, &c, &d] {
        wait_until("every member settles on a's removal of b", || {
            names(m) == ["a", "c", "d"]
        });
    }
    wait_until("b learns it was removed", || b.is_removed());
    assert!(matches!(
        b.call("c", json!(0)),
        Err(MeshError::Removed { by }) if by == "a"
    ));
    assert_eq!(c.call("a", json!(0)).unwrap()["from"], "c");
    assert_eq!(a.call("d", json!(0)).unwrap()["from"], "a");
}

#[test]
fn members_whose_records_differ_for_good_dont_exchange_them_at_every_heartbeat() {
    // A member keeps the records it signed and those admitting it even when they are void
    // (the removal that voids them may turn out void itself); the others turn them away. So a
    // and c hold records that differ for good. They exchanged all of them at every heartbeat.
    let tmp = tempfile::tempdir().unwrap();
    let a = start_fixed("a", tmp.path());
    let c = start_fixed("c", tmp.path());
    pair(&a, &c);
    wait_until("c knows a", || names(&c) == ["a", "c"]);
    let (pa, pc) = (place(&a), place(&c));
    drop(a);
    drop(c);
    {
        // a admits r and removes it, keeping nothing; r, removed, admits a 1000 times: void
        // records, which a keeps as they admit it.
        let keys = pa.0.join("keys");
        let id = identity::Identity::load(&keys).unwrap();
        let mut t = trust::Trust::load_or_found(&keys, &id, "a", "127.0.0.1:1").unwrap();
        let r_dir = tmp.path().join("r-keys");
        let r = identity::Identity::load_or_create(&r_dir).unwrap();
        let clock = t.next_clock();
        let admitted = Admission::sign(&id, "a", "r", r.public(), "127.0.0.1:1", "", clock);
        let removed = Removal::sign(&id, "a", "r", r.public(), Vec::new(), clock + 1);
        t.merge(vec![Record::Admission(admitted), Record::Removal(removed)]);
        assert_eq!(t.view.members.len(), 2);
        let held = t.records().len();
        let void: Vec<Record> = (0..1000u64)
            .map(|i| {
                Record::Admission(Admission::sign(
                    &r,
                    "r",
                    "a",
                    id.public(),
                    "127.0.0.1:1",
                    "",
                    clock + 2 + i,
                ))
            })
            .collect();
        t.merge(void);
        assert_eq!(t.records().len(), held + 1000);
        assert_eq!(t.view.members.len(), 2);
        t.save().unwrap();
    }
    let a = again("a", &pa.0, pa.1);
    let c = again("c", &pc.0, pc.1);
    wait_until("a and c deal with each other", || {
        a.caught_up_for("c") && c.caught_up_for("a")
    });
    wait_until("a and c exchanged records", || {
        a.exchanges() >= 1 && c.exchanges() >= 1
    });
    let digest = |n: &Node| {
        trust::Trust::load_read_only(&n.dir.join("keys"))
            .unwrap()
            .digest()
    };
    assert_ne!(digest(&a), digest(&c));
    thread::sleep(HEARTBEAT * 10);
    let before = a.exchanges() + c.exchanges();
    thread::sleep(HEARTBEAT * 30);
    let n = a.exchanges() + c.exchanges() - before;
    assert!(n <= 2, "{n} record exchanges in 30 heartbeats");
    assert!(a.caught_up_for("c") && c.caught_up_for("a"));
    // A record that counts still goes round.
    let d = start("d", tmp.path());
    pair(&a, &d);
    wait_until("c knows d", || names(&c) == ["a", "c", "d"]);
}

#[test]
fn stop_returns_when_its_wake_up_connection_lands_elsewhere() {
    // `stop` wakes the accept loop by connecting to its own port. On every interface, that
    // connection goes to 127.0.0.1; another process listening on 127.0.0.1 and the same port
    // (macOS lets it, beside 0.0.0.0: an ssh forward, say) takes it instead. The same when the
    // address listened on is gone (a VPN's, after it went down). Stop must return anyway.
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config("a", &tmp.path().join("a"), "0.1.0");
    cfg.listen = "0.0.0.0:0".parse().unwrap();
    let a = Mesh::start(cfg, Arc::new(Echo)).unwrap();
    let port = a.local_addr().unwrap().port();
    // Linux doesn't let it: the wake-up can't land elsewhere there unless the address goes.
    let squatter = std::net::TcpListener::bind(("127.0.0.1", port)).ok();
    let (tx, rx) = std::sync::mpsc::channel();
    let a2 = a.clone();
    thread::spawn(move || {
        a2.stop();
        let _ = tx.send(());
    });
    let stopped = rx.recv_timeout(Duration::from_secs(10)).is_ok();
    drop(squatter);
    assert!(stopped, "stop didn't return in 10 s");
}
