//! The check of files git calls modified only through its own filters reads a file and its blob
//! a piece at a time: a big file costs no more memory than a small one. A test binary of its own,
//! for its allocator, which counts what the process holds at most.

use potluck::gitinfo;
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The system allocator, counting the bytes held now and the most held since [`Counting::reset`].
struct Counting;

static HELD: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as the caller's.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = HELD.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as the caller's.
        unsafe { System.dealloc(ptr, layout) };
        HELD.fetch_sub(layout.size(), Ordering::SeqCst);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: as the caller's.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            // The old block and the new one may both be held while it moves.
            let now = HELD.fetch_add(new_size, Ordering::SeqCst) + new_size;
            PEAK.fetch_max(now, Ordering::SeqCst);
            HELD.fetch_sub(layout.size(), Ordering::SeqCst);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

impl Counting {
    /// Starts counting the most held from what is held now.
    fn reset() -> usize {
        let now = HELD.load(Ordering::SeqCst);
        PEAK.store(now, Ordering::SeqCst);
        now
    }

    /// The most held since `reset`, above what was held then.
    fn peak_above(base: usize) -> usize {
        PEAK.load(Ordering::SeqCst).saturating_sub(base)
    }
}

fn tmp() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("potluck-test-")
        .tempdir_in(root)
        .unwrap()
}

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "init.defaultBranch=main", "-c", "user.name=t"])
        .args([
            "-c",
            "user.email=t@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(["-c", "core.compression=0", "-c", "gc.auto=0"])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `len` bytes of no particular pattern, with a line break now and then (never "\r").
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|i| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if i % 997 == 996 {
                b'\n'
            } else {
                b'a' + (x % 26) as u8
            }
        })
        .collect()
}

/// Moves the file's mtime back a minute, so git reads it again.
fn reread(path: &Path) {
    let then = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(then)
        .unwrap();
}

const BIG: usize = 24 << 20;
/// What the check may hold at most, whatever the files' size.
const BOUND: usize = 4 << 20;

#[test]
fn big_files_git_calls_modified_are_compared_a_piece_at_a_time() {
    let t = tmp();
    let src = t.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    git(&src, &["init", "-q"]);
    // Mixed line endings, committed before `eol=crlf`: a checkout writes every line with CRLF and
    // git calls the file modified, but it holds what the commit's checkout writes.
    let mut mixed = noise(BIG, 1);
    for (i, b) in mixed.iter_mut().enumerate() {
        if *b == b'\n' && i % 2 == 0 {
            *b = b'\r';
        }
    }
    let mixed: Vec<u8> = mixed
        .split(|b| *b == b'\r')
        .collect::<Vec<_>>()
        .join(&b"\r\n"[..]);
    std::fs::write(src.join("mixed.bat"), &mixed).unwrap();
    // A big file under `* text=auto`, committed, then edited in place (the same size).
    let data = noise(BIG, 2);
    std::fs::write(src.join("data.txt"), &data).unwrap();
    git(&src, &["add", "-A"]);
    git(&src, &["commit", "-q", "-m", "one"]);
    std::fs::write(src.join(".gitattributes"), "* text=auto\n*.bat eol=crlf\n").unwrap();
    git(&src, &["add", ".gitattributes"]);
    git(&src, &["commit", "-q", "-m", "attributes"]);
    let co = t.path().join("co");
    git(
        t.path(),
        &["clone", "-q", src.to_str().unwrap(), co.to_str().unwrap()],
    );
    reread(&co.join("mixed.bat"));

    // Explained: the file holds what a checkout writes.
    let base = Counting::reset();
    let repo = gitinfo::inspect(&co);
    let peak = Counting::peak_above(base);
    assert!(repo.is_ok(), "{repo:?}");
    assert!(
        peak < BOUND,
        "held {peak} bytes at most for {BIG}-byte files"
    );

    // Not explained: an edit of the same size.
    let mut edited = data.clone();
    edited[BIG / 2] = b'!';
    std::fs::write(co.join("data.txt"), &edited).unwrap();
    reread(&co.join("data.txt"));
    let base = Counting::reset();
    let refused = gitinfo::inspect(&co);
    let peak = Counting::peak_above(base);
    let e = format!("{:#}", refused.unwrap_err());
    assert!(e.contains(" M data.txt") && !e.contains("mixed.bat"), "{e}");
    assert!(
        peak < BOUND,
        "held {peak} bytes at most for {BIG}-byte files"
    );
}
