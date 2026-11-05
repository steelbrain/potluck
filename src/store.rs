//! The machine's own SQLite database: the jobs it was given (as JSON, with indexed id, key, state
//! and times), the workspaces it holds, and sequences for ids. Nothing is stored globally.
//! Finished jobs and their directories are pruned after the retention period, counted from when
//! this machine recorded each end on its own clocks ([`Ended`]): for a job another machine ran,
//! when that machine's report of its end arrived, whatever the runner's clock says.
//!
//! - One file, `potluck.db`, in WAL mode, so readers never wait on the writer; opened by the
//!   daemon's process alone (SQLite's `unix-excl`), so the WAL index is in memory, not in a
//!   memory-mapped file that a full disk turns into SIGBUS.
//! - Records are stored whole as JSON; the columns beside them exist only to find them.
//! - The schema version is SQLite's `user_version`. A database from a newer potluck is refused
//!   rather than guessed at.
//! - A small key-value table holds the daemon's own bookkeeping that has no shared type (for
//!   example how far a cancel or a run-limit kill has got), so a restart picks up where it was.

use crate::types::{JobId, JobRecord, Millis, WorkspaceRecord, WorkspaceState};
use rusqlite::{OptionalExtension, params};
use std::path::Path;

/// The schema this build writes. Bump it with a migration in [`Store::migrate`].
const SCHEMA_VERSION: i64 = 1;

/// The `kv` entries `ended/<job id>`: the boot-clock reading of when this machine recorded the
/// job's end, as `<boot ms> <boot id>`; deleted with the job.
pub const ENDED_PREFIX: &str = "ended/";

fn ended_key(id: &str) -> String {
    format!("{ENDED_PREFIX}{id}")
}

/// When this machine recorded a job's end, on its wall and boot clocks.
#[derive(Clone, Debug)]
pub struct Ended {
    pub wall: Millis,
    pub boot_ms: u64,
    /// The boot the boot-clock reading belongs to; empty when unknown (then none is kept).
    pub boot_id: String,
}

/// The boot clock now, for [`Store::prune_jobs`]: ends recorded in this boot are recent while
/// less than `within_ms` old on it.
#[derive(Clone, Debug)]
pub struct BootNow {
    pub id: String,
    pub now_ms: u64,
    pub within_ms: u64,
}

impl BootNow {
    /// Whether an `ended/` entry's reading is from this boot and less than `within_ms` old.
    fn recent(&self, entry: &str) -> bool {
        let Some((ms, id)) = entry.split_once(' ') else {
            return false;
        };
        !self.id.is_empty()
            && id == self.id
            && ms
                .parse::<u64>()
                .is_ok_and(|ms| self.now_ms.saturating_sub(ms) < self.within_ms)
    }
}

pub struct Store {
    conn: rusqlite::Connection,
}

impl Store {
    pub fn open(path: &Path) -> anyhow::Result<Store> {
        // SQLite's "unix-excl" VFS: the WAL index lives in this process's memory instead of a
        // memory-mapped `-shm` file. On a full btrfs, writing to that mapping killed the daemon
        // with SIGBUS (in walTryBeginRead); an ordinary write just fails and is made again
        // later. Other processes can't open the database meanwhile; only the daemon does (one
        // daemon per home).
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            path,
            rusqlite::OpenFlags::default(),
            c"unix-excl",
        )
        .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        // WAL: readers don't block the writer. NORMAL is durable across a process crash, which is
        // what matters here (a power loss may lose the last few changes, never corrupt).
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        let mut store = Store { conn };
        store
            .migrate()
            .map_err(|e| anyhow::anyhow!("{}: {e:#}", path.display()))?;
        Ok(store)
    }

    /// Closes the database (the last connection checkpoints its write-ahead log), for a daemon
    /// about to start another binary in its process. Anything asked of the store after this
    /// goes to an empty database in memory and is lost.
    pub fn close(&mut self) {
        if let Ok(empty) = rusqlite::Connection::open_in_memory() {
            let db = std::mem::replace(&mut self.conn, empty);
            let _ = db.close();
        }
    }

    fn migrate(&mut self) -> anyhow::Result<()> {
        let version: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            anyhow::bail!(
                "the database has schema version {version}, newer than this potluck's \
                 ({SCHEMA_VERSION}); update potluck or move the database aside"
            );
        }
        if version < 1 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(
                "CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    key TEXT UNIQUE,
                    state TEXT NOT NULL,
                    done INTEGER NOT NULL,
                    submitted_at INTEGER NOT NULL,
                    ended_at INTEGER,
                    json TEXT NOT NULL
                );
                CREATE INDEX jobs_state ON jobs(state);
                CREATE INDEX jobs_done_ended ON jobs(done, ended_at);
                CREATE INDEX jobs_submitted ON jobs(submitted_at);
                CREATE TABLE workspaces (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    repo_id TEXT NOT NULL,
                    last_used_at INTEGER NOT NULL,
                    json TEXT NOT NULL
                );
                CREATE INDEX workspaces_state ON workspaces(state);
                CREATE TABLE seqs (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
                CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                PRAGMA user_version = 1;",
            )?;
            tx.commit()?;
        }
        Ok(())
    }

    /// The next value of the named sequence (`job`, `workspace`). Starts at 1.
    pub fn next_seq(&mut self, name: &str) -> anyhow::Result<u64> {
        let v: i64 = self.conn.query_row(
            "INSERT INTO seqs (name, value) VALUES (?1, 1)
             ON CONFLICT(name) DO UPDATE SET value = value + 1
             RETURNING value",
            params![name],
            |r| r.get(0),
        )?;
        Ok(v as u64)
    }

    /// Writes a job's record, its end column taken from the record ([`Store::put_job_at`]
    /// without the time this machine recorded it).
    pub fn put_job(&mut self, job: &JobRecord) -> anyhow::Result<()> {
        self.put_job_at(job, None)
    }

    /// Writes a job's record. The first write of it as done also records `ended`, when this
    /// machine recorded its end (its `ended_at` is the runner's): the end column, which retention
    /// and the recent list go by, and the boot-clock reading in the `ended/<id>` entry. Later
    /// writes keep both. Without `ended`, the end column is the record's `ended_at`.
    pub fn put_job_at(&mut self, job: &JobRecord, ended: Option<&Ended>) -> anyhow::Result<()> {
        let json = serde_json::to_string(job)?;
        let done = job.state.is_done();
        let first_end = done
            && !self
                .conn
                .query_row(
                    "SELECT done FROM jobs WHERE id = ?1",
                    params![job.id],
                    |r| r.get::<_, bool>(0),
                )
                .optional()?
                .unwrap_or(false);
        let column = match ended {
            Some(e) if done => Some(e.wall),
            _ => job.ended_at,
        };
        // One transaction: the entry and the record's end are written together or not at all.
        let tx = self.conn.transaction()?;
        if first_end {
            let key = ended_key(&job.id);
            match ended.filter(|e| !e.boot_id.is_empty()) {
                Some(e) => tx.execute(
                    "INSERT INTO kv (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = ?2",
                    params![key, format!("{} {}", e.boot_ms, e.boot_id)],
                )?,
                None => tx.execute("DELETE FROM kv WHERE key = ?1", params![key])?,
            };
        }
        tx.execute(
            "INSERT INTO jobs (id, key, state, done, submitted_at, ended_at, json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET key = ?2, state = ?3, done = ?4, submitted_at = ?5,
                 ended_at = CASE WHEN jobs.done = 1 AND ?4 = 1 THEN jobs.ended_at ELSE ?6 END,
                 json = ?7",
            params![
                job.id,
                job.spec.key,
                job.state.as_str(),
                done,
                job.submitted_at as i64,
                column.map(|t| t as i64),
                json
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Forgets a job (a claim of another machine's job that was refused).
    pub fn delete_job(&mut self, id: &str) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM jobs WHERE id = ?1", params![id])?;
        tx.execute("DELETE FROM kv WHERE key = ?1", params![ended_key(id)])?;
        tx.commit()?;
        Ok(())
    }

    pub fn job(&self, id: &str) -> anyhow::Result<Option<JobRecord>> {
        self.one_job("SELECT json FROM jobs WHERE id = ?1", id)
    }

    /// The id of the job dispatched with `key`, if any (from its column: also when its record
    /// can't be read).
    pub fn job_by_key(&self, key: &str) -> anyhow::Result<Option<JobId>> {
        Ok(self
            .conn
            .query_row("SELECT id FROM jobs WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    fn one_job(&self, sql: &str, arg: &str) -> anyhow::Result<Option<JobRecord>> {
        let json: Option<String> = self
            .conn
            .query_row(sql, params![arg], |r| r.get(0))
            .optional()?;
        json.map(|j| parse_job(&j)).transpose()
    }

    /// Jobs not done yet, to rebuild state after a restart, oldest first. One whose record can't
    /// be read fails it, naming the job: the daemon must not lose a running job's record.
    pub fn active_jobs(&self) -> anyhow::Result<Vec<JobRecord>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, json FROM jobs WHERE done = 0 ORDER BY submitted_at, id")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            let (id, json) = row?;
            out.push(parse_job(&json).map_err(|e| {
                anyhow::anyhow!(
                    "job {id}, not finished: {e:#}; a newer potluck may have written it: run \
                     that version, or let it finish there"
                )
            })?);
        }
        Ok(out)
    }

    /// The `limit` finished jobs that ended last, newest first; records this potluck can't read
    /// (a newer one wrote them) are left out, so the rest still show.
    pub fn recent_jobs(&self, limit: usize) -> anyhow::Result<Vec<JobRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT json FROM jobs WHERE done = 1 ORDER BY ended_at DESC, id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for json in rows {
            if let Ok(job) = parse_job(&json?) {
                out.push(job);
            }
        }
        Ok(out)
    }

    /// Deletes finished jobs whose end this machine recorded before `before` (wall clock), except
    /// those it recorded in boot `boot.id` less than `boot.within_ms` before `boot.now_ms` on its
    /// boot clock: a step of the wall clock doesn't make them old. Returns their ids so their
    /// directories go too. Their records aren't read, so one this potluck can't read goes as well.
    pub fn prune_jobs(&mut self, before: Millis, boot: &BootNow) -> anyhow::Result<Vec<JobId>> {
        let candidates: Vec<(JobId, Option<String>)> = {
            let mut stmt = self.conn.prepare(
                "SELECT jobs.id, kv.value FROM jobs LEFT JOIN kv ON kv.key = ?2 || jobs.id
                 WHERE jobs.done = 1 AND jobs.ended_at < ?1",
            )?;
            let rows = stmt.query_map(params![before as i64, ENDED_PREFIX], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
            rows.collect::<Result<_, _>>()?
        };
        let mut ids: Vec<JobId> = candidates
            .into_iter()
            .filter(|(_, ended)| !ended.as_deref().is_some_and(|v| boot.recent(v)))
            .map(|(id, _)| id)
            .collect();
        ids.sort();
        let tx = self.conn.transaction()?;
        for id in &ids {
            tx.execute("DELETE FROM jobs WHERE id = ?1", params![id])?;
            tx.execute("DELETE FROM kv WHERE key = ?1", params![ended_key(id)])?;
        }
        tx.commit()?;
        Ok(ids)
    }

    pub fn put_workspace(&mut self, ws: &WorkspaceRecord) -> anyhow::Result<()> {
        let json = serde_json::to_string(ws)?;
        self.conn.execute(
            "INSERT INTO workspaces (id, state, repo_id, last_used_at, json)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET state = ?2, repo_id = ?3, last_used_at = ?4, json = ?5",
            params![
                ws.id,
                ws.state.as_str(),
                ws.repo_id,
                ws.last_used_at as i64,
                json
            ],
        )?;
        Ok(())
    }

    /// Workspaces not removed.
    pub fn workspaces(&self) -> anyhow::Result<Vec<WorkspaceRecord>> {
        self.workspace_list("SELECT json FROM workspaces WHERE state != ?1 ORDER BY id")
    }

    /// Records of removed workspaces not pruned yet (a removal that failed or was cut short is
    /// done again from them).
    pub fn removed_workspaces(&self) -> anyhow::Result<Vec<WorkspaceRecord>> {
        self.workspace_list("SELECT json FROM workspaces WHERE state = ?1 ORDER BY id")
    }

    fn workspace_list(&self, sql: &str) -> anyhow::Result<Vec<WorkspaceRecord>> {
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![WorkspaceState::Removed.as_str()], |r| {
            r.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for json in rows {
            let json = json?;
            out.push(
                serde_json::from_str(&json)
                    .map_err(|e| anyhow::anyhow!("bad workspace record in the database: {e}"))?,
            );
        }
        Ok(out)
    }

    /// One workspace record, removed or not.
    pub fn workspace(&self, id: &str) -> anyhow::Result<Option<WorkspaceRecord>> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT json FROM workspaces WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?;
        json.map(|j| {
            serde_json::from_str(&j)
                .map_err(|e| anyhow::anyhow!("bad workspace record in the database: {e}"))
        })
        .transpose()
    }

    /// Deletes records of workspaces removed and last used before `before`, except those named in
    /// `keep` (whose files are still there); returns their ids.
    pub fn prune_workspaces(
        &mut self,
        before: Millis,
        keep: &std::collections::BTreeSet<String>,
    ) -> anyhow::Result<Vec<String>> {
        let tx = self.conn.transaction()?;
        let ids: Vec<String> = {
            let mut stmt =
                tx.prepare("SELECT id FROM workspaces WHERE state = ?1 AND last_used_at < ?2")?;
            let rows = stmt.query_map(
                params![WorkspaceState::Removed.as_str(), before as i64],
                |r| r.get::<_, String>(0),
            )?;
            rows.collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|id| !keep.contains(id))
                .collect()
        };
        for id in &ids {
            tx.execute("DELETE FROM workspaces WHERE id = ?1", params![id])?;
        }
        tx.commit()?;
        Ok(ids)
    }

    /// The daemon's own bookkeeping: sets `key` to `value`.
    pub fn put_kv(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = ?2",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn kv(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM kv WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// Every bookkeeping entry whose key starts with `prefix`, by key.
    pub fn kv_prefixed(&self, prefix: &str) -> anyhow::Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT key, value FROM kv WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
        )?;
        let rows = stmt.query_map(params![prefix], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn delete_kv(&mut self, key: &str) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM kv WHERE key = ?1", params![key])?;
        Ok(())
    }
}

fn parse_job(json: &str) -> anyhow::Result<JobRecord> {
    serde_json::from_str(json).map_err(|e| anyhow::anyhow!("bad job record in the database: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobSpec, JobState, RepoRef, Request, Usage};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn job(id: &str, key: Option<&str>, state: JobState, submitted: Millis) -> JobRecord {
        JobRecord {
            id: id.into(),
            spec: JobSpec {
                key: key.map(Into::into),
                labels: BTreeMap::new(),
                priority: 3,
                request: Request::default(),
                on: BTreeMap::new(),
                repo: RepoRef {
                    id: "repo-0123abcd".into(),
                    source: PathBuf::from("/src/.git"),
                    commit: "a".repeat(40),
                    branch: Some("main".into()),
                    submodules: vec![],
                    carried: vec![],
                },
                workspace: None,
                cmd: vec!["true".into()],
                env: BTreeMap::new(),
                unset_env: Default::default(),
                timeout_ms: 60_000,
                hold_ms: None,
                keep: vec![],
                rerun_if_lost: true,
                kill_on_stall: false,
                recipe: None,
                submitted_from: "here".into(),
            },
            state,
            submitted_at: submitted,
            started_at: None,
            ended_at: state.is_done().then_some(submitted + 10),
            node: None,
            workspace: None,
            lease: None,
            handle: None,
            exit_code: None,
            signal: None,
            message: None,
            stuck_pids: vec![],
            usage: Usage::default(),
            passes: 0,
            stalled: false,
            kept: vec![],
            notes: Vec::new(),
        }
    }

    fn ws(id: &str, state: WorkspaceState, last_used: Millis) -> WorkspaceRecord {
        WorkspaceRecord {
            id: id.into(),
            node: "here".into(),
            repo_id: "repo-0123abcd".into(),
            commit: "a".repeat(40),
            branch: Some("main".into()),
            path: PathBuf::from("/w").join(id),
            state,
            created_at: 1,
            last_used_at: last_used,
            held_until: 0,
            pending: 0,
            size_bytes: None,
        }
    }

    #[test]
    fn jobs_round_trip_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("potluck.db");
        let mut s = Store::open(&path).unwrap();
        let mut a = job("here-1", Some("k1"), JobState::Queued, 100);
        s.put_job(&a).unwrap();
        s.put_job(&job("here-2", None, JobState::Running, 200))
            .unwrap();
        s.put_job(&job("here-3", None, JobState::Succeeded, 50))
            .unwrap();
        assert_eq!(s.job("here-1").unwrap().unwrap(), a);
        assert_eq!(s.job_by_key("k1").unwrap().as_deref(), Some("here-1"));
        assert!(s.job_by_key("nope").unwrap().is_none());
        assert!(s.job("here-9").unwrap().is_none());

        // An update replaces the record and its indexed columns.
        a.state = JobState::Failed;
        a.ended_at = Some(150);
        a.exit_code = Some(3);
        s.put_job(&a).unwrap();
        drop(s);

        let mut s = Store::open(&path).unwrap();
        assert_eq!(s.job("here-1").unwrap().unwrap().exit_code, Some(3));
        s.put_job(&job("there-1", None, JobState::Starting, 300))
            .unwrap();
        s.delete_job("there-1").unwrap();
        assert!(s.job("there-1").unwrap().is_none());
        let active: Vec<_> = s.active_jobs().unwrap().into_iter().map(|j| j.id).collect();
        assert_eq!(active, ["here-2"]);
        let recent: Vec<_> = s
            .recent_jobs(10)
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        assert_eq!(recent, ["here-1", "here-3"], "newest ended first");
        assert_eq!(s.recent_jobs(1).unwrap().len(), 1);
    }

    /// The WAL index is kept in memory, not in a memory-mapped `-shm` file: on a full btrfs a
    /// write to that mapping killed the daemon with SIGBUS.
    #[test]
    fn no_shared_memory_file_is_mapped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("potluck.db");
        let mut s = Store::open(&path).unwrap();
        s.put_job(&job("here-1", None, JobState::Queued, 1))
            .unwrap();
        assert!(s.job("here-1").unwrap().is_some());
        assert!(!dir.path().join("potluck.db-shm").exists());
        drop(s);
        // Reopened, with what was written.
        let s = Store::open(&path).unwrap();
        assert!(s.job("here-1").unwrap().is_some());
        assert!(!dir.path().join("potluck.db-shm").exists());
    }

    #[test]
    fn keys_are_unique() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("db")).unwrap();
        s.put_job(&job("here-1", Some("k"), JobState::Queued, 1))
            .unwrap();
        assert!(
            s.put_job(&job("here-2", Some("k"), JobState::Queued, 2))
                .is_err()
        );
        // Jobs without a key don't collide.
        s.put_job(&job("here-3", None, JobState::Queued, 3))
            .unwrap();
        s.put_job(&job("here-4", None, JobState::Queued, 4))
            .unwrap();
    }

    #[test]
    fn sequences_count_up_per_name_and_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut s = Store::open(&path).unwrap();
        assert_eq!(s.next_seq("job").unwrap(), 1);
        assert_eq!(s.next_seq("job").unwrap(), 2);
        assert_eq!(s.next_seq("workspace").unwrap(), 1);
        drop(s);
        let mut s = Store::open(&path).unwrap();
        assert_eq!(s.next_seq("job").unwrap(), 3);
    }

    /// Boot "b1" at 1000 ms on its boot clock, recent meaning under 500 ms.
    fn boot() -> BootNow {
        BootNow {
            id: "b1".into(),
            now_ms: 1_000,
            within_ms: 500,
        }
    }

    fn ended(wall: Millis, boot_ms: u64, boot_id: &str) -> Ended {
        Ended {
            wall,
            boot_ms,
            boot_id: boot_id.into(),
        }
    }

    /// Retention goes by when this machine recorded each end, not by the record's `ended_at` (a
    /// runner's clock): the first save as done sets it, later saves keep it. An end recorded in
    /// this boot is pruned only once the boot clock agrees; one from another boot, or written
    /// before ends were recorded so (no `ended/` entry), goes by the wall clock alone.
    #[test]
    fn prune_goes_by_when_this_machine_recorded_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("db")).unwrap();
        // Ended at 20 by its runner's clock, recorded here at 900: not old.
        let mut a = job("here-1", None, JobState::Running, 10);
        s.put_job_at(&a, Some(&ended(1, 1, "b1"))).unwrap();
        a.state = JobState::Succeeded;
        a.ended_at = Some(20);
        s.put_job_at(&a, Some(&ended(900, 990, "b1"))).unwrap();
        // Saved again later (a late crash report, say): when it ended here stays.
        s.put_job_at(&a, Some(&ended(5_000, 4_000, "b1"))).unwrap();
        assert_eq!(s.prune_jobs(500, &boot()).unwrap(), Vec::<JobId>::new());
        assert_eq!(s.kv("ended/here-1").unwrap().as_deref(), Some("990 b1"));
        // Recorded at 100 on the wall clock, but 100 ms ago on this boot's: a step of the wall
        // clock, kept. Recorded in another boot, or at an unknown boot-clock time: wall clock.
        s.put_job_at(
            &job("here-2", None, JobState::Failed, 1),
            Some(&ended(100, 900, "b1")),
        )
        .unwrap();
        s.put_job_at(
            &job("here-3", None, JobState::Failed, 1),
            Some(&ended(100, 900, "b0")),
        )
        .unwrap();
        s.put_job_at(
            &job("here-4", None, JobState::Failed, 1),
            Some(&ended(100, 900, "")),
        )
        .unwrap();
        assert_eq!(s.kv("ended/here-4").unwrap(), None);
        // As a potluck before this wrote it: the record's end, no entry.
        s.put_job(&job("here-5", None, JobState::Failed, 1))
            .unwrap();
        // Recorded in this boot long enough ago on both clocks.
        s.put_job_at(
            &job("here-6", None, JobState::Failed, 1),
            Some(&ended(100, 400, "b1")),
        )
        .unwrap();
        assert_eq!(
            s.prune_jobs(500, &boot()).unwrap(),
            ["here-3", "here-4", "here-5", "here-6"]
        );
        assert_eq!(s.kv("ended/here-3").unwrap(), None, "deleted with its job");
        assert_eq!(s.kv("ended/here-6").unwrap(), None);
        assert!(s.job("here-2").unwrap().is_some());
        // Later, by both clocks.
        let later = BootNow {
            now_ms: 2_000,
            ..boot()
        };
        assert_eq!(s.prune_jobs(500, &later).unwrap(), ["here-2"]);
        assert!(s.job("here-1").unwrap().is_some());
        // A refused claim's job is forgotten with its entry.
        s.put_job_at(
            &job("there-1", None, JobState::Cancelled, 1),
            Some(&ended(100, 900, "b1")),
        )
        .unwrap();
        s.delete_job("there-1").unwrap();
        assert_eq!(s.kv("ended/there-1").unwrap(), None);
    }

    #[test]
    fn prune_takes_only_old_finished_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("db")).unwrap();
        s.put_job(&job("here-1", Some("old"), JobState::Succeeded, 10))
            .unwrap(); // ended 20
        s.put_job(&job("here-2", None, JobState::Failed, 1000))
            .unwrap(); // ended 1010
        s.put_job(&job("here-3", None, JobState::Running, 5))
            .unwrap();
        assert_eq!(s.prune_jobs(500, &boot()).unwrap(), ["here-1"]);
        assert!(s.job("here-1").unwrap().is_none());
        assert!(
            s.job("here-3").unwrap().is_some(),
            "running jobs are never pruned"
        );
        // A pruned key is free again.
        assert!(s.job_by_key("old").unwrap().is_none());
    }

    /// A record this potluck can't read (written by a newer one before a downgrade, or damaged)
    /// doesn't stop pruning, `status` or keys: they need its id, not its record.
    #[test]
    fn a_record_that_doesnt_parse_stops_nothing_that_needs_only_its_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("db")).unwrap();
        s.put_job(&job("here-1", Some("k"), JobState::Succeeded, 10))
            .unwrap();
        s.put_job(&job("here-2", None, JobState::Failed, 20))
            .unwrap();
        s.conn
            .execute(
                "UPDATE jobs SET json = '{\"id\":\"here-1\",\"state\":\"from_the_future\"}' \
                 WHERE id = 'here-1'",
                [],
            )
            .unwrap();
        assert_eq!(s.job_by_key("k").unwrap().as_deref(), Some("here-1"));
        let recent: Vec<_> = s
            .recent_jobs(10)
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        assert_eq!(recent, ["here-2"]);
        assert_eq!(s.prune_jobs(500, &boot()).unwrap(), ["here-1", "here-2"]);
        assert!(s.job_by_key("k").unwrap().is_none());
        // A job not finished whose record can't be read is named, not skipped.
        s.put_job(&job("here-3", None, JobState::Running, 30))
            .unwrap();
        s.conn
            .execute("UPDATE jobs SET json = '{}' WHERE id = 'here-3'", [])
            .unwrap();
        let e = format!("{:#}", s.active_jobs().unwrap_err());
        assert!(e.starts_with("job here-3, not finished: "), "{e}");
    }

    #[test]
    fn workspaces_list_skips_removed_and_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("db")).unwrap();
        s.put_workspace(&ws("here-w1", WorkspaceState::Held, 10))
            .unwrap();
        s.put_workspace(&ws("here-w2", WorkspaceState::Removed, 10))
            .unwrap();
        s.put_workspace(&ws("here-w3", WorkspaceState::Removed, 900))
            .unwrap();
        let mut w1 = ws("here-w1", WorkspaceState::Idle, 20);
        w1.size_bytes = Some(42);
        s.put_workspace(&w1).unwrap();
        assert_eq!(s.workspaces().unwrap(), vec![w1]);
        let removed: Vec<String> = s
            .removed_workspaces()
            .unwrap()
            .into_iter()
            .map(|w| w.id)
            .collect();
        assert_eq!(removed, ["here-w2", "here-w3"]);
        assert_eq!(
            s.workspace("here-w2").unwrap().unwrap().state,
            WorkspaceState::Removed
        );
        let keep = std::collections::BTreeSet::from(["here-w2".to_string()]);
        assert!(s.prune_workspaces(500, &keep).unwrap().is_empty());
        assert_eq!(
            s.prune_workspaces(500, &Default::default()).unwrap(),
            ["here-w2"]
        );
        assert!(s.workspace("here-w2").unwrap().is_none());
        assert!(s.workspace("here-w3").unwrap().is_some());
    }

    #[test]
    fn kv_and_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut s = Store::open(&path).unwrap();
        assert_eq!(s.kv("a").unwrap(), None);
        s.put_kv("a", "1").unwrap();
        s.put_kv("a", "2").unwrap();
        assert_eq!(s.kv("a").unwrap().as_deref(), Some("2"));
        s.delete_kv("a").unwrap();
        assert_eq!(s.kv("a").unwrap(), None);
        for k in ["run/x", "cleanup/b", "cleanup/a", "cleanups", "cleanup%"] {
            s.put_kv(k, k).unwrap();
        }
        let got: Vec<String> = s
            .kv_prefixed("cleanup/")
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(got, ["cleanup/a", "cleanup/b"]);
        let mode: String = s
            .conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        // A second connection (another reader in this process) sees committed writes.
        s.put_kv("b", "x").unwrap();
        let other = Store::open(&path).unwrap();
        assert_eq!(other.kv("b").unwrap().as_deref(), Some("x"));
    }

    #[test]
    fn refuses_a_newer_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(Store::open(&path).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);
        let err = Store::open(&path).err().unwrap().to_string();
        assert!(err.contains("schema version 99"), "{err}");
    }
}
