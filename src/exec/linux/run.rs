//! Running helper programs (systemctl, systemd-run, eu-stack, coredumpctl) with a time limit, so a
//! hung user manager or debugger can't hang the daemon.

use std::io::Read;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub(super) struct Output {
    /// `None` when the program ran out of time and was killed.
    pub status: Option<ExitStatus>,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn success(&self) -> bool {
        self.status.is_some_and(|s| s.success())
    }

    /// What went wrong, for an error message: stderr, else stdout, else the exit status.
    pub fn message(&self) -> String {
        let text = if self.stderr.trim().is_empty() {
            self.stdout.trim()
        } else {
            self.stderr.trim()
        };
        match (text.is_empty(), self.status) {
            (_, None) => format!("timed out {text}").trim_end().to_string(),
            (true, Some(s)) => s.to_string(),
            (false, _) => text.to_string(),
        }
    }
}

/// Runs `cmd` with stdin from /dev/null, collecting its output; kills it after `timeout`.
pub(super) fn run(cmd: &mut Command, timeout: Duration) -> std::io::Result<Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = reader(child.stdout.take());
    let stderr = reader(child.stderr.take());
    let deadline = Instant::now() + timeout;
    let mut pause = Duration::from_millis(1);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            // Our own child, still unreaped, so the pid is still ours.
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(20));
    };
    // A grandchild could hold a pipe open after the child is gone: don't wait for it forever.
    let collect = |rx: mpsc::Receiver<Vec<u8>>| {
        rx.recv_timeout(Duration::from_secs(1))
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default()
    };
    Ok(Output {
        status,
        stdout: collect(stdout),
        stderr: collect(stderr),
    })
}

fn reader<R: Read + Send + 'static>(pipe: Option<R>) -> mpsc::Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    match pipe {
        Some(mut pipe) => {
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = pipe.read_to_end(&mut buf);
                let _ = tx.send(buf);
            });
        }
        None => {
            let _ = tx.send(Vec::new());
        }
    }
    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_output_and_status() {
        let out = run(
            Command::new("sh").args(["-c", "echo out; echo err >&2; exit 3"]),
            Duration::from_secs(30),
        )
        .unwrap();
        assert_eq!(out.status.and_then(|s| s.code()), Some(3));
        assert_eq!(
            (out.stdout.as_str(), out.stderr.as_str()),
            ("out\n", "err\n")
        );
        assert!(!out.success());
        assert_eq!(out.message(), "err");
    }

    #[test]
    fn kills_at_the_time_limit() {
        let started = Instant::now();
        let out = run(Command::new("sleep").arg("30"), Duration::from_millis(200)).unwrap();
        assert!(out.status.is_none());
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(out.message().starts_with("timed out"));
    }
}
