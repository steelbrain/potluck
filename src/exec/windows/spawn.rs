//! Starting the supervisor: `CreateProcessW` with what `std::process::Command` can't give it,
//! a job object from its first instruction (PROC_THREAD_ATTRIBUTE_JOB_LIST), exactly two
//! inherited handles (PROC_THREAD_ATTRIBUTE_HANDLE_LIST) and a console of its own without a
//! window.

use crate::sys::win::{Owned, wide};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_ACCESS_DENIED, HANDLE,
};
use windows_sys::Win32::System::JobObjects::IsProcessInJob;
use windows_sys::Win32::System::Threading::{
    CREATE_BREAKAWAY_FROM_JOB, CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess,
    InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_JOB_LIST, PROCESS_INFORMATION,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, UpdateProcThreadAttribute,
};

/// What to start.
pub struct Spawn<'a> {
    pub program: &'a Path,
    /// The arguments after the program's own name.
    pub args: &'a [OsString],
    /// Exactly the environment it gets.
    pub env: &'a BTreeMap<String, String>,
    pub cwd: &'a Path,
    pub stdin: &'a File,
    /// Its stdout and stderr.
    pub stdout: &'a File,
    /// The job object it runs in from its first instruction.
    pub job: HANDLE,
}

/// A started process.
pub struct Child {
    pub process: Owned,
}

/// Starts `s.program` in job `s.job`, with a console of its own that has no window
/// (CREATE_NO_WINDOW), stdin and stdout/stderr as given and no other handle of this process.
/// When this process runs in a job of its own (a service's), the new one leaves it
/// (CREATE_BREAKAWAY_FROM_JOB), so it outlives this process's job; when that job doesn't allow
/// it, it starts within it.
pub fn spawn(s: &Spawn) -> io::Result<Child> {
    let mut cmdline = command_line(s.program, s.args)?;
    let env = env_block(s.env)?;
    let program = wide(s.program);
    let cwd = wide(s.cwd);
    // The handles it inherits must be inheritable here. Duplicates are, for as long as this
    // takes; a process another thread of this one starts meanwhile may inherit them too, and
    // only holds them open.
    let stdin = inheritable(s.stdin.as_raw_handle())?;
    let stdout = inheritable(s.stdout.as_raw_handle())?;
    let handles = [stdin.0, stdout.0];
    let jobs = [s.job];
    let list = AttributeList::new(2)?;
    list.set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &handles)?;
    list.set(PROC_THREAD_ATTRIBUTE_JOB_LIST, &jobs)?;
    let mut si = STARTUPINFOEXW::default();
    si.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    si.StartupInfo.hStdInput = stdin.0;
    si.StartupInfo.hStdOutput = stdout.0;
    si.StartupInfo.hStdError = stdout.0;
    si.lpAttributeList = list.ptr();
    let base = EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW;
    let breakaway = if in_a_job() {
        CREATE_BREAKAWAY_FROM_JOB
    } else {
        0
    };
    let mut flags = base | breakaway;
    loop {
        let mut pi = PROCESS_INFORMATION::default();
        // SAFETY: every pointer is to a live, NUL-terminated buffer (the command line one is
        // writable, as CreateProcessW wants); the attribute list and what it points to live until
        // after the call; pi is filled on success.
        let ok = unsafe {
            CreateProcessW(
                program.as_ptr(),
                cmdline.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1,
                flags,
                env.as_ptr().cast(),
                cwd.as_ptr(),
                &raw const si.StartupInfo,
                &mut pi,
            )
        };
        if ok != 0 {
            drop(Owned::new(pi.hThread));
            let process = Owned::new(pi.hProcess)
                .ok_or_else(|| io::Error::other("CreateProcessW gave no process handle"))?;
            return Ok(Child { process });
        }
        let e = io::Error::last_os_error();
        // This process's job doesn't let it go: it starts within it.
        if flags & CREATE_BREAKAWAY_FROM_JOB != 0
            && e.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32)
        {
            flags = base;
            continue;
        }
        return Err(e);
    }
}

/// Gives process `process` a handle of its own to `h` (not inheritable), which it holds until it
/// exits, without knowing of it.
pub fn give(process: HANDLE, h: HANDLE) -> io::Result<()> {
    let mut theirs: HANDLE = std::ptr::null_mut();
    // SAFETY: valid handles: this process's own, `h`, and `process` with PROCESS_DUP_HANDLE.
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            h,
            process,
            &mut theirs,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// An inheritable duplicate of `h`.
fn inheritable(h: HANDLE) -> io::Result<Owned> {
    let mut dup: HANDLE = std::ptr::null_mut();
    // SAFETY: valid handles of this process; dup receives a new one, closed by Owned.
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            h,
            GetCurrentProcess(),
            &mut dup,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Owned::new(dup).ok_or_else(|| io::Error::other("DuplicateHandle gave no handle"))
}

/// Whether this process runs in a job object.
fn in_a_job() -> bool {
    let mut r = 0;
    // SAFETY: this process's pseudo handle; a null job asks about any job.
    let ok = unsafe { IsProcessInJob(GetCurrentProcess(), std::ptr::null_mut(), &mut r) };
    ok != 0 && r != 0
}

/// A process and thread attribute list of a fixed number of entries.
struct AttributeList {
    /// usizes keep it aligned for the list.
    buf: Vec<usize>,
}

impl AttributeList {
    fn new(count: u32) -> io::Result<AttributeList> {
        let mut size = 0usize;
        // SAFETY: a size query: no list, size receives the bytes needed (the call then fails
        // with ERROR_INSUFFICIENT_BUFFER, which is expected).
        unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), count, 0, &mut size) };
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut buf = vec![0usize; size.div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: a buffer of at least `size` bytes, aligned.
        let ok = unsafe {
            InitializeProcThreadAttributeList(buf.as_mut_ptr().cast(), count, 0, &mut size)
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // Initialized: deleted on drop from here on.
        Ok(AttributeList { buf })
    }

    fn ptr(&self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buf.as_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST
    }

    /// Sets `attribute` to `values`, which must outlive the list's use (the list keeps a pointer
    /// to them, not a copy).
    fn set(&self, attribute: u32, values: &[HANDLE]) -> io::Result<()> {
        // SAFETY: an initialized list; values is a live array of the size given.
        let ok = unsafe {
            UpdateProcThreadAttribute(
                self.ptr(),
                0,
                attribute as usize,
                values.as_ptr().cast(),
                std::mem::size_of_val(values),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        // SAFETY: an initialized list, deleted once.
        unsafe { DeleteProcThreadAttributeList(self.ptr()) };
    }
}

/// The command line for `program` and `args`, NUL-terminated: the program's path in quotes (a
/// path holds no quote), then each argument quoted as the C runtime and Rust's own argument
/// parsing read it back, as `std::process::Command` writes them.
pub fn command_line(program: &Path, args: &[OsString]) -> io::Result<Vec<u16>> {
    let mut cmd: Vec<u16> = Vec::new();
    cmd.push(u16::from(b'"'));
    cmd.extend(program.as_os_str().encode_wide());
    cmd.push(u16::from(b'"'));
    for arg in args {
        cmd.push(u16::from(b' '));
        push_arg(&mut cmd, arg)?;
    }
    if cmd.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an argument holds a NUL character",
        ));
    }
    cmd.push(0);
    Ok(cmd)
}

/// One argument: in quotes when it is empty or holds a space or tab; a run of backslashes is
/// doubled before a quote (which is escaped) and before the closing quote.
fn push_arg(cmd: &mut Vec<u16>, arg: &OsStr) -> io::Result<()> {
    let units: Vec<u16> = arg.encode_wide().collect();
    let quote = units.is_empty() || units.iter().any(|&c| c == 0x20 || c == 0x09);
    if quote {
        cmd.push(u16::from(b'"'));
    }
    let mut backslashes = 0usize;
    for &c in &units {
        if c == u16::from(b'\\') {
            backslashes += 1;
        } else {
            if c == u16::from(b'"') {
                // 2n + 1 backslashes in all before an inner quote.
                cmd.extend(std::iter::repeat_n(u16::from(b'\\'), backslashes + 1));
            }
            backslashes = 0;
        }
        cmd.push(c);
    }
    if quote {
        // 2n before the closing quote.
        cmd.extend(std::iter::repeat_n(u16::from(b'\\'), backslashes));
        cmd.push(u16::from(b'"'));
    }
    Ok(())
}

/// An environment block for CreateProcessW: `KEY=VALUE` strings, each ended by a NUL, then one
/// more NUL. Windows reads names without regard to case and expects them sorted so; of names
/// that differ only in case the first in `env`'s order is kept (`PATH` before `Path`).
pub fn env_block(env: &BTreeMap<String, String>) -> io::Result<Vec<u16>> {
    let mut vars: Vec<(&String, &String)> = env.iter().collect();
    // Stable: names equal but for case keep `env`'s order, and dedup keeps the first.
    vars.sort_by_cached_key(|(k, _)| k.to_uppercase());
    vars.dedup_by(|b, a| a.0.to_uppercase() == b.0.to_uppercase());
    let mut block: Vec<u16> = Vec::new();
    for (key, value) in vars {
        // A name may start with `=` (`=C:`, the current directory of drive C:), but hold none
        // after its first character.
        if key.is_empty()
            || key.chars().skip(1).any(|c| c == '=')
            || key.contains('\0')
            || value.contains('\0')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid environment variable {key:?}"),
            ));
        }
        block.extend(OsStr::new(key).encode_wide());
        block.push(u16::from(b'='));
        block.extend(OsStr::new(value).encode_wide());
        block.push(0);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(units: &[u16]) -> String {
        String::from_utf16(units).unwrap()
    }

    #[test]
    fn arguments_are_quoted_as_the_c_runtime_reads_them() {
        let args: Vec<OsString> = [
            "plain",
            "",
            "a b",
            "tab\there",
            r#"q"uote"#,
            r"back\slash\",
            r#"end\"#,
            r#"x\"y"#,
        ]
        .iter()
        .map(OsString::from)
        .collect();
        let line = command_line(Path::new(r"C:\p\potluck.exe"), &args).unwrap();
        assert_eq!(line.last(), Some(&0));
        assert_eq!(
            text(&line[..line.len() - 1]),
            r#""C:\p\potluck.exe" plain "" "a b" "tab	here" q\"uote back\slash\ end\ x\\\"y"#
        );
        let with_nul = [OsString::from("a\0b")];
        assert!(command_line(Path::new("p"), &with_nul).is_err());
    }

    #[test]
    fn the_environment_block_is_sorted_without_case_and_ends_twice() {
        let env: BTreeMap<String, String> =
            [("b", "2"), ("PATH", "job"), ("Path", "login"), ("A", "1")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        let block = env_block(&env).unwrap();
        assert_eq!(text(&block), "A=1\0b=2\0PATH=job\0\0");
        assert_eq!(text(&env_block(&BTreeMap::new()).unwrap()), "\0\0");
        let drive = BTreeMap::from([("=C:".to_string(), r"C:\x".to_string())]);
        assert!(env_block(&drive).is_ok());
        for (k, v) in [("", "x"), ("A=B", "x"), ("A", "x\0y")] {
            let env = BTreeMap::from([(k.to_string(), v.to_string())]);
            assert!(env_block(&env).is_err(), "{k:?}={v:?}");
        }
    }
}
