//! `forge slot -- <command>`: runs a command holding the box's one build slot,
//! so sessions take turns building (one big build can fill the box's memory).

use std::fs::{File, OpenOptions};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Command, ExitStatus};

use anyhow::{Context, Result};

/// Waits for the lock at `lock_path`, runs `cmd` with inherited stdio, and
/// returns its exit code. The lock is released when the command ends.
pub fn run_in_slot(lock_path: &Path, cmd: &[String]) -> Result<i32> {
    let (program, args) = cmd.split_first().context("forge slot needs a command to run")?;
    let mut lock = fd_lock::RwLock::new(open_lock(lock_path)?);
    let _held = lock.write().with_context(|| format!("locking {}", lock_path.display()))?;
    let status = Command::new(program).args(args).status().with_context(|| format!("running {program}"))?;
    Ok(exit_code(status))
}

/// Opens the lock file, making it if missing. One this user can only read
/// still locks.
fn open_lock(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .or_else(|_| File::open(path))
        .with_context(|| format!("opening {}", path.display()))
}

/// The command's exit code; a signal `n` is 128 + n, as shells report it.
fn exit_code(status: ExitStatus) -> i32 {
    status.code().or(status.signal().map(|n| 128 + n)).unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    fn cmd(script: &str) -> Vec<String> {
        vec!["sh".into(), "-c".into(), script.into()]
    }

    #[test]
    fn passes_the_exit_code_through() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("build.lock");
        assert_eq!(run_in_slot(&lock, &cmd("exit 0")).unwrap(), 0);
        assert!(lock.exists());
        assert_eq!(run_in_slot(&lock, &cmd("exit 3")).unwrap(), 3);
        assert_eq!(run_in_slot(&lock, &cmd("kill -9 $$")).unwrap(), 137);
        assert!(run_in_slot(&lock, &[]).is_err());
        assert!(run_in_slot(&lock, &["/no/such/program".into()]).is_err());
    }

    #[test]
    fn waits_for_the_holder() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("build.lock");
        let released = Arc::new(AtomicBool::new(false));
        let (held_tx, held_rx) = mpsc::channel();
        let holder = {
            let (lock, released) = (lock.clone(), released.clone());
            std::thread::spawn(move || {
                let mut l = fd_lock::RwLock::new(open_lock(&lock).unwrap());
                let _guard = l.write().unwrap();
                held_tx.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(300));
                released.store(true, Ordering::SeqCst);
            })
        };
        held_rx.recv().unwrap();
        let marker = dir.path().join("ran");
        let code = run_in_slot(&lock, &cmd(&format!("touch {}", marker.display()))).unwrap();
        assert_eq!(code, 0);
        assert!(released.load(Ordering::SeqCst), "the command ran while another held the slot");
        assert!(marker.exists());
        holder.join().unwrap();
    }
}
