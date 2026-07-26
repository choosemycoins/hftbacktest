//! One collector per output directory, enforced by the kernel.
//!
//! Two collectors sharing a directory do not merely duplicate work: both open
//! the same `<symbol>_<date>.gz` in append mode, and their `GzEncoder`s
//! interleave ~48 KB blocks from two independent deflate streams into one file.
//! Neither member can then be decoded — the day is lost for both instances, and
//! nothing about the running processes looks wrong while it happens. That is
//! why this is a hard refusal to start rather than a warning.
//!
//! The window is real: `deploy/install.sh` restarts instances one at a time, so
//! the collision arrives from host boot bringing several units up together, or
//! from an operator running the binary by hand against a directory a service is
//! already recording into.
//!
//! `flock` is the right primitive for it. The lock lives on the open file
//! description, so the kernel drops it when the process exits **however** it
//! exits — including SIGKILL and OOM, neither of which runs any cleanup code. A
//! lock file whose mere existence meant "taken" would instead need a liveness
//! check and would strand the directory after every hard kill.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};

/// Lock file kept in the output directory. The leading dot keeps it out of the
/// `*_<date>.gz` and `_meta_*` globs the converters and the operator use.
pub const LOCK_FILE: &str = ".collector.lock";

/// An exclusive claim on one output directory, held for as long as the value
/// lives.
///
/// Keep it alive for the whole run. Dropping it — or letting it fall out of
/// scope early — releases the directory to the next process while this one is
/// still writing to it.
#[derive(Debug)]
pub struct DirectoryLock {
    /// The claim itself. Closing this descriptor is what releases the lock, so
    /// the field is held rather than used.
    _file: File,
    path: PathBuf,
}

impl DirectoryLock {
    /// Where the claim is recorded, for logging.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Claims `dir` for this process, or fails naming whoever holds it.
///
/// The claim is recorded in the file as plain text so that `cat` answers the
/// question without any tooling.
pub fn acquire(dir: &str, instance: &str) -> Result<DirectoryLock, anyhow::Error> {
    let path = Path::new(dir).join(LOCK_FILE);
    // Opened read-write but NOT truncating: the file still holds the current
    // holder's record, which is exactly what a failed attempt has to report,
    // and truncation happens only after the lock is won.
    let mut file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("couldn't open the lock file {}", path.display()))?;

    // SAFETY: `file` owns a valid descriptor that outlives the call, and
    // `flock` neither retains it nor writes through any pointer. The return
    // value is checked before anything is read.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let error = std::io::Error::last_os_error();
        // EWOULDBLOCK is the answer that means "someone else has it"; anything
        // else (a filesystem with no lock support, EINTR) is a different fault
        // and must not be reported as a collision.
        if error.kind() != std::io::ErrorKind::WouldBlock {
            return Err(anyhow::Error::new(error).context(format!(
                "couldn't lock {} for exclusive use",
                path.display()
            )));
        }
        let mut holder = String::new();
        let holder = match file.read_to_string(&mut holder) {
            Ok(_) if !holder.trim().is_empty() => holder.trim().to_string(),
            // Either the holder died between creating the file and writing to
            // it, or it predates this record. Say so rather than print nothing.
            _ => "unknown (the lock file carries no holder record)".to_string(),
        };
        return Err(anyhow!(
            "{dir} is already being recorded into, by {holder}. Two collectors in one \
             directory interleave gzip members into the same file and make it undecodable, \
             so this one will not start. Give each instance its own data directory \
             (COLLECTOR_DATA_DIR), or stop the holder first."
        ));
    }

    // Won. Replace the previous holder's record with ours. `set_len` before the
    // write rather than a truncating open, so that a shorter record cannot
    // leave a tail of the old one behind.
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    // `File` is unbuffered, so this reaches the descriptor immediately and a
    // competing process reads a complete record; `flush` is kept for the
    // contract rather than for any buffer it drains.
    write!(
        file,
        "instance={instance} pid={} since={}",
        std::process::id(),
        chrono::Utc::now().to_rfc3339()
    )?;
    file.flush()?;

    // Deliberately never removed, not on drop and not on a clean exit. Unlink
    // races: a process that removed it on the way out could delete the file a
    // newly started collector had already opened and locked, after which a
    // third would create a fresh file and lock that instead — two holders, both
    // convinced they were alone. A stale file is harmless; an unlink race is
    // the corruption this module exists to prevent.
    Ok(DirectoryLock { _file: file, path })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU32, Ordering},
    };

    use super::*;

    static SEQ: AtomicU32 = AtomicU32::new(0);

    fn scratch(name: &str) -> String {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "collector-lock-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_string()
    }

    /// The reason the lock exists. Two collectors appending to one directory
    /// interleave gzip members into the same symbol file, and the result cannot
    /// be decoded — the data is not merely duplicated, it is destroyed. So the
    /// second one must be refused, not warned.
    ///
    /// `flock` is held per open file description, so two independent opens
    /// conflict even inside one process. That is what makes this testable
    /// without spawning a second binary.
    #[test]
    fn a_second_collector_on_the_same_directory_is_refused() {
        let dir = scratch("conflict");
        let held = acquire(&dir, "bybit").expect("the first collector must get the lock");

        let error = acquire(&dir, "hyperliquid")
            .expect_err("a second collector on the same directory must be refused")
            .to_string();

        // An operator seeing this in the journal has to be able to act on it,
        // which means knowing which instance to look at and which pid to kill.
        assert!(
            error.contains("bybit"),
            "the refusal must name the holding instance: {error}"
        );
        assert!(
            error.contains(&std::process::id().to_string()),
            "the refusal must carry the holder's pid: {error}"
        );

        drop(held);
    }

    /// Nothing outlives the process: a supervisor restart, a rollback, or a
    /// crash must not leave a directory permanently unusable. The kernel
    /// releases the lock when the descriptor closes, including on SIGKILL.
    #[test]
    fn releasing_the_lock_lets_the_next_process_in() {
        let dir = scratch("release");
        drop(acquire(&dir, "bybit").unwrap());
        acquire(&dir, "bybit").expect("the lock must not survive its holder");
    }

    /// One lock per directory, not per host. The intended deployment is one
    /// instance per venue, each with its own data directory.
    #[test]
    fn separate_directories_do_not_conflict() {
        let a = scratch("dir-a");
        let b = scratch("dir-b");
        let _first = acquire(&a, "bybit").unwrap();
        acquire(&b, "hyperliquid").expect("a different directory is a different lock");
    }

    /// The holder's record must survive a failed attempt. Opening the file for
    /// truncating writes before taking the lock would erase the one piece of
    /// evidence the refusal is supposed to report — and the loser would then
    /// blank the file for the next process to read too.
    #[test]
    fn a_refused_attempt_leaves_the_holder_record_intact() {
        let dir = scratch("intact");
        let _held = acquire(&dir, "bybit").unwrap();
        let _ = acquire(&dir, "hyperliquid");

        let content = fs::read_to_string(format!("{dir}/{LOCK_FILE}")).unwrap();
        assert!(
            content.contains("bybit") && !content.contains("hyperliquid"),
            "the loser overwrote the holder's record: {content:?}"
        );
    }
}
