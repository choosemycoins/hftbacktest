use std::{ffi::CString, io, path::Path};

/// Free space available to an unprivileged process, in bytes.
///
/// Uses `f_bavail`, not `f_bfree`: the difference is the reserved blocks only
/// root may use, and the collector does not run as root. Reporting `f_bfree`
/// would let it keep writing right up to a "disk full" error it had predicted
/// was still minutes away.
pub fn available_bytes(path: &str) -> io::Result<u64> {
    // statvfs resolves through to the mount, so a directory that does not exist
    // yet would report the parent's filesystem — check explicitly instead of
    // silently measuring the wrong thing.
    if !Path::new(path).is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{path} is not a directory"),
        ));
    }
    let c_path = CString::new(path)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    // SAFETY: `c_path` is a valid NUL-terminated string that outlives the call,
    // and `stat` is a properly sized, aligned buffer that libc fully
    // initialises on success. The return value is checked before it is read.
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };

    // `f_frsize` is the fragment size that block counts are expressed in;
    // `f_bsize` is the preferred I/O size and is the wrong multiplier, though
    // the two are equal on most modern filesystems.
    // The widths of these fields differ by target — `f_frsize` is u64 on macOS
    // but u32 on several Linux targets — so the conversion is redundant on some
    // platforms and required on others. Widening explicitly keeps the code
    // correct everywhere; the lint only fires where it happens to be a no-op.
    #[allow(clippy::useless_conversion)]
    let blocks = u64::from(stat.f_bavail);
    #[allow(clippy::useless_conversion)]
    let frag = u64::from(stat.f_frsize);
    Ok(blocks.saturating_mul(frag))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_a_plausible_figure_for_an_existing_directory() {
        let free = available_bytes(std::env::temp_dir().to_str().unwrap()).unwrap();
        assert!(free > 0, "temp dir reported zero bytes free");
        // Sanity bound: catches a unit mix-up (blocks reported as bytes, or
        // f_bsize/f_frsize confusion) that a "> 0" assertion would pass.
        assert!(
            free < 1 << 50,
            "implausible free space {free} bytes — wrong multiplier?"
        );
    }

    #[test]
    fn missing_directory_is_an_error_not_a_wrong_answer() {
        let err = available_bytes("/definitely/not/a/real/path/xyz").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn a_file_is_not_accepted_as_a_directory() {
        let f = std::env::temp_dir().join(format!("collector-disk-test-{}", std::process::id()));
        std::fs::write(&f, b"x").unwrap();
        let err = available_bytes(f.to_str().unwrap()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        let _ = std::fs::remove_file(&f);
    }
}
