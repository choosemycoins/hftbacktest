use std::{
    collections::{HashMap, hash_map::Entry},
    fs::File,
    io,
    io::Write,
};

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{Compression, write::GzEncoder};
use tracing::{error, info};

pub struct RotatingFile {
    date: NaiveDate,
    path: String,
    file: Option<GzEncoder<File>>,
}

impl RotatingFile {
    fn create(datetime: DateTime<Utc>, path: &str) -> Result<GzEncoder<File>, io::Error> {
        let date = datetime.date_naive().format("%Y%m%d");
        // `append`, not plain `write`: the collector reopens today's file on
        // every restart (deploy, rollback, supervisor recycle, crash). Opening
        // for truncating writes without `truncate(true)` wrote a fresh gzip
        // member over the start of the existing one, leaving a file that
        // decoders reject with UnexpectedEof — the day's data was not merely
        // lost but unrecoverable. Appending starts a new gzip member instead;
        // the result is a multi-member gzip, which `gunzip`/`zcat` and
        // Python's `gzip` read transparently. Rust readers must use
        // `flate2::read::MultiGzDecoder`, not `GzDecoder`.
        let file = File::options()
            .create(true)
            .append(true)
            .open(format!("{path}_{date}.gz"))?;
        Ok(GzEncoder::new(file, Compression::default()))
    }

    pub fn new(datetime: DateTime<Utc>, path: String) -> Result<Self, io::Error> {
        Ok(Self {
            date: datetime.date_naive(),
            file: Some(Self::create(datetime, &path)?),
            path,
        })
    }

    pub fn write(&mut self, datetime: DateTime<Utc>, data: String) -> Result<(), io::Error> {
        let date = datetime.date_naive();
        if date != self.date {
            // Open the new day's file BEFORE finishing the old one. If `create`
            // fails (disk full, data dir remounted read-only) the old encoder is
            // still installed, so this object stays in a valid state and the
            // error propagates without stranding `self.file` as `None`.
            let next = Self::create(datetime, &self.path)?;
            if let Some(file) = self.file.replace(next)
                && let Err(error) = file.finish()
            {
                error!(?error, %self.path, "couldn't finish the previous day's file");
            }
            self.date = date;
            info!(%date, %self.path, "date is changed");
        }
        let timestamp = datetime.timestamp_nanos_opt().unwrap();
        self.file
            .as_mut()
            .unwrap()
            .write_all(format!("{timestamp} {data}\n").as_bytes())
    }
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        // Must be total. `Writer` holds these in a `HashMap`, and a panic in one
        // element's drop glue abandons the drop of every remaining element — so
        // one file's failure would leave every other symbol's gzip stream
        // unterminated and undecodable, which is precisely what the append-mode
        // fix above exists to prevent.
        if let Some(file) = self.file.take()
            && let Err(error) = file.finish()
        {
            error!(?error, %self.path, "couldn't finish the gzip stream; file may be truncated");
        }
    }
}

#[cfg(test)]
mod rotating_file_tests {
    use std::{
        fs,
        io::Read,
        sync::atomic::{AtomicU32, Ordering},
    };

    use chrono::TimeZone;
    use flate2::read::MultiGzDecoder;

    use super::*;

    static SEQ: AtomicU32 = AtomicU32::new(0);

    /// Unique scratch directory per test, no external crate needed.
    fn scratch(name: &str) -> String {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "collector-file-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_string()
    }

    fn read_all(path: &str) -> String {
        let bytes = fs::read(path).unwrap();
        let mut out = String::new();
        // `MultiGzDecoder`, not `GzDecoder`: a file reopened in append mode
        // holds one gzip member per collector session. Plain `GzDecoder`
        // stops after the first member and would silently return only the
        // pre-restart data.
        MultiGzDecoder::new(&bytes[..])
            .read_to_string(&mut out)
            .unwrap();
        out
    }

    /// Restarting the collector on the same UTC day must not destroy the data
    /// already recorded that day. Deploy, rollback and any supervisor restart
    /// all reopen the same `{symbol}_{date}.gz`, so opening it for truncating
    /// writes loses everything recorded before the restart.
    #[test]
    fn reopening_same_day_appends_instead_of_overwriting() {
        let dir = scratch("append");
        let path = format!("{dir}/btcusdt");
        let t = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();

        {
            let mut f = RotatingFile::new(t, path.clone()).unwrap();
            f.write(t, "before-restart".to_string()).unwrap();
        }
        {
            let mut f = RotatingFile::new(t, path.clone()).unwrap();
            f.write(t, "after-restart".to_string()).unwrap();
        }

        let content = read_all(&format!("{path}_20260725.gz"));
        assert!(
            content.contains("before-restart"),
            "pre-restart data was lost; file contains: {content:?}"
        );
        assert!(
            content.contains("after-restart"),
            "post-restart data missing; file contains: {content:?}"
        );
    }

    /// A UTC date change must start a new file and leave the previous day's
    /// file complete and readable.
    #[test]
    fn date_change_rotates_to_a_new_file() {
        let dir = scratch("rotate");
        let path = format!("{dir}/ethusdt");
        let day1 = Utc.with_ymd_and_hms(2026, 7, 25, 23, 59, 59).unwrap();
        let day2 = Utc.with_ymd_and_hms(2026, 7, 26, 0, 0, 1).unwrap();

        {
            let mut f = RotatingFile::new(day1, path.clone()).unwrap();
            f.write(day1, "day-one".to_string()).unwrap();
            f.write(day2, "day-two".to_string()).unwrap();
        }

        assert!(read_all(&format!("{path}_20260725.gz")).contains("day-one"));
        let d2 = read_all(&format!("{path}_20260726.gz"));
        assert!(d2.contains("day-two"));
        assert!(!d2.contains("day-one"), "day 1 data leaked into day 2 file");
    }

    /// Each line is `{recv_timestamp_nanos} {raw_payload}`. The data pipeline
    /// (`py-hftbacktest/hftbacktest/data/utils/*`) splits on the first space,
    /// so the prefix format is a contract, not an implementation detail.
    #[test]
    fn line_format_is_nanos_space_payload() {
        let dir = scratch("format");
        let path = format!("{dir}/solusdt");
        let t = Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap();

        {
            let mut f = RotatingFile::new(t, path.clone()).unwrap();
            f.write(t, r#"{"a":1}"#.to_string()).unwrap();
        }

        let content = read_all(&format!("{path}_20260725.gz"));
        let line = content.lines().next().unwrap();
        let (ts, payload) = line.split_once(' ').unwrap();
        assert_eq!(ts.parse::<i64>().unwrap(), t.timestamp_nanos_opt().unwrap());
        assert_eq!(payload, r#"{"a":1}"#);
    }
}

pub struct Writer {
    path: String,
    file: HashMap<String, RotatingFile>,
}

impl Writer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            file: Default::default(),
        }
    }

    pub fn write(
        &mut self,
        recv_time: DateTime<Utc>,
        symbol: String,
        data: String,
    ) -> Result<(), anyhow::Error> {
        match self.file.entry(symbol.to_lowercase()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().write(recv_time, data)?;
            }
            Entry::Vacant(entry) => {
                let symbol = entry.key().clone();
                let path = self.path.as_str();
                entry
                    .insert(RotatingFile::new(recv_time, format!("{path}/{symbol}"))?)
                    .write(recv_time, data)?;
            }
        }
        Ok(())
    }
}
