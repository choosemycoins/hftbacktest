use std::{
    collections::{HashMap, hash_map::Entry},
    fs::File,
    io,
    io::Write,
};

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{Compression, write::GzEncoder};
use tracing::{error, info};

/// How a stream is stored.
///
/// Market data is gzipped: it is large, written continuously, and only ever
/// read after the fact. The meta stream is not, because its whole purpose is to
/// be readable *while the collector runs* — and gzip cannot deliver that.
/// `GzEncoder::flush()` emits a deflate sync point but no member trailer, so a
/// standard reader still rejects the file with `UnexpectedEof`; the data only
/// becomes decodable when the member is finished at shutdown. Measured over a
/// 12-minute run, the meta stream stayed at 10 bytes on disk throughout and
/// materialised only on exit — useless for diagnosing a problem in progress,
/// and lost entirely to a SIGKILL. At ~90 KB/day it does not need compressing.
///
/// The plain stream also uses a different extension, which keeps it out of the
/// `*_<date>.gz` wildcards that feed the data converters.
#[derive(Clone, Copy, PartialEq)]
pub enum Encoding {
    Gzip,
    /// Written uncompressed and flushed after every record.
    PlainFlushed,
}

impl Encoding {
    fn extension(self) -> &'static str {
        match self {
            Encoding::Gzip => "gz",
            Encoding::PlainFlushed => "jsonl",
        }
    }
}

enum Sink {
    Gz(Box<GzEncoder<File>>),
    Plain(File),
}

impl Sink {
    fn write_all(&mut self, buf: &[u8]) -> Result<(), io::Error> {
        match self {
            Sink::Gz(f) => f.write_all(buf),
            Sink::Plain(f) => f.write_all(buf),
        }
    }

    /// Closes the stream. For gzip this writes the member trailer, without
    /// which the file cannot be decoded.
    fn finish(self) -> Result<(), io::Error> {
        match self {
            Sink::Gz(f) => f.finish().map(|_| ()),
            Sink::Plain(mut f) => f.flush(),
        }
    }
}

pub struct RotatingFile {
    date: NaiveDate,
    path: String,
    encoding: Encoding,
    file: Option<Sink>,
}

impl RotatingFile {
    fn create(datetime: DateTime<Utc>, path: &str, encoding: Encoding) -> Result<Sink, io::Error> {
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
            .open(format!("{path}_{date}.{}", encoding.extension()))?;
        Ok(match encoding {
            Encoding::Gzip => Sink::Gz(Box::new(GzEncoder::new(file, Compression::default()))),
            Encoding::PlainFlushed => Sink::Plain(file),
        })
    }

    pub fn new(
        datetime: DateTime<Utc>,
        path: String,
        encoding: Encoding,
    ) -> Result<Self, io::Error> {
        Ok(Self {
            date: datetime.date_naive(),
            file: Some(Self::create(datetime, &path, encoding)?),
            encoding,
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
            let next = Self::create(datetime, &self.path, self.encoding)?;
            if let Some(file) = self.file.replace(next)
                && let Err(error) = file.finish()
            {
                error!(?error, %self.path, "couldn't finish the previous day's file");
            }
            self.date = date;
            info!(%date, %self.path, "date is changed");
        }
        let timestamp = datetime.timestamp_nanos_opt().unwrap();
        // The writer owns the record separator: exactly one `\n` per record,
        // appended below. A payload that already carries a terminator writes a
        // blank line into the file — silently, with a plausible-looking file
        // as the only evidence (the Paradex fault: its venue newline-terminates
        // WS frames, trimmed at `paradex/mod.rs`). Enforced here, at the owner
        // of the separator, so any future backend that forwards a terminated
        // frame reds its tests instead of recording damaged days. Debug-only:
        // free in release, and the offline quality gate remains the backstop.
        debug_assert!(
            !data.contains('\n'),
            "a record must not carry a line terminator; the writer appends the \
             one separator itself (payload: {data:?})"
        );
        let file = self.file.as_mut().unwrap();
        file.write_all(format!("{timestamp} {data}\n").as_bytes())?;

        // A plain stream is flushed per record — that is the reason it is not
        // compressed. Gzip streams are left to buffer; forcing a sync point on
        // them costs compression on the high-volume path and still would not
        // make them decodable, since the member trailer is only written on
        // close.
        if self.encoding == Encoding::PlainFlushed
            && let Sink::Plain(f) = file
        {
            f.flush()?;
        }
        Ok(())
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

    /// The writer owns the record separator: `write` appends exactly one `\n`
    /// per record, so a payload that already carries a line terminator would
    /// put a blank line into the file — the Paradex fault (`paradex/mod.rs`
    /// trims at its send site). This pin enforces the invariant at the
    /// component that owns the separator: the NEXT backend that forwards a
    /// terminated frame fails its tests here instead of writing
    /// plausible-looking damaged files. `debug_assert`, so it is free in
    /// release and the hot path is untouched.
    #[test]
    #[should_panic(expected = "line terminator")]
    fn a_payload_carrying_a_line_terminator_is_refused_in_debug() {
        let dir = scratch("terminator");
        let path = format!("{dir}/btcusdt");
        let t = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();
        let mut f = RotatingFile::new(t, path, Encoding::Gzip).unwrap();
        f.write(t, "{\"px\":1}\n".to_string()).unwrap();
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
            let mut f = RotatingFile::new(t, path.clone(), Encoding::Gzip).unwrap();
            f.write(t, "before-restart".to_string()).unwrap();
        }
        {
            let mut f = RotatingFile::new(t, path.clone(), Encoding::Gzip).unwrap();
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
            let mut f = RotatingFile::new(day1, path.clone(), Encoding::Gzip).unwrap();
            f.write(day1, "day-one".to_string()).unwrap();
            f.write(day2, "day-two".to_string()).unwrap();
        }

        assert!(read_all(&format!("{path}_20260725.gz")).contains("day-one"));
        let d2 = read_all(&format!("{path}_20260726.gz"));
        assert!(d2.contains("day-two"));
        assert!(!d2.contains("day-one"), "day 1 data leaked into day 2 file");
    }

    /// The meta stream must be readable while the collector is still running.
    /// Without a flush the gzip encoder holds records until ~48 KB accumulate,
    /// which for a stream of a few dozen lines per session means they only
    /// reach disk at shutdown — and not at all after a SIGKILL.
    #[test]
    fn meta_records_are_readable_before_shutdown() {
        let dir = scratch("metaflush");
        let t = Utc.with_ymd_and_hms(2026, 7, 25, 9, 0, 0).unwrap();
        let mut w = Writer::new(&dir, "hyperliquid");

        w.write(
            t,
            META_STREAM.to_string(),
            r#"{"_collector":"disconnected"}"#.to_string(),
        )
        .unwrap();

        // Deliberately not dropping `w`: this is the live-process case.
        // The meta stream is plain `.jsonl`, so it is readable immediately.
        // A gzip stream could not be: `flush()` emits a deflate sync point but
        // no member trailer, so a decoder still fails with UnexpectedEof.
        let content =
            fs::read_to_string(format!("{dir}/_meta_hyperliquid_20260725.jsonl")).unwrap();
        assert!(
            content.contains("disconnected"),
            "meta record not readable while the collector is running: {content:?}"
        );
        assert!(
            !std::path::Path::new(&format!("{dir}/_meta_hyperliquid_20260725.gz")).exists(),
            "meta must not be gzipped — a *.gz wildcard would feed it to the converters"
        );
    }

    /// Symbol streams are deliberately NOT flushed per record — that would cost
    /// compression on the high-volume path. They are complete after a clean
    /// shutdown, which is what the signal handling exists to guarantee.
    #[test]
    fn symbol_records_are_complete_after_shutdown() {
        let dir = scratch("symflush");
        let t = Utc.with_ymd_and_hms(2026, 7, 25, 9, 0, 0).unwrap();
        {
            let mut w = Writer::new(&dir, "hyperliquid");
            w.write(t, "BTC".to_string(), r#"{"px":"1"}"#.to_string())
                .unwrap();
        }
        assert!(read_all(&format!("{dir}/btc_20260725.gz")).contains("px"));
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
            let mut f = RotatingFile::new(t, path.clone(), Encoding::Gzip).unwrap();
            f.write(t, r#"{"a":1}"#.to_string()).unwrap();
        }

        let content = read_all(&format!("{path}_20260725.gz"));
        let line = content.lines().next().unwrap();
        let (ts, payload) = line.split_once(' ').unwrap();
        assert_eq!(ts.parse::<i64>().unwrap(), t.timestamp_nanos_opt().unwrap());
        assert_eq!(payload, r#"{"a":1}"#);
    }
}

/// Reserved stream name for records that belong to no symbol.
///
/// Everything the collector observes has to land somewhere, but `Writer` files
/// by symbol and connection-level frames — subscription acks, venue errors,
/// disconnects — carry none. They go here instead of being dropped, which is
/// what makes a recording self-describing: a later consumer can tell what was
/// subscribed with which parameters, and where the gaps came from, instead of
/// inferring it from what happens to be present.
///
/// No venue uses a leading underscore in a symbol, so this cannot collide.
pub const META_STREAM: &str = "_meta";

pub struct Writer {
    path: String,
    meta_stream: String,
    file: HashMap<String, RotatingFile>,
}

impl Writer {
    /// `instance` names the sidecar after what produced it —
    /// `_meta_<instance>_<date>.jsonl`.
    ///
    /// It is **not** what makes an output directory shareable: nothing is, and
    /// two collectors must never record into one. They would append to the same
    /// `<symbol>_<date>.gz` — Bybit `BTCUSDT` and Binance `btcusdt` are one
    /// filename once lowercased — and interleave two independent gzip streams
    /// into a file no decoder can read. `lock.rs` refuses the second process
    /// outright, so this suffix earns its keep afterwards instead: when days
    /// from several instances are gathered into one directory for conversion,
    /// the sidecars still do not overwrite each other.
    pub fn new(path: &str, instance: &str) -> Self {
        Self {
            path: path.to_string(),
            meta_stream: format!("{META_STREAM}_{instance}"),
            file: Default::default(),
        }
    }

    pub fn write(
        &mut self,
        recv_time: DateTime<Utc>,
        symbol: String,
        data: String,
    ) -> Result<(), anyhow::Error> {
        let is_meta = symbol == META_STREAM;
        let (symbol, encoding) = if is_meta {
            (self.meta_stream.clone(), Encoding::PlainFlushed)
        } else {
            (symbol, Encoding::Gzip)
        };
        match self.file.entry(symbol.to_lowercase()) {
            Entry::Occupied(mut entry) => entry.get_mut().write(recv_time, data)?,
            Entry::Vacant(entry) => {
                let symbol = entry.key().clone();
                let path = self.path.as_str();
                entry
                    .insert(RotatingFile::new(
                        recv_time,
                        format!("{path}/{symbol}"),
                        encoding,
                    )?)
                    .write(recv_time, data)?
            }
        }
        Ok(())
    }
}
