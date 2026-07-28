//! Whether the host clock was disciplined while the recording was made.
//!
//! Every line in every file is stamped with `Utc::now()` at receive time, and
//! everything downstream is built on the assumption that those stamps mean
//! something. Nothing checked that assumption. On 2026-07-27 a host came back
//! from a reboot with an undisciplined clock, recorded a full day, and the
//! offline time policy rejected the lot at assembly time on a local-exchange
//! skew of -7.04 ms — the first anyone heard of it, a day after the only moment
//! it could have been fixed.
//!
//! So the clock is sampled once a minute into `_meta`, exactly as free disk
//! space is. That buys two things a log line would not:
//!
//! * **Live.** The sidecar is the one file an operator can tail while the
//!   collector runs (`file.rs`, `Encoding::PlainFlushed`), so an unsynchronised
//!   clock is visible in the minute it happens rather than at assembly.
//! * **Provable afterwards.** A dataset manifest that spans a window can state
//!   what the clock was doing across it, from the recording's own bytes,
//!   instead of postulating that it was fine.
//!
//! ## Why `adjtimex(2)` and not `chronyc tracking`
//!
//! Three reasons, in order of how much they matter.
//!
//! * It reports the **kernel's** discipline of `CLOCK_REALTIME` — the very
//!   clock `Utc::now()` reads. `chronyc tracking` reports chrony's model of the
//!   offset it is about to apply, which is one step removed from what actually
//!   stamped the line, and a chrony that is running happily while failing to
//!   discipline the kernel is precisely the reboot case.
//! * It is one syscall with no privileges, no socket and **no subprocess**. A
//!   `chronyc` fork every minute is a process spawn on a box whose whole job is
//!   to not fall behind, and it fails on any host that runs
//!   `systemd-timesyncd` instead — which also disciplines the kernel, and which
//!   this reads identically.
//! * `STA_UNSYNC` is a fact the kernel sets, not a threshold anyone has to
//!   choose. It is set from boot until something disciplines the clock, and set
//!   again when `maxerror` saturates, so the failure that started this is
//!   reported by a flag rather than inferred from a number.
//!
//! ## What is a warning and what is only evidence
//!
//! The alarm is deliberately coarse: [`Alarm`] is raised on `STA_UNSYNC` or on
//! `maxerror` past [`MAX_ERROR_WARN_US`], and on nothing else. It is **not** how
//! a 7 ms skew is found — no threshold that catches 7 ms could avoid firing on
//! a healthy host, and an alarm that fires on healthy hosts is one nobody reads.
//! The five recorded numbers are how it is found, after the fact and exactly.
//! The alarm's job is the other failure: a clock nobody is disciplining at all.
//!
//! Non-Linux hosts have no `adjtimex`, so they record [`Sample::Unsupported`]
//! rather than a healthy-looking reading. A dev run on macOS must not be able
//! to produce a recording that claims a disciplined clock.

use serde_json::Value;

/// `STA_UNSYNC` — "clock unsynchronized".
///
/// Spelled out rather than taken from `libc`, because the reading of a `timex`
/// is where the units and the flags live, and that is what needs testing on a
/// machine with no `adjtimex` to test against. The values are kernel ABI and
/// identical on every Linux target (`uapi/linux/timex.h`).
const STA_UNSYNC: i32 = 0x0040;

/// `STA_NANO` — `offset` is in nanoseconds rather than microseconds.
///
/// Load-bearing, not a detail: chrony and `systemd-timesyncd` both put the
/// kernel in nanosecond mode, so reading `offset` as microseconds unconditionally
/// would report a healthy 7 µs host as 7 ms out, and the 7 ms one as 7 s.
const STA_NANO: i32 = 0x2000;

/// `TIME_ERROR` — what `adjtimex` returns while the clock is unsynchronised.
///
/// Checked as well as `STA_UNSYNC` and not instead of it: the kernel also
/// returns it for `STA_CLOCKERR`, so the two together are "the kernel does not
/// stand behind this clock" whichever way it came about.
const TIME_ERROR: i32 = 5;

/// How large `maxerror` may get before the clock is called undisciplined, in
/// microseconds.
///
/// The kernel grows `maxerror` by 500 µs per second between updates
/// (`MAXFREQ / NSEC_PER_USEC`, in `second_overflow()`) and every time daemon
/// resets it on a successful poll. So the widest *legitimate* excursion on a
/// healthy host is `maxpoll × 500 µs` above whatever the last reset left
/// behind, and it differs by daemon — both of which this gauge reads
/// identically and therefore has to hold:
///
/// * chrony, default `maxpoll` 10 → 1024 s → **~512 ms**;
/// * `systemd-timesyncd`, `NTP_POLL_INTERVAL_MAX_USEC` = 2048 s with
///   `maxerror` set to 0 on each sync → **~1.024 s**.
///
/// Four seconds is a quarter of the kernel's own saturation at 16 s
/// (`NTP_PHASE_LIMIT`), where it gives up and sets `STA_UNSYNC` unaided about
/// nine hours after the last successful update. It therefore reports four
/// times sooner than the kernel does, while leaving room for three consecutive
/// missed `timesyncd` polls — a transient a healthy host really does have, and
/// one this alarm is not for.
///
/// It is chosen so that a false alarm is not possible rather than so that a
/// small skew is caught; see the module docs on why those are different jobs.
/// An earlier 1 s was derived from chrony's `maxpoll` alone and fired and
/// cleared once per poll cycle — roughly forty times a day — on a stock
/// `systemd-timesyncd` host with nothing wrong with it.
pub const MAX_ERROR_WARN_US: i64 = 4_000_000;

/// The raw fields of `struct timex`, before anything is made of them.
///
/// A plain struct rather than `libc::timex` so that the interpretation — which
/// is where the units are, and where a mistake is silent — is testable on a
/// machine that has no `adjtimex` at all. The syscall's only job is to fill
/// this in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timex {
    /// `adjtimex`'s return value: `TIME_OK`..`TIME_ERROR`.
    pub ret: i32,
    /// `STA_*` status flags.
    pub status: i32,
    /// Remaining offset the kernel is slewing in — µs, or ns under `STA_NANO`.
    pub offset: i64,
    /// Frequency offset in ppm, scaled by 2^16.
    pub freq: i64,
    /// Maximum error, µs.
    pub maxerror: i64,
    /// Estimated error, µs.
    pub esterror: i64,
}

/// The kernel's view of how well `CLOCK_REALTIME` is disciplined.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Discipline {
    /// False when the kernel does not stand behind this clock.
    pub synchronized: bool,
    pub est_error_us: i64,
    pub max_error_us: i64,
    /// Correction still being slewed in, in microseconds — fractional, because
    /// the kernel reports it in nanoseconds whenever `STA_NANO` is set.
    pub offset_us: f64,
    /// Frequency correction being applied, in parts per million.
    pub freq_ppm: f64,
}

impl Discipline {
    /// Reads a `timex` in the units it was actually filled in with.
    pub fn from_timex(raw: Timex) -> Self {
        let nano = raw.status & STA_NANO != 0;
        Self {
            synchronized: raw.ret != TIME_ERROR && raw.status & STA_UNSYNC == 0,
            est_error_us: raw.esterror,
            max_error_us: raw.maxerror,
            offset_us: if nano {
                raw.offset as f64 / 1000.0
            } else {
                raw.offset as f64
            },
            // ppm scaled by 2^16 — the kernel's fixed point, not a unit anyone
            // reading the sidecar should have to know about.
            freq_ppm: raw.freq as f64 / 65536.0,
        }
    }

    /// Why this clock should not be trusted, or `None` if it should.
    fn fault(&self) -> Option<String> {
        if !self.synchronized {
            return Some(format!(
                "the kernel reports CLOCK_REALTIME unsynchronised (STA_UNSYNC); \
                 max_error {} us, offset {:.1} us",
                self.max_error_us, self.offset_us
            ));
        }
        if self.max_error_us > MAX_ERROR_WARN_US {
            return Some(format!(
                "max_error has grown to {} us, past the {MAX_ERROR_WARN_US} us limit; \
                 nothing has disciplined this clock for a long time",
                self.max_error_us
            ));
        }
        None
    }
}

/// One minute's answer, including the two ways there isn't one.
#[derive(Clone, Debug, PartialEq)]
pub enum Sample {
    /// The kernel answered.
    Kernel(Discipline),
    /// This platform has no `adjtimex`. Nothing is claimed either way — which
    /// is the point: a dev run must not record a healthy-looking clock it never
    /// measured.
    Unsupported,
    /// Linux, and the call failed. Deliberately not folded into `Unsupported`:
    /// "there is no such syscall here" and "the syscall did not work" send an
    /// investigation to opposite places.
    Unavailable(String),
}

/// `{"_collector":"clock", …}` — the sidecar record for one sample.
pub fn record(sample: &Sample) -> Value {
    let mut fields = serde_json::Map::new();
    fields.insert(crate::meta::TAG.to_string(), Value::from("clock"));
    match sample {
        Sample::Kernel(d) => {
            fields.insert("sync".to_string(), Value::from(d.synchronized));
            fields.insert("est_error_us".to_string(), Value::from(d.est_error_us));
            fields.insert("max_error_us".to_string(), Value::from(d.max_error_us));
            fields.insert("offset_us".to_string(), Value::from(d.offset_us));
            fields.insert("freq_ppm".to_string(), Value::from(d.freq_ppm));
        }
        Sample::Unsupported => {
            fields.insert("unsupported".to_string(), Value::from(true));
            fields.insert(
                "platform".to_string(),
                Value::from(std::env::consts::OS.to_string()),
            );
        }
        Sample::Unavailable(error) => {
            fields.insert("error".to_string(), Value::from(error.clone()));
        }
    }
    Value::Object(fields)
}

/// Asks the kernel how well it is disciplining `CLOCK_REALTIME`.
///
/// One function on every platform, with the `#[cfg]` confined to
/// [`read_timex`]. Writing two `sample`s instead — the obvious shape — makes
/// half of this module unreachable on whichever target is being built, and
/// `dead_code` then fires on the units, the flags and the reader: the exact
/// code that has to be right on the deployed target and is tested on every
/// other one. Suppressing that with `allow(dead_code)` would also suppress the
/// next thing that really did become dead.
pub fn sample() -> Sample {
    match read_timex() {
        Ok(Some(raw)) => Sample::Kernel(Discipline::from_timex(raw)),
        Ok(None) => Sample::Unsupported,
        Err(error) => Sample::Unavailable(error),
    }
}

/// Fills in a `timex` from the kernel; `None` where there is no `adjtimex`.
///
/// `modes = 0` makes this a read-only query, which needs no privileges.
/// `adjtimex` rather than glibc's `ntp_adjtime`, because musl provides only the
/// former and which libc the deployed binary links is not this module's
/// business.
#[cfg(target_os = "linux")]
fn read_timex() -> Result<Option<Timex>, String> {
    // SAFETY: `timex` is a C struct of integers and a `timeval`, for which an
    // all-zero bit pattern is a valid value and is exactly what a read-only
    // query requires (`modes` must be 0, or the call would try to *set*
    // something). The pointer is to a live, correctly aligned local that
    // outlives the call, and the return value is checked before any field is
    // read.
    let mut raw: libc::timex = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::adjtimex(&mut raw) };
    if ret < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(Some(Timex {
        ret,
        status: raw.status,
        // Widened explicitly: these are `c_long` on most Linux targets and
        // `i64` on x32, so the conversion is a no-op on some and required on
        // others.
        #[allow(clippy::useless_conversion)]
        offset: i64::from(raw.offset),
        #[allow(clippy::useless_conversion)]
        freq: i64::from(raw.freq),
        #[allow(clippy::useless_conversion)]
        maxerror: i64::from(raw.maxerror),
        #[allow(clippy::useless_conversion)]
        esterror: i64::from(raw.esterror),
    }))
}

#[cfg(not(target_os = "linux"))]
fn read_timex() -> Result<Option<Timex>, String> {
    Ok(None)
}

/// A change of state worth a log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Alarm {
    /// The clock stopped being trustworthy, and why.
    Raised(String),
    /// It recovered.
    Cleared,
}

/// Samples the clock on the minutely tick, and reports only the **edges**.
///
/// Edge-triggered because the failure lasts: a host that comes back from a
/// reboot without a time source stays unsynchronised for as long as it takes
/// someone to notice, and a warning per minute for hours is a journal nobody
/// reads. The state itself is in every minute's record; the log carries the two
/// transitions.
#[derive(Default)]
pub struct ClockGauge {
    alarmed: bool,
}

impl ClockGauge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flips the alarm if this sample changed it.
    ///
    /// [`Sample::Unsupported`] and [`Sample::Unavailable`] leave it exactly as
    /// it was. Neither is evidence that the clock recovered, and clearing a
    /// raised alarm on "we could not look" is how a fault comes to be reported
    /// as fixed.
    pub fn observe(&mut self, sample: &Sample) -> Option<Alarm> {
        let Sample::Kernel(discipline) = sample else {
            return None;
        };
        match (discipline.fault(), self.alarmed) {
            (Some(reason), false) => {
                self.alarmed = true;
                Some(Alarm::Raised(reason))
            }
            (None, true) => {
                self.alarmed = false;
                Some(Alarm::Cleared)
            }
            _ => None,
        }
    }

    /// One minute's work: the record for `_meta`, and the edge if there was one.
    pub fn tick(&mut self) -> (Value, Option<Alarm>) {
        let sample = sample();
        let alarm = self.observe(&sample);
        (record(&sample), alarm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta;

    /// A disciplined host: chrony has the kernel in nanosecond mode, the
    /// remaining slew is microseconds, and `maxerror` is a few milliseconds.
    fn healthy() -> Timex {
        Timex {
            ret: 0,
            status: STA_NANO,
            offset: -12_000,
            freq: 65536 * 13,
            maxerror: 16_000,
            esterror: 400,
        }
    }

    /// The incident this exists for. A host comes back from a reboot with
    /// nothing disciplining the clock; the kernel says so from the first
    /// sample, and the recording carries that in the minute it happened rather
    /// than leaving assembly to discover it a day later.
    #[test]
    fn the_reboot_case_is_visible_in_the_first_sample() {
        let after_reboot = Timex {
            ret: TIME_ERROR,
            status: STA_UNSYNC,
            offset: 0,
            freq: 0,
            maxerror: 16_000_000,
            esterror: 16_000_000,
        };

        let d = Discipline::from_timex(after_reboot);
        assert!(!d.synchronized);

        let r = record(&Sample::Kernel(d));
        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "clock");
        assert_eq!(r["sync"], false);
        assert_eq!(r["max_error_us"], 16_000_000i64);
    }

    /// `STA_UNSYNC` is the flag, but the kernel also refuses to stand behind a
    /// clock through its return value alone (`STA_CLOCKERR` produces exactly
    /// that). Reading only the status bits would call such a host healthy.
    #[test]
    fn a_kernel_that_returns_time_error_is_not_synchronised() {
        let d = Discipline::from_timex(Timex {
            ret: TIME_ERROR,
            ..healthy()
        });
        assert!(!d.synchronized);
    }

    /// The units are the whole risk in reading a `timex`, and getting them
    /// wrong is silent: chrony puts the kernel in nanosecond mode, so a reader
    /// that assumes microseconds is out by a thousand — which turns the 7.04 ms
    /// skew that started this into a plausible-looking 7 µs.
    #[test]
    fn the_offset_is_reported_in_microseconds_in_both_kernel_modes() {
        let nanos = Discipline::from_timex(Timex {
            status: STA_NANO,
            offset: -7_040_000,
            ..healthy()
        });
        assert!(
            (nanos.offset_us - -7040.0).abs() < 1e-6,
            "nanosecond mode read as {} us",
            nanos.offset_us
        );

        let micros = Discipline::from_timex(Timex {
            status: 0,
            offset: -7_040,
            ..healthy()
        });
        assert!(
            (micros.offset_us - -7040.0).abs() < 1e-6,
            "microsecond mode read as {} us",
            micros.offset_us
        );
    }

    /// `freq` is ppm in the kernel's 2^16 fixed point. Recording it raw would
    /// put a number nobody can read without the kernel source into a file whose
    /// point is to be machine-read.
    #[test]
    fn the_frequency_is_unscaled_from_the_kernels_fixed_point() {
        let d = Discipline::from_timex(Timex {
            freq: 65536 * -13,
            ..healthy()
        });
        assert!(
            (d.freq_ppm - -13.0).abs() < 1e-9,
            "freq read as {} ppm",
            d.freq_ppm
        );
    }

    /// A clock nobody has disciplined for hours is a fault before the kernel
    /// admits it: `maxerror` grows 500 us/s and only saturates into
    /// `STA_UNSYNC` after about nine hours. The threshold is what makes those
    /// nine hours reportable.
    #[test]
    fn a_growing_max_error_is_reported_before_the_kernel_gives_up() {
        let mut gauge = ClockGauge::new();

        let drifting = Sample::Kernel(Discipline::from_timex(Timex {
            maxerror: MAX_ERROR_WARN_US + 1,
            ..healthy()
        }));
        assert!(
            matches!(gauge.observe(&drifting), Some(Alarm::Raised(_))),
            "a maxerror past the limit raised nothing"
        );
    }

    /// The margin under the threshold has to hold a healthy host under **either**
    /// time daemon this gauge claims to read, because a warning that is always
    /// there is a warning nobody reads.
    ///
    /// The kernel grows `maxerror` at 500 µs/s and both daemons reset it on
    /// every successful poll, so the widest legitimate excursion is
    /// `maxpoll × 500 µs` plus whatever the daemon left behind:
    ///
    /// * chrony, default `maxpoll` 10 → 1024 s → ~512 ms;
    /// * `systemd-timesyncd`, `NTP_POLL_INTERVAL_MAX_USEC` = 2048 s and
    ///   `maxerror` set to 0 on each sync → **1.024 s**.
    ///
    /// The second is the one that matters: it is over a second, on a stock
    /// install, on a clock that is perfectly disciplined. A 1 s threshold fired
    /// and cleared once per poll cycle — about forty times a day — on exactly
    /// the hosts the module docs say are supported.
    #[test]
    fn neither_supported_time_daemon_warns_at_its_widest_poll() {
        for (daemon, maxpoll_s) in [("chrony", 1024i64), ("systemd-timesyncd", 2048)] {
            let mut gauge = ClockGauge::new();
            // 500 µs/s is `MAXFREQ / NSEC_PER_USEC` in the kernel's
            // `second_overflow()`; `healthy()`'s own maxerror stands in for what
            // the daemon left behind at the last reset.
            let between_polls = Sample::Kernel(Discipline::from_timex(Timex {
                maxerror: maxpoll_s * 500 + healthy().maxerror,
                ..healthy()
            }));
            assert_eq!(
                gauge.observe(&between_polls),
                None,
                "{daemon} at its default maxpoll warns on a healthy clock"
            );
        }
    }

    /// Edge-triggered. An unsynchronised host stays unsynchronised until
    /// someone fixes it, and a warning a minute for the hours that takes buries
    /// everything else in the journal.
    #[test]
    fn an_alarm_is_raised_once_and_cleared_once() {
        let mut gauge = ClockGauge::new();
        let bad = Sample::Kernel(Discipline::from_timex(Timex {
            ret: TIME_ERROR,
            status: STA_UNSYNC,
            ..healthy()
        }));
        let good = Sample::Kernel(Discipline::from_timex(healthy()));

        assert!(matches!(gauge.observe(&bad), Some(Alarm::Raised(_))));
        assert_eq!(gauge.observe(&bad), None, "warned twice about one fault");
        assert_eq!(gauge.observe(&bad), None);

        assert_eq!(gauge.observe(&good), Some(Alarm::Cleared));
        assert_eq!(gauge.observe(&good), None, "cleared twice");

        assert!(
            matches!(gauge.observe(&bad), Some(Alarm::Raised(_))),
            "a second fault after a recovery must warn again"
        );
    }

    /// "We could not look" is not "it recovered". Clearing on a failed reading
    /// would report a fault as fixed on the strength of no evidence at all.
    #[test]
    fn a_reading_that_failed_neither_raises_nor_clears() {
        let mut gauge = ClockGauge::new();
        let bad = Sample::Kernel(Discipline::from_timex(Timex {
            status: STA_UNSYNC,
            ..healthy()
        }));

        assert!(matches!(gauge.observe(&bad), Some(Alarm::Raised(_))));
        assert_eq!(gauge.observe(&Sample::Unsupported), None);
        assert_eq!(
            gauge.observe(&Sample::Unavailable("EFAULT".to_string())),
            None
        );
        assert_eq!(
            gauge.observe(&bad),
            None,
            "the alarm was silently cleared by a reading that never happened"
        );
    }

    /// A platform with no `adjtimex` must say so rather than record a
    /// healthy-looking clock. A dev run on macOS writes real files that could
    /// be read later; one claiming `sync: true` about a clock nothing measured
    /// would be worse than no record at all.
    #[test]
    fn a_platform_without_adjtimex_says_so_instead_of_claiming_a_healthy_clock() {
        let r = record(&Sample::Unsupported);

        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "clock");
        assert_eq!(r["unsupported"], true);
        assert!(
            r.get("sync").is_none(),
            "an unsupported platform claimed a synchronisation state: {r}"
        );
        assert_eq!(r["platform"], std::env::consts::OS);
    }

    /// A failed syscall is its own answer, and must not look like a healthy
    /// clock either.
    #[test]
    fn a_failed_syscall_is_recorded_as_an_error() {
        let r = record(&Sample::Unavailable(
            "Bad address (os error 14)".to_string(),
        ));

        assert_eq!(r[meta::TAG], "clock");
        assert_eq!(r["error"], "Bad address (os error 14)");
        assert!(r.get("sync").is_none(), "{r}");
        assert!(
            r.get("unsupported").is_none(),
            "a syscall failure was filed as an unsupported platform: {r}"
        );
    }

    /// Every recorded number reaches the sidecar under the name the offline
    /// tools read. A field renamed here is a field the manifest silently stops
    /// finding.
    #[test]
    fn a_healthy_sample_carries_all_five_numbers() {
        let r = record(&Sample::Kernel(Discipline::from_timex(healthy())));

        assert_eq!(r["sync"], true);
        assert_eq!(r["est_error_us"], 400i64);
        assert_eq!(r["max_error_us"], 16_000i64);
        assert_eq!(r["offset_us"], -12.0);
        assert_eq!(r["freq_ppm"], 13.0);
    }

    /// The tag is what keeps this out of the symbol files and inside the one
    /// vocabulary `meta.rs` documents.
    #[test]
    fn the_gauge_produces_a_tagged_record_on_every_platform() {
        let mut gauge = ClockGauge::new();
        let (r, _) = gauge.tick();

        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "clock");
    }

    /// On Linux the syscall has to actually answer — a `sample()` that always
    /// returned `Unavailable` would pass every test above and record nothing
    /// useful in production.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_running_kernel_answers() {
        match sample() {
            Sample::Kernel(_) => {}
            other => panic!("adjtimex did not answer on Linux: {other:?}"),
        }
    }

    /// And off Linux it must be the stub, not a silent failure that happens to
    /// look the same in the record.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn a_non_linux_host_reports_unsupported() {
        assert_eq!(sample(), Sample::Unsupported);
    }
}
