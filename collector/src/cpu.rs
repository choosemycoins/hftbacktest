//! What the host's CPU was doing, and how much of it the hypervisor took.
//!
//! The collector's failure mode is always the same sentence — *the writer
//! cannot keep up* — and it has two completely different causes. Either the
//! venue is sending more than this process can gzip, or the host is not being
//! given the CPU it thinks it has. From inside the process those are
//! indistinguishable: the queue fills, `queue_overflow` is written, the run
//! ends. Every gauge already here would read the same in both cases.
//!
//! The recording boxes are **burstable** instances (t4-class), where the second
//! cause is not hypothetical. Such an instance is entitled to a baseline
//! fraction of a vCPU and banks credits while it stays under it; once the
//! credits are spent the hypervisor throttles it back to that baseline, and the
//! guest kernel accounts the difference as `steal`. So the very failure that
//! looks most like "the venue flooded us" is the one the venue had no part in,
//! and the only witness is a counter in `/proc/stat`.
//!
//! Sampled once a minute into `_meta` alongside the clock, liveness and disk
//! gauges, for the reasons `clock.rs` sets out: the sidecar is the one file an
//! operator can tail live, and a recording that carries its own history can be
//! argued about afterwards from its own bytes.
//!
//! ## Why `/proc/stat` and not a load average
//!
//! `/proc/loadavg` counts runnable tasks, which on a throttled instance barely
//! moves: the collector is one task, and one task waiting for a CPU it is not
//! being given looks like one task running. `steal` is the hypervisor's own
//! admission, in the same units as every other CPU number, and it needs no
//! privileges, no agent and no subprocess to read.
//!
//! Non-Linux hosts have no `/proc/stat`, so they record [`Reading::Unsupported`]
//! rather than a healthy-looking zero. A dev run on macOS must not be able to
//! produce a recording that claims nothing was stolen.

use serde_json::Value;

/// Steal above which a minute is called throttled, in percent of the interval.
///
/// Derived rather than picked, from the two ends it has to sit between.
///
/// * **Below it, the noise.** Ordinary shared tenancy costs a fraction of a
///   percent: `steal` there is the scheduling jitter of a neighbour, and on the
///   idle end of a t4 it reads well under 1% minute after minute. A threshold
///   inside that band would fire on healthy hosts, and an alarm that fires on
///   healthy hosts is one nobody reads — the same argument `clock.rs` makes for
///   its own deliberately coarse limit.
/// * **Above it, the fault.** A burstable instance out of credits is capped at
///   its baseline entitlement — 10–40% of a vCPU across the t4 sizes — so a
///   process that wants a whole one is handed back 60–90% as steal. Throttling
///   does not arrive gently at 12%; it arrives as most of the machine.
///
/// 10% is an order of magnitude above the noise and six times below the fault,
/// which means it cannot fire on ordinary jitter and cannot miss throttling. It
/// also fires *early*: at 10% the collector is still keeping up, so the warning
/// arrives while there is time to resize the instance or shed a symbol, rather
/// than alongside the `queue_overflow` it was supposed to explain.
///
/// It is not a limit on anything. Nothing here ever stops the collector over
/// CPU: the data being recorded is still worth having, and a process that
/// exited because the hypervisor was busy would take the recording down for the
/// one reason it cannot do anything about.
pub const STEAL_WARN_PCT: f64 = 10.0;

/// The counters of the aggregate `cpu` line, in USER_HZ jiffies since boot.
///
/// A plain struct rather than a parsed-and-interpreted blob, for the reason
/// `clock.rs` keeps `Timex`: these are cumulative, and every interesting
/// question is about a *difference* between two of them. Splitting the read
/// from the subtraction is what lets the subtraction — where a wrong denominator
/// is silent — be tested on a machine with no `/proc/stat` at all.
///
/// `guest` and `guest_nice` are deliberately absent. Linux counts them inside
/// `user` and `nice` already (`Documentation/filesystems/proc.rst`), so a struct
/// carrying them invites a total that double-counts, and a total that
/// double-counts inflates the denominator and quietly shrinks the one number
/// this module exists to report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuTimes {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    /// Time the hypervisor spent running something that is not this guest.
    pub steal: u64,
}

/// How one interval was spent, in percent. The four sum to 100.
///
/// Four numbers rather than eight, grouped so that nothing falls between them:
///
/// * `user_pct` — user time, `nice` folded in. Reniced work is still this
///   machine's work, and a separate field for it would be one nobody reads.
/// * `system_pct` — kernel time, with `irq` and `softirq` folded in. Softirq is
///   where a busy network card's cost lands, which on this workload is the
///   kernel time worth seeing.
/// * `idle_pct` — nothing to run, with `iowait` folded in. `iowait` *is* idle by
///   the kernel's own accounting — the CPU was free, a task happened to be
///   blocked on I/O — and reporting it separately invites reading it as work.
/// * `steal_pct` — the hypervisor ran someone else.
///
/// They are grouped rather than dropped because an operator who reads three of
/// these has to be able to name the fourth. Reporting only the four raw fields
/// asked for would leave `nice`, `iowait`, `irq` and `softirq` unaccounted, and
/// a sidecar whose numbers sum to 87 raises a question about the gauge instead
/// of about the host.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Utilisation {
    pub steal_pct: f64,
    pub user_pct: f64,
    pub system_pct: f64,
    pub idle_pct: f64,
}

impl Utilisation {
    /// The share of the interval between two readings, or `None` if there is no
    /// interval to divide by.
    ///
    /// `None` in two cases, both of which must not become a number:
    ///
    /// * a counter went **backwards** — a CPU came or went, or the file was read
    ///   across a rewrite. On unsigned counters that subtraction wraps to
    ///   something astronomical, which would be recorded as a perfectly
    ///   plausible-looking percentage;
    /// * the interval is **empty** — two reads inside one jiffy, which is a
    ///   division by zero.
    pub fn between(prev: CpuTimes, now: CpuTimes) -> Option<Self> {
        let user = now.user.checked_sub(prev.user)?;
        let nice = now.nice.checked_sub(prev.nice)?;
        let system = now.system.checked_sub(prev.system)?;
        let idle = now.idle.checked_sub(prev.idle)?;
        let iowait = now.iowait.checked_sub(prev.iowait)?;
        let irq = now.irq.checked_sub(prev.irq)?;
        let softirq = now.softirq.checked_sub(prev.softirq)?;
        let steal = now.steal.checked_sub(prev.steal)?;

        // Every field of the delta exactly once — which is why `guest` is not a
        // field: it is already inside `user`.
        let total = user + nice + system + idle + iowait + irq + softirq + steal;
        if total == 0 {
            return None;
        }
        let pct = |part: u64| part as f64 * 100.0 / total as f64;
        Some(Self {
            steal_pct: pct(steal),
            user_pct: pct(user + nice),
            system_pct: pct(system + irq + softirq),
            idle_pct: pct(idle + iowait),
        })
    }

    /// Why this minute deserves a line in the journal, or `None` if it does not.
    fn fault(&self) -> Option<String> {
        (self.steal_pct > STEAL_WARN_PCT).then(|| {
            format!(
                "the hypervisor took {:.1}% of the last interval (steal), past the \
                 {STEAL_WARN_PCT:.0}% limit; idle {:.1}%, user {:.1}%, system {:.1}%",
                self.steal_pct, self.idle_pct, self.user_pct, self.system_pct
            )
        })
    }
}

/// Reads the aggregate `cpu` line of a `/proc/stat`.
///
/// Split out from the file read so the parsing — where a column counted wrong
/// is silent — is testable on a machine that has no `/proc/stat`.
///
/// Two refusals rather than a best effort:
///
/// * **no aggregate line.** Defaulting to zeros would record a perfectly idle
///   host with no steal, which is a lie in the shape of a healthy reading.
/// * **fewer than eight columns.** `steal` is the eighth (kernels before 2.6.11
///   have seven). Treating an absent column as zero would report "no steal" on
///   the one kernel that could not have told us either way — exactly the
///   misreading this module exists to prevent.
///
/// Columns nine and ten (`guest`, `guest_nice`) are read and dropped on
/// purpose: Linux counts them inside `user` and `nice` already.
pub fn parse_proc_stat(text: &str) -> Result<CpuTimes, String> {
    // `"cpu "` with the space: `cpu0` is one core's line and reading it as the
    // host's would report a single core's steal for the machine.
    let line = text
        .lines()
        .find(|l| l.starts_with("cpu "))
        .ok_or_else(|| "/proc/stat has no aggregate `cpu ` line".to_string())?;

    let mut fields = Vec::with_capacity(8);
    for (i, word) in line.split_whitespace().skip(1).take(8).enumerate() {
        fields.push(
            word.parse::<u64>()
                .map_err(|e| format!("/proc/stat `cpu` column {}: {word:?}: {e}", i + 1))?,
        );
    }
    if fields.len() < 8 {
        return Err(format!(
            "/proc/stat `cpu` line has {} columns, so it carries no steal figure; \
             this kernel cannot report what the hypervisor took",
            fields.len()
        ));
    }
    Ok(CpuTimes {
        user: fields[0],
        nice: fields[1],
        system: fields[2],
        idle: fields[3],
        iowait: fields[4],
        irq: fields[5],
        softirq: fields[6],
        steal: fields[7],
    })
}

/// One read of the host's counters, including the two ways there isn't one.
#[derive(Clone, Debug, PartialEq)]
pub enum Sample {
    /// The kernel answered.
    Kernel(CpuTimes),
    /// This platform has no `/proc/stat`.
    Unsupported,
    /// Linux, and the read or the parse failed. Deliberately not folded into
    /// `Unsupported`, for the reason `clock.rs` keeps them apart: "there is no
    /// such file here" and "the file would not open" send an investigation to
    /// opposite places.
    Unavailable(String),
}

/// Asks the host what it did with the CPU.
///
/// The `#[cfg]` is confined to [`read_proc_stat`], and the parse happens here,
/// on every platform. Pushing the parse inside the Linux arm instead — the
/// obvious way to write it — makes [`parse_proc_stat`] unreachable on every
/// other target, and `dead_code` then fires on the exact code that has to be
/// right on the deployed one and is tested on all the rest. Suppressing that
/// with `allow(dead_code)` would also suppress the next thing that really did
/// become dead. `clock.rs` documents the same trade for the same reason.
pub fn sample() -> Sample {
    match read_proc_stat() {
        Ok(Some(text)) => match parse_proc_stat(&text) {
            Ok(times) => Sample::Kernel(times),
            // A `/proc/stat` this cannot read is not a platform without one.
            Err(error) => Sample::Unavailable(error),
        },
        Ok(None) => Sample::Unsupported,
        Err(error) => Sample::Unavailable(error),
    }
}

/// The bytes of `/proc/stat`; `None` where there is no such file.
#[cfg(target_os = "linux")]
fn read_proc_stat() -> Result<Option<String>, String> {
    std::fs::read_to_string("/proc/stat")
        .map(Some)
        .map_err(|e| e.to_string())
}

#[cfg(not(target_os = "linux"))]
fn read_proc_stat() -> Result<Option<String>, String> {
    Ok(None)
}

/// What one minute of this gauge amounts to.
///
/// Distinct from [`Sample`] because the recordable thing is a *difference*, and
/// the ways there isn't one differ: the very first tick read the host perfectly
/// well and still has nothing to report.
#[derive(Clone, Debug, PartialEq)]
pub enum Reading {
    /// Percentages over the interval since the previous sample.
    Interval(Utilisation),
    /// The first sample of the process: read, kept, nothing to subtract from.
    FirstSample,
    /// No `/proc/stat` on this platform.
    Unsupported,
    /// The read failed, or the counters did not make sense across the interval.
    Unavailable(String),
}

/// `{"_collector":"cpu", …}` — the sidecar record for one minute.
///
/// The percentages are rounded to two decimals. The sidecar is a file an
/// operator tails, and seventeen significant figures of a number whose input is
/// a 10 ms jiffy claims a precision that does not exist.
pub fn record(reading: &Reading) -> Value {
    let mut fields = serde_json::Map::new();
    fields.insert(crate::meta::TAG.to_string(), Value::from("cpu"));
    match reading {
        Reading::Interval(u) => {
            let round = |v: f64| (v * 100.0).round() / 100.0;
            fields.insert("steal_pct".to_string(), Value::from(round(u.steal_pct)));
            fields.insert("user_pct".to_string(), Value::from(round(u.user_pct)));
            fields.insert("system_pct".to_string(), Value::from(round(u.system_pct)));
            fields.insert("idle_pct".to_string(), Value::from(round(u.idle_pct)));
        }
        Reading::FirstSample => {
            fields.insert("first_sample".to_string(), Value::from(true));
        }
        Reading::Unsupported => {
            fields.insert("unsupported".to_string(), Value::from(true));
            fields.insert(
                "platform".to_string(),
                Value::from(std::env::consts::OS.to_string()),
            );
        }
        Reading::Unavailable(error) => {
            fields.insert("error".to_string(), Value::from(error.clone()));
        }
    }
    Value::Object(fields)
}

/// A change of state worth a log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Alarm {
    /// The host stopped getting the CPU it asked for, and by how much.
    Raised(String),
    /// It recovered.
    Cleared,
}

/// Samples `/proc/stat` on the minutely tick and reports only the **edges**.
///
/// Edge-triggered for the reason [`crate::clock::ClockGauge`] is: an instance
/// that has spent its credits stays throttled until they accrue again, which is
/// hours, and a warning a minute for hours is a journal nobody reads. The state
/// itself is in every minute's record; the log carries the two transitions.
#[derive(Default)]
pub struct CpuGauge {
    prev: Option<CpuTimes>,
    alarmed: bool,
}

impl CpuGauge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one sample in: the minute's reading, and the edge if there was one.
    ///
    /// A sample that is not a reading — no `/proc/stat`, or a read that failed —
    /// leaves **both** pieces of state exactly as they were. Not the alarm,
    /// because "we could not look" is not "it recovered" and clearing on it is
    /// how a fault comes to be reported as fixed. And not `prev`, because
    /// dropping it would make the next good sample another `FirstSample` and
    /// silently discard the interval across the gap — the minutes either side of
    /// a failed read are exactly the ones worth measuring.
    pub fn observe(&mut self, sample: &Sample) -> (Reading, Option<Alarm>) {
        let times = match sample {
            Sample::Kernel(times) => *times,
            Sample::Unsupported => return (Reading::Unsupported, None),
            Sample::Unavailable(error) => return (Reading::Unavailable(error.clone()), None),
        };
        let previous = self.prev.replace(times);
        let Some(utilisation) = previous.and_then(|p| Utilisation::between(p, times)) else {
            return match previous {
                None => (Reading::FirstSample, None),
                // Read fine, subtracted to nonsense: a CPU came or went, or the
                // two reads landed in one jiffy. `prev` has already been moved
                // on, so the next minute measures against this one.
                Some(_) => (
                    Reading::Unavailable(
                        "the counters did not advance across the interval (a CPU came or \
                         went, or two reads landed in one jiffy)"
                            .to_string(),
                    ),
                    None,
                ),
            };
        };

        let alarm = match (utilisation.fault(), self.alarmed) {
            (Some(reason), false) => {
                self.alarmed = true;
                Some(Alarm::Raised(reason))
            }
            (None, true) => {
                self.alarmed = false;
                Some(Alarm::Cleared)
            }
            _ => None,
        };
        (Reading::Interval(utilisation), alarm)
    }

    /// One minute's work: the record for `_meta`, and the edge if there was one.
    pub fn tick(&mut self) -> (Value, Option<Alarm>) {
        let (reading, alarm) = self.observe(&sample());
        (record(&reading), alarm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta;

    /// A real `/proc/stat` head, from a two-vCPU Linux host.
    const PROC_STAT: &str = "\
cpu  9553 60 4212 1258391 1044 0 141 812 0 0
cpu0 4830 30 2130 629119 512 0 90 400 0 0
cpu1 4723 30 2082 629272 532 0 51 412 0 0
intr 2381911 9 0 0 0 0 0 0 0 1 0
ctxt 6183492
btime 1785283200
processes 24191
procs_running 2
procs_blocked 0
";

    /// A minute in which the collector had the machine to itself.
    fn idle_host() -> (CpuTimes, CpuTimes) {
        let prev = CpuTimes {
            user: 9553,
            nice: 60,
            system: 4212,
            idle: 1_258_391,
            iowait: 1044,
            irq: 0,
            softirq: 141,
            steal: 812,
        };
        // 60s x 2 vCPU x 100 Hz = 12 000 jiffies: 600 user, 200 system, the
        // rest idle.
        let now = CpuTimes {
            user: prev.user + 600,
            system: prev.system + 200,
            idle: prev.idle + 11_200,
            ..prev
        };
        (prev, now)
    }

    /// The aggregate `cpu` line is the one that is read; the per-core lines and
    /// everything below them are not. Taking `cpu0` by accident would report one
    /// core's steal as the host's, which on an unevenly scheduled guest is a
    /// different number entirely.
    #[test]
    fn only_the_aggregate_cpu_line_is_read() {
        let t = parse_proc_stat(PROC_STAT).expect("a real /proc/stat must parse");
        assert_eq!(t.user, 9553);
        assert_eq!(t.nice, 60);
        assert_eq!(t.system, 4212);
        assert_eq!(t.idle, 1_258_391);
        assert_eq!(t.iowait, 1044);
        assert_eq!(t.irq, 0);
        assert_eq!(t.softirq, 141);
        assert_eq!(t.steal, 812);
    }

    /// A `/proc/stat` with no aggregate line is an error, not a zeroed reading.
    /// Zeros would be recorded as a perfectly idle host with no steal at all.
    #[test]
    fn a_file_without_the_aggregate_line_is_an_error() {
        let err = parse_proc_stat("cpu0 1 2 3 4 5 6 7 8\nbtime 1785283200\n")
            .expect_err("a file with no aggregate cpu line must not parse");
        assert!(err.contains("cpu"), "{err}");
    }

    /// A kernel too old to report `steal` must not be read as a host with no
    /// steal — that is precisely the reading this gauge exists to disprove.
    #[test]
    fn a_line_without_the_steal_field_is_refused_rather_than_read_as_zero() {
        let err = parse_proc_stat("cpu  9553 60 4212 1258391 1044 0 141\n")
            .expect_err("a line with no steal column must not parse as steal=0");
        assert!(err.contains("steal"), "{err}");
    }

    /// The whole point of splitting the read from the interpretation: the
    /// counters are monotonic since boot, so a gauge that reported them directly
    /// would report the average since boot for ever and never notice a bad
    /// minute. Boot totals here are large and the interval is small and busy.
    #[test]
    fn the_percentages_are_over_the_interval_and_not_since_boot() {
        let prev = CpuTimes {
            user: 100_000,
            nice: 0,
            system: 20_000,
            idle: 8_000_000,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 1_000,
        };
        // 12 000 jiffies of interval, three quarters of it stolen.
        let now = CpuTimes {
            user: prev.user + 2_000,
            system: prev.system + 1_000,
            steal: prev.steal + 9_000,
            ..prev
        };

        let u = Utilisation::between(prev, now).expect("a forward interval must read");
        assert!(
            (u.steal_pct - 75.0).abs() < 1e-9,
            "steal read as {}",
            u.steal_pct
        );
        assert!(
            (u.user_pct - 16.666_666_666).abs() < 1e-6,
            "user {}",
            u.user_pct
        );
        assert!((u.idle_pct - 0.0).abs() < 1e-9, "idle {}", u.idle_pct);
    }

    /// The four reported numbers account for the whole interval. An operator
    /// reading three of them has to be able to tell what the fourth was.
    #[test]
    fn the_four_percentages_account_for_the_whole_interval() {
        let (prev, now) = idle_host();
        let u = Utilisation::between(prev, now).unwrap();
        let sum = u.steal_pct + u.user_pct + u.system_pct + u.idle_pct;
        assert!(
            (sum - 100.0).abs() < 1e-9,
            "the four numbers sum to {sum}, not 100"
        );
    }

    /// `guest` and `guest_nice` are already counted inside `user` and `nice`
    /// (`Documentation/filesystems/proc.rst`). Adding them to the total would
    /// inflate the denominator, which quietly *shrinks* the reported steal — the
    /// one number this gauge is for.
    #[test]
    fn guest_time_is_not_counted_twice() {
        let prev = parse_proc_stat("cpu  0 0 0 0 0 0 0 0 0 0\n").unwrap();
        // 10 000 jiffies of interval: half user (all of it guest), half stolen.
        let now = parse_proc_stat("cpu  5000 0 0 0 0 0 0 5000 5000 0\n").unwrap();

        let u = Utilisation::between(prev, now).unwrap();
        assert!(
            (u.steal_pct - 50.0).abs() < 1e-9,
            "guest time was added to the total, diluting steal to {}",
            u.steal_pct
        );
    }

    /// Counters that go backwards mean the reading cannot be trusted — a CPU
    /// came or went, or the file was read across a rewrite. Reporting a negative
    /// percentage, or an unsigned wrap, would be worse than reporting nothing.
    #[test]
    fn a_counter_that_went_backwards_yields_no_reading() {
        let (prev, now) = idle_host();
        assert_eq!(Utilisation::between(now, prev), None);
    }

    /// Two reads inside one jiffy have no interval to divide by.
    #[test]
    fn a_zero_length_interval_yields_no_reading() {
        let (prev, _) = idle_host();
        assert_eq!(Utilisation::between(prev, prev), None);
    }

    /// The first sample has nothing to subtract from, so it claims no
    /// percentages at all. A zeroed reading here would open every recording with
    /// a minute of "no steal" that was never measured.
    #[test]
    fn the_first_sample_claims_no_percentages() {
        let mut gauge = CpuGauge::new();
        let (prev, _) = idle_host();

        let (reading, alarm) = gauge.observe(&Sample::Kernel(prev));
        assert_eq!(reading, Reading::FirstSample);
        assert_eq!(alarm, None);

        let r = record(&reading);
        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "cpu");
        assert_eq!(r["first_sample"], true);
        assert!(
            r.get("steal_pct").is_none(),
            "the first sample claimed a steal figure it never measured: {r}"
        );
    }

    /// Every recorded number reaches the sidecar under the name the offline
    /// tools read. A field renamed here is a field the report silently stops
    /// finding.
    #[test]
    fn a_measured_minute_carries_all_four_numbers() {
        let mut gauge = CpuGauge::new();
        let (prev, now) = idle_host();
        gauge.observe(&Sample::Kernel(prev));

        let (reading, _) = gauge.observe(&Sample::Kernel(now));
        let r = record(&reading);

        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "cpu");
        assert_eq!(r["steal_pct"], 0.0);
        assert_eq!(r["user_pct"], 5.0);
        assert_eq!(r["system_pct"], 1.67);
        assert_eq!(r["idle_pct"], 93.33);
    }

    /// The incident this exists for. A burstable instance that has spent its
    /// credits is throttled to its baseline, and the collector then cannot keep
    /// up — which looks exactly like a venue flooding us or a slow disk. The
    /// hypervisor's own number is the only thing that tells those apart.
    #[test]
    fn credit_exhaustion_on_a_burstable_instance_is_reported() {
        let mut gauge = CpuGauge::new();
        let (prev, _) = idle_host();
        // Throttled to a ~20% baseline: the guest wants the CPU and the
        // hypervisor gives it away.
        let throttled = CpuTimes {
            user: prev.user + 1_800,
            system: prev.system + 600,
            steal: prev.steal + 9_600,
            ..prev
        };

        gauge.observe(&Sample::Kernel(prev));
        let (reading, alarm) = gauge.observe(&Sample::Kernel(throttled));

        let Reading::Interval(u) = reading else {
            panic!("a forward interval must produce a reading: {reading:?}");
        };
        assert!((u.steal_pct - 80.0).abs() < 1e-9, "steal {}", u.steal_pct);
        match alarm {
            Some(Alarm::Raised(reason)) => assert!(
                reason.contains("80"),
                "the warning has to carry the number an operator acts on: {reason}"
            ),
            other => panic!("credit exhaustion raised {other:?}"),
        }
    }

    /// A few percent of steal is what shared tenancy costs and is not a fault.
    /// A gauge that warned about it would be one nobody reads by the second day.
    #[test]
    fn ordinary_shared_tenancy_steal_does_not_warn() {
        let mut gauge = CpuGauge::new();
        let (prev, _) = idle_host();
        let busy_neighbour = CpuTimes {
            user: prev.user + 600,
            system: prev.system + 200,
            steal: prev.steal + 120, // 1% of a 12 000-jiffy interval
            idle: prev.idle + 11_080,
            ..prev
        };

        gauge.observe(&Sample::Kernel(prev));
        let (_, alarm) = gauge.observe(&Sample::Kernel(busy_neighbour));
        assert_eq!(alarm, None, "1% steal warned");
    }

    /// Edge-triggered, for the reason the clock gauge is: a throttled instance
    /// stays throttled until the credits recover, and a warning a minute for the
    /// hours that takes buries everything else in the journal.
    #[test]
    fn an_alarm_is_raised_once_and_cleared_once() {
        let mut gauge = CpuGauge::new();
        let (base, _) = idle_host();
        let mut clock = base;
        let mut advance = |gauge: &mut CpuGauge, steal: u64| {
            clock = CpuTimes {
                user: clock.user + (12_000 - steal) / 2,
                idle: clock.idle + (12_000 - steal) / 2,
                steal: clock.steal + steal,
                ..clock
            };
            gauge.observe(&Sample::Kernel(clock)).1
        };

        assert_eq!(advance(&mut gauge, 0), None, "the first sample warned");
        assert!(matches!(advance(&mut gauge, 9_600), Some(Alarm::Raised(_))));
        assert_eq!(
            advance(&mut gauge, 9_600),
            None,
            "warned twice about one fault"
        );
        assert_eq!(advance(&mut gauge, 0), Some(Alarm::Cleared));
        assert_eq!(advance(&mut gauge, 0), None, "cleared twice");
        assert!(
            matches!(advance(&mut gauge, 9_600), Some(Alarm::Raised(_))),
            "a second throttling after a recovery must warn again"
        );
    }

    /// "We could not look" is not "it recovered", and it is not a fresh
    /// baseline either: a failed read must leave the previous counters in place
    /// so the next good one measures across the gap instead of losing it.
    #[test]
    fn a_reading_that_failed_neither_raises_nor_clears() {
        let mut gauge = CpuGauge::new();
        let (prev, _) = idle_host();
        let throttled = CpuTimes {
            steal: prev.steal + 9_600,
            user: prev.user + 1_200,
            idle: prev.idle + 1_200,
            ..prev
        };

        gauge.observe(&Sample::Kernel(prev));
        assert!(matches!(
            gauge.observe(&Sample::Kernel(throttled)).1,
            Some(Alarm::Raised(_))
        ));
        assert_eq!(gauge.observe(&Sample::Unsupported).1, None);
        assert_eq!(gauge.observe(&Sample::Unavailable("EACCES".into())).1, None);
        assert_eq!(
            gauge.observe(&Sample::Kernel(throttled)).1,
            None,
            "the alarm was silently cleared by a reading that never happened"
        );
    }

    /// A failed read must not cost the interval either side of it. Dropping the
    /// kept counters would make the next good sample a `FirstSample` again, so a
    /// single unreadable minute would blind the gauge for two — and the minutes
    /// around a failure are the ones worth measuring.
    #[test]
    fn a_failed_read_does_not_reset_the_baseline() {
        let mut gauge = CpuGauge::new();
        let (prev, now) = idle_host();

        gauge.observe(&Sample::Kernel(prev));
        gauge.observe(&Sample::Unavailable("EINTR".into()));
        let (reading, _) = gauge.observe(&Sample::Kernel(now));

        match reading {
            Reading::Interval(u) => assert!((u.idle_pct - 93.333_333).abs() < 1e-3),
            other => panic!("the interval across a failed read was lost: {other:?}"),
        }
    }

    /// A platform with no `/proc/stat` must say so rather than record a host
    /// with no steal. A dev run on macOS writes real files that could be read
    /// later, and one claiming `steal_pct: 0` about a machine nothing measured
    /// is worse than no record at all.
    #[test]
    fn a_platform_without_proc_stat_says_so_instead_of_claiming_no_steal() {
        let r = record(&Reading::Unsupported);

        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "cpu");
        assert_eq!(r["unsupported"], true);
        assert_eq!(r["platform"], std::env::consts::OS);
        assert!(
            r.get("steal_pct").is_none(),
            "an unsupported platform claimed a steal figure: {r}"
        );
    }

    /// A failed read is its own answer, and must not look like an unsupported
    /// platform: "there is no such file here" and "the file would not open" send
    /// an investigation to opposite places.
    #[test]
    fn a_failed_read_is_recorded_as_an_error() {
        let r = record(&Reading::Unavailable(
            "Permission denied (os error 13)".into(),
        ));

        assert_eq!(r[meta::TAG], "cpu");
        assert_eq!(r["error"], "Permission denied (os error 13)");
        assert!(r.get("steal_pct").is_none(), "{r}");
        assert!(
            r.get("unsupported").is_none(),
            "a read failure was filed as an unsupported platform: {r}"
        );
    }

    /// The tag is what keeps this out of the symbol files and inside the one
    /// vocabulary `meta.rs` documents.
    #[test]
    fn the_gauge_produces_a_tagged_record_on_every_platform() {
        let mut gauge = CpuGauge::new();
        let (r, _) = gauge.tick();

        assert!(meta::is_record(&r), "{r}");
        assert_eq!(r[meta::TAG], "cpu");
    }

    /// On Linux the file has to actually answer — a `sample()` that always
    /// returned `Unavailable` would pass every test above and record nothing
    /// useful on the one platform this ships to.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_running_kernel_answers() {
        match sample() {
            Sample::Kernel(t) => assert!(t.idle > 0, "an idle counter of zero: {t:?}"),
            other => panic!("/proc/stat did not answer on Linux: {other:?}"),
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
