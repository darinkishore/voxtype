//! Machine-readable download progress for `voxtype setup --download`.
//!
//! `--progress-format json` switches the download path from curl's progress
//! bar to newline-delimited JSON on stdout, one object per line, so a GUI
//! (the Omarchy settings panel is the first consumer) can render its own
//! progress bar instead of scraping a terminal:
//!
//! ```text
//! {"event":"progress","model":"sensevoice-small","file":"model.int8.onnx","bytes":12345678,"total":98765432,"pct":12.5}
//! {"event":"done","model":"sensevoice-small"}
//! {"event":"error","model":"sensevoice-small","message":"..."}
//! ```
//!
//! `total` and `pct` are `null` when the size isn't known ahead of time,
//! which only happens if a server refuses the `HEAD` request the whisper
//! path uses to size a download. R2 models always know their sizes, because
//! `manifest.json` lists them.
//!
//! Human output stays the default and is byte-for-byte unchanged. The JSON
//! mode is additive: failures still bail, so the process still exits
//! non-zero, and the `error` event is emitted alongside that rather than in
//! place of it.
//!
//! The selected format lives in a process global. Threading it through would
//! mean a new parameter on every `download_*_model` entry point plus the
//! interactive selector and macOS first-launch callers, and the format is a
//! property of this process's stdout that is set once before any download
//! starts, so an atomic is the honest shape for it.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Smallest gap between two `progress` lines for the same file. The
/// downloader polls a little slower than this, so in practice the cap is the
/// poll interval; this only stops a faster caller from flooding the panel.
const MIN_INTERVAL: Duration = Duration::from_millis(200);

/// How `voxtype setup` reports download progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProgressFormat {
    /// curl's progress bar plus the usual status lines.
    #[default]
    Human,
    /// NDJSON events on stdout, no human chatter.
    Json,
}

impl ProgressFormat {
    /// Parse the `--progress-format` value. Clap restricts the input to
    /// [`crate::cli::PROGRESS_FORMATS`], so `None` means those two lists have
    /// drifted apart.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "human" => Some(ProgressFormat::Human),
            "json" => Some(ProgressFormat::Json),
            _ => None,
        }
    }
}

static JSON: AtomicBool = AtomicBool::new(false);

/// Set once the event consumer has gone away, so we stop paying for writes
/// that can't land.
static STDOUT_BROKEN: AtomicBool = AtomicBool::new(false);

/// Select the format for this process. Called once from the CLI dispatch
/// before any download starts.
///
/// Selecting JSON also stops a disconnecting consumer from killing the
/// process: see [`tolerate_closed_stdout`].
pub fn set_format(format: ProgressFormat) {
    let json = format == ProgressFormat::Json;
    JSON.store(json, Ordering::Relaxed);
    if json {
        tolerate_closed_stdout();
    }
}

/// Ignore `SIGPIPE` for the rest of the process.
///
/// `main` deliberately restores the default disposition so that long-running
/// streams like `voxtype status --follow | head` die quietly when the reader
/// leaves. A download is different: the bytes are the point, and the event
/// stream is only a view of them. With the default disposition, a panel that
/// disconnects (or a `| head -8`) kills voxtype mid-transfer. Ignoring the
/// signal turns that into a write error [`emit`] can absorb, and the download
/// runs to completion.
///
/// The `.part` staging in `super::model` makes an interrupted transfer safe
/// either way; this just stops the interruption from happening at all.
#[cfg(unix)]
fn tolerate_closed_stdout() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

#[cfg(not(unix))]
fn tolerate_closed_stdout() {}

/// Is this process emitting NDJSON? Download code also uses this to stay
/// quiet, since stdout belongs to the event stream in that mode.
pub fn is_json() -> bool {
    JSON.load(Ordering::Relaxed)
}

#[derive(Serialize)]
struct ProgressEvent<'a> {
    event: &'static str,
    model: &'a str,
    file: &'a str,
    bytes: u64,
    total: Option<u64>,
    pct: Option<f64>,
}

#[derive(Serialize)]
struct DoneEvent<'a> {
    event: &'static str,
    model: &'a str,
}

#[derive(Serialize)]
struct ErrorEvent<'a> {
    event: &'static str,
    model: &'a str,
    message: &'a str,
}

/// Percentage to one decimal, or `None` when the total isn't known. Clamped
/// at 100 so a server that reports a smaller size than it sends can't push a
/// progress bar past full.
fn pct(bytes: u64, total: Option<u64>) -> Option<f64> {
    let total = total.filter(|t| *t > 0)?;
    let raw = (bytes as f64 / total as f64 * 100.0).min(100.0);
    Some((raw * 10.0).round() / 10.0)
}

fn progress_line(model: &str, file: &str, bytes: u64, total: Option<u64>) -> String {
    let event = ProgressEvent {
        event: "progress",
        model,
        file,
        bytes,
        total,
        pct: pct(bytes, total),
    };
    serde_json::to_string(&event).unwrap_or_default()
}

fn done_line(model: &str) -> String {
    serde_json::to_string(&DoneEvent {
        event: "done",
        model,
    })
    .unwrap_or_default()
}

fn error_line(model: &str, message: &str) -> String {
    serde_json::to_string(&ErrorEvent {
        event: "error",
        model,
        message,
    })
    .unwrap_or_default()
}

/// Write one NDJSON line and flush it. The flush matters: stdout is block
/// buffered when it's a pipe, and a panel reading progress needs each line as
/// it happens rather than at exit.
///
/// A write failure means the consumer disconnected. That is not the
/// download's problem, so it's recorded and the rest of the run stays silent
/// rather than failing or retrying.
fn emit(line: String) {
    if STDOUT_BROKEN.load(Ordering::Relaxed) {
        return;
    }
    let mut out = std::io::stdout().lock();
    let wrote = writeln!(out, "{}", line).and_then(|()| out.flush());
    if wrote.is_err() {
        STDOUT_BROKEN.store(true, Ordering::Relaxed);
    }
}

/// The setup run for `model` finished with nothing left to do. No-op in human
/// mode.
pub fn done(model: &str) {
    if is_json() {
        emit(done_line(model));
    }
}

/// The download of `model` failed. Additive to the non-zero exit status, not
/// a replacement for it. No-op in human mode.
pub fn error(model: &str, message: &str) {
    if is_json() {
        emit(error_line(model, message));
    }
}

/// Emit the terminal event for a finished `voxtype setup` run.
pub fn report_outcome<T>(model: &str, result: &anyhow::Result<T>) {
    match result {
        Ok(_) => done(model),
        Err(e) => error(model, &e.to_string()),
    }
}

/// Progress reporter for one file of one model.
///
/// Cheap to construct and a no-op in human mode, so callers don't need to
/// branch on the format themselves.
pub struct FileProgress {
    model: String,
    file: String,
    total: Option<u64>,
    last_emit: Option<Instant>,
    last_bytes: Option<u64>,
}

impl FileProgress {
    pub fn new(model: &str, file: &str, total: Option<u64>) -> Self {
        Self {
            model: model.to_string(),
            file: file.to_string(),
            total,
            last_emit: None,
            last_bytes: None,
        }
    }

    /// Report a byte count mid-download. Dropped if the count hasn't moved or
    /// if a line went out less than [`MIN_INTERVAL`] ago.
    pub fn update(&mut self, bytes: u64) {
        if !is_json() || self.last_bytes == Some(bytes) {
            return;
        }
        if let Some(last) = self.last_emit {
            if last.elapsed() < MIN_INTERVAL {
                return;
            }
        }
        self.emit_now(bytes);
    }

    /// Report the final byte count for this file. Never throttled, so every
    /// file ends on a line the panel can complete its bar with.
    pub fn finish(&mut self, bytes: u64) {
        if !is_json() {
            return;
        }
        self.emit_now(bytes);
    }

    fn emit_now(&mut self, bytes: u64) {
        emit(progress_line(&self.model, &self.file, bytes, self.total));
        self.last_emit = Some(Instant::now());
        self.last_bytes = Some(bytes);
    }
}

/// Report a file that needed no download (already on disk and sha256-verified)
/// as complete, so a panel summing per-file progress still reaches 100%.
pub fn file_already_complete(model: &str, file: &str, size: u64) {
    if is_json() {
        emit(progress_line(model, file, size, Some(size)));
    }
}

/// Switch this process into JSON mode for one test, restoring human mode when
/// the guard drops.
///
/// The format is process-wide, and `cargo test` runs tests in parallel, so
/// every test that flips it takes the same lock. Without that, a test
/// asserting human-mode behaviour can observe another test's JSON mode.
#[cfg(test)]
pub(crate) fn json_mode_for_test() -> JsonModeGuard {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    set_format(ProgressFormat::Json);
    JsonModeGuard(guard)
}

#[cfg(test)]
pub(crate) struct JsonModeGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

#[cfg(test)]
impl Drop for JsonModeGuard {
    fn drop(&mut self) {
        set_format(ProgressFormat::Human);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Key order and types are the contract the panel parses, so pin the
    /// exact serialization rather than just the field values.
    #[test]
    fn progress_line_matches_the_documented_shape() {
        assert_eq!(
            progress_line(
                "parakeet-tdt-0.6b-v3-int8",
                "encoder.onnx",
                12_345_678,
                Some(98_765_432)
            ),
            r#"{"event":"progress","model":"parakeet-tdt-0.6b-v3-int8","file":"encoder.onnx","bytes":12345678,"total":98765432,"pct":12.5}"#
        );
    }

    #[test]
    fn done_and_error_lines_match_the_documented_shape() {
        assert_eq!(
            done_line("tiny.en"),
            r#"{"event":"done","model":"tiny.en"}"#
        );
        assert_eq!(
            error_line("tiny.en", "curl exited with code 22"),
            r#"{"event":"error","model":"tiny.en","message":"curl exited with code 22"}"#
        );
    }

    #[test]
    fn unknown_totals_report_null_rather_than_dropping_the_key() {
        let line = progress_line("tiny.en", "ggml-tiny.en.bin", 1024, None);
        assert!(line.contains(r#""total":null"#), "{}", line);
        assert!(line.contains(r#""pct":null"#), "{}", line);
    }

    #[test]
    fn pct_rounds_to_one_decimal_and_clamps_at_full() {
        assert_eq!(pct(0, Some(100)), Some(0.0));
        assert_eq!(pct(1, Some(3)), Some(33.3));
        assert_eq!(pct(2, Some(3)), Some(66.7));
        assert_eq!(pct(100, Some(100)), Some(100.0));
        // A server that under-reports its own size must not exceed 100.
        assert_eq!(pct(120, Some(100)), Some(100.0));
        assert_eq!(pct(10, Some(0)), None);
        assert_eq!(pct(10, None), None);
    }

    #[test]
    fn format_parses_exactly_the_values_clap_accepts() {
        for value in crate::cli::PROGRESS_FORMATS {
            assert!(
                ProgressFormat::parse(value).is_some(),
                "clap accepts '{}' but ProgressFormat::parse rejects it",
                value
            );
        }
        assert_eq!(ProgressFormat::parse("ndjson"), None);
        assert_eq!(ProgressFormat::default(), ProgressFormat::Human);
    }

    /// The throttle only holds back repeats; a changed byte count after the
    /// interval must go out. Human mode emits nothing at all.
    #[test]
    fn throttle_drops_repeats_and_respects_human_mode() {
        let _json = json_mode_for_test();
        let mut p = FileProgress::new("m", "f", Some(100));

        // Human mode: update leaves no trace of an emission.
        set_format(ProgressFormat::Human);
        p.update(10);
        assert!(p.last_emit.is_none());

        set_format(ProgressFormat::Json);
        p.update(10);
        assert_eq!(p.last_bytes, Some(10));
        let first = p.last_emit.expect("first update should emit");

        // Same byte count: no second line.
        p.update(10);
        assert_eq!(p.last_emit, Some(first));
        // New count, but inside the throttle window.
        p.update(20);
        assert_eq!(p.last_emit, Some(first));
        // finish() ignores the window.
        p.finish(100);
        assert_eq!(p.last_bytes, Some(100));
        assert!(p.last_emit.expect("finish should emit") >= first);
    }
}
