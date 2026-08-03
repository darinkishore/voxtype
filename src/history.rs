//! Transcript history: append-only JSONL at
//! `$XDG_DATA_HOME/voxtype/history.jsonl` (usually
//! `~/.local/share/voxtype/history.jsonl`). The daemon appends one entry per
//! delivered dictation; `voxtype history` reads it back. Failures are
//! logged, never fatal — history must not break dictation.

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// RFC 3339 local timestamp of delivery.
    pub at: String,
    /// How the session ended: "stop", "timeout", or "batch".
    pub ended: String,
    pub text: String,
}

pub fn history_path() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join("voxtype").join("history.jsonl"))
}

/// Append one entry.
pub fn append(text: &str, ended: &str) {
    let Some(path) = history_path() else { return };
    let entry = HistoryEntry {
        at: chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ended: ended.to_string(),
        text: text.to_string(),
    };
    let res = (|| -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let line = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
        writeln!(f, "{}", line)
    })();
    if let Err(e) = res {
        tracing::warn!("Failed to append transcript history: {}", e);
    }
}

/// Last `count` entries, oldest first. Unparseable lines are skipped.
pub fn read_last(count: usize) -> Vec<HistoryEntry> {
    let Some(path) = history_path() else {
        return Vec::new();
    };
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let mut entries: Vec<HistoryEntry> = content
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let n = entries.len();
    if n > count {
        entries.drain(..n - count);
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_roundtrips_through_json() {
        let e = HistoryEntry {
            at: "2026-08-03T14:00:00-07:00".into(),
            ended: "stop".into(),
            text: "hello world".into(),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: HistoryEntry = serde_json::from_str(&s).unwrap();
        assert_eq!(back.text, "hello world");
        assert_eq!(back.ended, "stop");
    }
}
