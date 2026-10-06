//! Input-device enumeration shared by the TUI's audio section and
//! `voxtype info devices --json`.
//!
//! Extracted from `src/tui/audio.rs` so both surfaces list the same devices
//! and both get the stderr silencing: ALSA's PCM probing writes "Cannot open
//! device /dev/dsp" and similar to stderr for every device cpal touches.
//! Inside the TUI's alternate screen those lines paint over the frame; in
//! `--json` output they'd be noise interleaved with the payload.

use cpal::traits::{DeviceTrait, HostTrait};

/// A capture device offered to the user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InputDevice {
    pub name: String,
    /// True for the synthetic `default` entry, which follows whatever the
    /// system default is at record time.
    pub default: bool,
}

/// Capture device names, with the synthetic `default` entry first.
pub fn enumerate_input_devices() -> Vec<String> {
    let _silenced = SilencedStderr::install();

    let mut out = vec!["default".to_string()];
    let host = cpal::default_host();
    if let Ok(devices) = host.input_devices() {
        for d in devices {
            if let Ok(name) = d.name() {
                if name != "default" && !out.contains(&name) {
                    out.push(name);
                }
            }
        }
    }
    out
}

/// Same list as [`enumerate_input_devices`], tagged for JSON output.
pub fn input_devices() -> Vec<InputDevice> {
    enumerate_input_devices()
        .into_iter()
        .map(|name| InputDevice {
            default: name == "default",
            name,
        })
        .collect()
}

/// RAII guard that redirects fd 2 (stderr) to /dev/null on construction and
/// restores the original fd on drop.
struct SilencedStderr {
    saved_fd: Option<libc::c_int>,
}

impl SilencedStderr {
    fn install() -> Self {
        let null_fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_WRONLY) };
        if null_fd < 0 {
            return Self { saved_fd: None };
        }
        let saved = unsafe { libc::dup(libc::STDERR_FILENO) };
        if saved < 0 {
            unsafe { libc::close(null_fd) };
            return Self { saved_fd: None };
        }
        unsafe { libc::dup2(null_fd, libc::STDERR_FILENO) };
        unsafe { libc::close(null_fd) };
        Self {
            saved_fd: Some(saved),
        }
    }
}

impl Drop for SilencedStderr {
    fn drop(&mut self) {
        if let Some(saved) = self.saved_fd.take() {
            unsafe {
                libc::dup2(saved, libc::STDERR_FILENO);
                libc::close(saved);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs on CI hosts with no sound card, so the only guarantee is the
    /// synthetic `default` entry and no duplicates.
    #[test]
    fn default_is_always_offered_exactly_once() {
        let devices = input_devices();
        let defaults: Vec<_> = devices.iter().filter(|d| d.default).collect();
        assert_eq!(defaults.len(), 1, "got {:?}", devices);
        assert_eq!(defaults[0].name, "default");

        let mut names: Vec<&str> = devices.iter().map(|d| d.name.as_str()).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "duplicate device names: {:?}", devices);
    }
}
