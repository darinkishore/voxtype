//! OpenAI Realtime engine configuration.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::super::default_true;

/// OpenAI Realtime cloud streaming WebSocket STT configuration
/// Requires: cargo build --features openai-realtime
///
/// OpenAI Realtime is a paid cloud STT provider (GA transcription API,
/// `gpt-live-transcribe`). API key required: set `api_key` here,
/// `api_key_file` (a file containing just the key), or the `OPENAI_API_KEY`
/// env var. Resolution order: `api_key` > `api_key_file` > env.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenaiRealtimeConfig {
    /// API key. If unset, falls back to `api_key_file`, then the
    /// `OPENAI_API_KEY` env var.
    #[serde(default)]
    pub api_key: Option<String>,

    /// Path to a file containing the API key (read once at construction,
    /// trimmed of surrounding whitespace). Checked after `api_key`, before
    /// the `OPENAI_API_KEY` env var.
    #[serde(default)]
    pub api_key_file: Option<PathBuf>,

    /// OpenAI Realtime transcription model. Default: "gpt-live-transcribe"
    /// (GA live transcription model). "gpt-transcribe" is also valid
    /// (transcribes after commit; returns detected languages).
    #[serde(default = "default_openai_realtime_model")]
    pub model: String,

    /// Transcription latency/quality tradeoff: "minimal", "low", "medium",
    /// "high", or "xhigh". Default: "low".
    #[serde(default = "default_openai_realtime_delay")]
    pub delay: String,

    /// Free-form vocabulary bias prompt. Mapped to
    /// `session.audio.input.transcription.prompt`. Empty/unset omits the
    /// field entirely.
    #[serde(default)]
    pub prompt: Option<String>,

    /// Literal high-value spellings to prime the model with (proper names,
    /// jargon). Mapped to `session.audio.input.transcription.keywords`.
    /// Entries must not contain `<`, `>`, or newlines — OpenAI rejects the
    /// session.update otherwise; voxtype validates this at construction.
    #[serde(default)]
    pub keywords: Vec<String>,

    /// ISO 639-1 language codes. Mapped to
    /// `session.audio.input.transcription.languages` (plural — OpenAI's GA
    /// API, unlike the pre-GA `language` singular field). Default: `["en"]`.
    #[serde(default = "default_openai_realtime_languages")]
    pub languages: Vec<String>,

    /// Input noise reduction mode: "near_field", "far_field", or "" to
    /// disable (omits `session.audio.input.noise_reduction` entirely).
    /// Default: "near_field".
    #[serde(default = "default_openai_realtime_noise_reduction")]
    pub noise_reduction: String,

    /// Enable server-side VAD (`turn_detection: {"type":"server_vad"}`).
    /// Default **false**: the default model `gpt-live-transcribe` rejects
    /// turn_detection outright ("Turn detection is not supported for this
    /// transcription model" — verified against the live API 2026-08-02),
    /// and its intended flow is deltas streaming during recording with an
    /// explicit `input_audio_buffer.commit` on record stop (which voxtype
    /// sends automatically). Set true only with a model that supports
    /// server VAD; the server then finalizes turns on its own —
    /// progressive per-utterance finals typed while dictating, mirroring
    /// Soniox's `is_final` semantics.
    #[serde(default)]
    pub turn_detection: bool,

    /// Server VAD speech-probability threshold (0.0-1.0). Only used when
    /// `turn_detection = true`. Default: 0.5.
    #[serde(default = "default_openai_realtime_vad_threshold")]
    pub vad_threshold: f32,

    /// Server VAD: milliseconds of audio to include before detected speech
    /// start. Only used when `turn_detection = true`. Default: 300.
    #[serde(default = "default_openai_realtime_vad_prefix_padding_ms")]
    pub vad_prefix_padding_ms: u32,

    /// Server VAD: milliseconds of trailing silence required to end a turn.
    /// Only used when `turn_detection = true`. Default: 550.
    #[serde(default = "default_openai_realtime_vad_silence_duration_ms")]
    pub vad_silence_duration_ms: u32,

    /// Streaming mode. true = live WebSocket session with progressive
    /// partials/finals (requires [hotkey] mode = "toggle"; PTT auto-promoted
    /// to toggle). false = batch mode: buffer audio while held, send a
    /// one-shot WebSocket session on release (PTT-compatible).
    #[serde(default = "default_true")]
    pub streaming: bool,

    /// Type deltas at the cursor as they arrive (streaming mode only).
    /// false = only finalized (`...completed`) segments are typed.
    /// Default: true.
    #[serde(default = "default_true")]
    pub type_partials: bool,
}

fn default_openai_realtime_model() -> String {
    "gpt-live-transcribe".to_string()
}

fn default_openai_realtime_delay() -> String {
    "low".to_string()
}

fn default_openai_realtime_languages() -> Vec<String> {
    vec!["en".to_string()]
}

fn default_openai_realtime_noise_reduction() -> String {
    "near_field".to_string()
}

fn default_openai_realtime_vad_threshold() -> f32 {
    0.5
}

fn default_openai_realtime_vad_prefix_padding_ms() -> u32 {
    300
}

fn default_openai_realtime_vad_silence_duration_ms() -> u32 {
    550
}

impl Default for OpenaiRealtimeConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            api_key_file: None,
            model: default_openai_realtime_model(),
            delay: default_openai_realtime_delay(),
            prompt: None,
            keywords: Vec::new(),
            languages: default_openai_realtime_languages(),
            noise_reduction: default_openai_realtime_noise_reduction(),
            turn_detection: false,
            vad_threshold: default_openai_realtime_vad_threshold(),
            vad_prefix_padding_ms: default_openai_realtime_vad_prefix_padding_ms(),
            vad_silence_duration_ms: default_openai_realtime_vad_silence_duration_ms(),
            streaming: true,
            type_partials: true,
        }
    }
}
