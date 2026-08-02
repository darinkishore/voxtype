//! OpenAI Realtime cloud streaming WebSocket STT backend (GA transcription
//! API).
//!
//! Connects to `wss://api.openai.com/v1/realtime?model=<model>` and pipes
//! 24 kHz mono `audio/pcm` (s16le) frames over WebSocket as base64-encoded
//! `input_audio_buffer.append` messages, receiving JSON transcription
//! events keyed by `item_id`. Implements both:
//!
//! - [`Transcriber::transcribe`] — batch path used when
//!   `[openai_realtime] streaming = false` (push-to-talk compatible).
//!   Buffers the audio, opens a one-shot WS session, sends the whole
//!   buffer, ends the turn (trailing silence with server VAD, or an
//!   explicit commit without it), and returns the first item's transcript.
//!
//! - [`StreamingTranscriber::start_stream`] — live streaming session.
//!   Exposed only when `[openai_realtime] streaming = true` (the default).
//!   Mirrors `soniox.rs`'s streaming shape: the daemon's
//!   `streaming_active()` gate auto-promotes push-to-talk to toggle when
//!   this is the active engine.
//!
//! ## Protocol notes (GA, 2026-08)
//!
//! This is the **GA** transcription API — `?model=<model>` in the
//! connect URL (not the pre-GA `?intent=transcription`), and the nested
//! `session.audio.input.{format,noise_reduction,transcription,turn_detection}`
//! `session.update` shape. `session.audio.input.format.rate` accepts only
//! `24000`; `languages` is a plural array field (there is no singular
//! `language` field in this API version).
//!
//! Voxtype must wait for `session.updated` before streaming any audio, and
//! treat an `error` event that arrives before then as fatal (surfaced
//! verbatim in the daemon log) — see [`run_streaming_session`].
//!
//! ## Turn handling
//!
//! With server VAD enabled (`[openai_realtime] turn_detection = true`,
//! the default), the server finalizes turns itself as the user speaks —
//! progressive per-utterance finals are typed while dictating, mirroring
//! Soniox's `is_final` semantics: `...transcription.delta` events are
//! non-final partials, `...transcription.completed` is the canonical final
//! that **replaces** the accumulated partial for that item (not merely
//! extends it — see [`Reconciler::process_completed`]).
//!
//! Because completion order across turns is not guaranteed, reconciliation
//! is keyed **per `item_id`** (a `HashMap`), never a single global
//! "currently typed" string like Soniox's reconciler uses. See
//! [`Reconciler`].
//!
//! On record stop: with VAD enabled, ~700 ms of zero-valued PCM samples is
//! sent (nudges the server to finalize a turn ending exactly at stop),
//! then events are drained for a bounded ~3 s. With VAD disabled, an
//! explicit `input_audio_buffer.commit` is sent instead, then the same
//! bounded drain.
//!
//! ## Sample rate
//!
//! `crate::transcribe::streaming::StreamingTranscriber` documents its
//! `samples_rx` contract as 16 kHz mono f32 (matching
//! `crate::audio::AudioCapture`'s output — the same assumption `soniox.rs`
//! makes). OpenAI Realtime's `audio/pcm` format accepts **only 24 kHz**.
//! This backend resamples every incoming chunk 16 kHz → 24 kHz (linear
//! interpolation, mirroring `crate::audio::cpal_capture`'s resampler)
//! before encoding to PCM16 — never mislabels 16 kHz audio as 24 kHz.
//!
//! ## Errors
//!
//! Connect timeouts, WS errors, and OpenAI `error` events surface as
//! `StreamingEvent::Error` followed by `Ended`. The daemon disowns the
//! session on `Error`/`Ended` so post-stop emissions are dropped (matches
//! the v0.7.2 disown-on-stop fix, same as Soniox).

use super::streaming::{SegmentId, StreamHandle, StreamingEvent, StreamingTranscriber};
use super::Transcriber;
use crate::config::OpenaiRealtimeConfig;
use crate::error::TranscribeError;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

/// Sample rate `samples_rx` delivers, per
/// `StreamingTranscriber::start_stream`'s documented contract (matches
/// `crate::audio::AudioCapture`'s output). Same assumption `soniox.rs`
/// makes for its own `SAMPLE_RATE` constant.
const SOURCE_SAMPLE_RATE: u32 = 16_000;

/// The only sample rate OpenAI Realtime's `audio/pcm` format accepts.
const TARGET_SAMPLE_RATE: u32 = 24_000;

/// Outgoing audio is coalesced into ~80 ms chunks before base64 + append,
/// per OpenAI's recommended chunking (40-100 ms; 80 ms is their example).
/// 24000 Hz * 2 bytes/sample * 0.08 s = 3840 bytes.
const CHUNK_BYTES: usize = 3840;

/// WS connect / session-update handshake timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for `session.updated` after sending `session.update`.
const SESSION_UPDATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded drain after signalling end-of-turn at record stop. OpenAI's
/// protocol has no `finished:true`-equivalent terminal signal (unlike
/// Soniox), so this timeout is the actual terminal condition on stop.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Trailing silence sent (when server VAD is enabled) to nudge the server
/// into finalizing a turn ending exactly at record-stop.
const TRAILING_SILENCE_MS: u32 = 700;

/// Batch (non-streaming) path timeout: wait this long for a `completed` (or
/// `failed`) event after ending the turn before giving up.
const BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Batch path: how many 16 kHz source samples to resample+encode per
/// outgoing `input_audio_buffer.append` (100 ms @ 16 kHz). Streaming uses
/// [`CHUNK_BYTES`]-exact coalescing instead; batch mode just needs
/// reasonably sized frames, not exact 80 ms alignment.
const BATCH_INPUT_CHUNK_SAMPLES: usize = 1_600;

#[derive(Debug)]
pub struct OpenaiRealtimeTranscriber {
    config: OpenaiRealtimeConfig,
    api_key: String,
}

/// Resolve the API key: `config.api_key` > `config.api_key_file` (read +
/// trimmed) > `OPENAI_API_KEY` env var. An explicitly-configured but
/// unreadable/empty `api_key_file` is a hard error rather than a silent
/// fall-through to the env var — mirrors `soniox.rs`'s `terms_file`
/// handling (an explicit misconfigured path should fail loudly, not
/// silently degrade).
fn resolve_api_key(config: &OpenaiRealtimeConfig) -> Result<String, TranscribeError> {
    if let Some(key) = &config.api_key {
        if !key.trim().is_empty() {
            return Ok(key.clone());
        }
    }
    if let Some(path) = &config.api_key_file {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            TranscribeError::ConfigError(format!(
                "OpenAI Realtime api_key_file unreadable ({}): {}",
                path.display(),
                e
            ))
        })?;
        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return Err(TranscribeError::ConfigError(format!(
                "OpenAI Realtime api_key_file ({}) is empty",
                path.display()
            )));
        }
        return Ok(trimmed.to_string());
    }
    if let Ok(key) = std::env::var("OPENAI_API_KEY") {
        if !key.trim().is_empty() {
            return Ok(key);
        }
    }
    Err(TranscribeError::ConfigError(
        "OpenAI Realtime API key required: set [openai_realtime] api_key, api_key_file, \
         or OPENAI_API_KEY"
            .into(),
    ))
}

/// Reject keywords containing characters OpenAI's session.update rejects
/// (`<`, `>`, newlines) so misconfiguration fails at startup rather than as
/// an opaque server-side `error` event mid-session.
fn validate_keywords(keywords: &[String]) -> Result<(), TranscribeError> {
    for kw in keywords {
        if kw.contains('<') || kw.contains('>') || kw.contains('\n') || kw.contains('\r') {
            return Err(TranscribeError::ConfigError(format!(
                "OpenAI Realtime keyword {:?} contains an unsupported character \
                 (<, >, or newline are rejected by the API)",
                kw
            )));
        }
    }
    Ok(())
}

impl OpenaiRealtimeTranscriber {
    pub fn new(config: OpenaiRealtimeConfig) -> Result<Self, TranscribeError> {
        let api_key = resolve_api_key(&config)?;
        validate_keywords(&config.keywords)?;

        tracing::info!(
            "OpenAI Realtime backend configured: model={}, delay={}, languages={:?}, \
             streaming={}, turn_detection={}",
            config.model,
            config.delay,
            config.languages,
            config.streaming,
            config.turn_detection,
        );

        Ok(Self { config, api_key })
    }

    /// Build the `session.update` payload (GA nested `audio.input.*`
    /// shape). `turn_detection` is `null` when server VAD is disabled.
    fn session_update(&self) -> serde_json::Value {
        let mut transcription = serde_json::json!({
            "model": self.config.model,
            "delay": self.config.delay,
            "languages": self.config.languages,
        });
        if let Some(prompt) = self.config.prompt.as_ref().filter(|p| !p.trim().is_empty()) {
            transcription["prompt"] = serde_json::Value::String(prompt.clone());
        }
        if !self.config.keywords.is_empty() {
            transcription["keywords"] = serde_json::json!(self.config.keywords);
        }

        let mut input = serde_json::json!({
            "format": { "type": "audio/pcm", "rate": TARGET_SAMPLE_RATE },
            "transcription": transcription,
        });
        if !self.config.noise_reduction.trim().is_empty() {
            input["noise_reduction"] = serde_json::json!({ "type": self.config.noise_reduction });
        }
        input["turn_detection"] = if self.config.turn_detection {
            serde_json::json!({
                "type": "server_vad",
                "threshold": self.config.vad_threshold,
                "prefix_padding_ms": self.config.vad_prefix_padding_ms,
                "silence_duration_ms": self.config.vad_silence_duration_ms,
            })
        } else {
            serde_json::Value::Null
        };

        serde_json::json!({
            "type": "session.update",
            "session": {
                "type": "transcription",
                "audio": { "input": input },
            },
        })
    }

    fn ws_url(&self) -> String {
        format!(
            "wss://api.openai.com/v1/realtime?model={}",
            self.config.model
        )
    }

    fn connect_request(
        &self,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, TranscribeError> {
        let mut request = self.ws_url().into_client_request().map_err(|e| {
            TranscribeError::InferenceFailed(format!(
                "OpenAI Realtime: failed to build connect request: {}",
                e
            ))
        })?;
        let auth = HeaderValue::from_str(&format!("Bearer {}", self.api_key)).map_err(|e| {
            TranscribeError::InferenceFailed(format!(
                "OpenAI Realtime: invalid API key header value: {}",
                e
            ))
        })?;
        request.headers_mut().insert("Authorization", auth);
        Ok(request)
    }
}

// === Sample conversion (mirrors soniox.rs's helpers; duplicated locally —
// both are small private free functions, not worth a shared module for two
// call sites with different target rates) ===

fn f32_to_i16(s: f32) -> i16 {
    (s.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16
}

fn f32_to_s16le_bytes(samples: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for &s in samples {
        out.extend_from_slice(&f32_to_i16(s).to_le_bytes());
    }
    out
}

/// Linear interpolation resampling. Mirrors
/// `crate::audio::cpal_capture::resample` (private to that module, so
/// duplicated here rather than exposed cross-module for one caller).
fn resample(samples: &[f32], from_rate: u32, to_rate: u32) -> Vec<f32> {
    if from_rate == to_rate || samples.is_empty() {
        return samples.to_vec();
    }

    let ratio = to_rate as f64 / from_rate as f64;
    let new_len = (samples.len() as f64 * ratio).ceil() as usize;
    let mut output = Vec::with_capacity(new_len);

    for i in 0..new_len {
        let src_idx = i as f64 / ratio;
        let idx = src_idx.floor() as usize;
        let frac = (src_idx - idx as f64) as f32;

        let sample = if idx + 1 < samples.len() {
            samples[idx] * (1.0 - frac) + samples[idx + 1] * frac
        } else {
            samples.get(idx).copied().unwrap_or(0.0)
        };

        output.push(sample);
    }

    output
}

/// Resample a 16 kHz mono chunk to 24 kHz and encode as PCM16 LE bytes.
fn encode_chunk(samples: &[f32]) -> Vec<u8> {
    let resampled = resample(samples, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE);
    f32_to_s16le_bytes(&resampled)
}

/// Count the number of Unicode scalars shared as a prefix between `a` and
/// `b`. Mirrors `soniox.rs::common_prefix_char_count`.
fn common_prefix_char_count(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

// === Reconciler: per-item_id delta/completed -> StreamingEvent ===

#[derive(Debug, Default, Clone)]
struct ItemState {
    segment_id: SegmentId,
    /// Text already emitted as `Partial` (and thus typed at the cursor as
    /// a tentative tail) for this item.
    typed_partial: String,
}

/// State machine that converts OpenAI Realtime transcription events into
/// voxtype `StreamingEvent`s, keyed per `item_id` — completion order across
/// turns is not guaranteed by the protocol, so (unlike `soniox.rs`'s single
/// `typed_partial` string) this tracks one `ItemState` per item.
#[derive(Debug, Default)]
struct Reconciler {
    items: HashMap<String, ItemState>,
    next_segment_id: SegmentId,
}

impl Reconciler {
    fn alloc_segment_id(&mut self) -> SegmentId {
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        id
    }

    /// `delta` is already an incremental fragment (not cumulative) per the
    /// OpenAI protocol, so — like Soniox's per-token deltas and Parakeet's
    /// chunk deltas — it is forwarded directly as a `Partial` event's text.
    fn process_delta(
        &mut self,
        item_id: &str,
        delta: &str,
        type_partials: bool,
    ) -> Option<StreamingEvent> {
        if delta.is_empty() {
            return None;
        }
        let segment_id = match self.items.get(item_id) {
            Some(state) => state.segment_id,
            None => {
                let id = self.alloc_segment_id();
                self.items.insert(
                    item_id.to_string(),
                    ItemState {
                        segment_id: id,
                        typed_partial: String::new(),
                    },
                );
                id
            }
        };
        if type_partials {
            if let Some(state) = self.items.get_mut(item_id) {
                state.typed_partial.push_str(delta);
            }
            Some(StreamingEvent::Partial {
                text: delta.to_string(),
                segment_id,
            })
        } else {
            None
        }
    }

    /// The final `transcript` is canonical and REPLACES whatever's been
    /// typed for this item so far — not merely extends it. Diffs against
    /// `typed_partial` the same way `soniox.rs`'s reconciler diffs a
    /// diverging final: common-prefix, backspace the tail that doesn't
    /// match, type the rest. When `typed_partial` is a prefix of the final
    /// (the common case with `type_partials = true`, and always true when
    /// `type_partials = false` since `typed_partial` stays empty), this
    /// reduces to a plain `Final` with just the new tail.
    fn process_completed(&mut self, item_id: &str, transcript: &str) -> Option<StreamingEvent> {
        let (segment_id, typed_partial) = match self.items.remove(item_id) {
            Some(state) => (state.segment_id, state.typed_partial),
            None => (self.alloc_segment_id(), String::new()),
        };

        if transcript.starts_with(&typed_partial) {
            let tail = &transcript[typed_partial.len()..];
            if tail.is_empty() {
                None
            } else {
                Some(StreamingEvent::Final {
                    text: tail.to_string(),
                    segment_id,
                })
            }
        } else {
            let lcp_chars = common_prefix_char_count(&typed_partial, transcript);
            let backspace = typed_partial.chars().count() - lcp_chars;
            let tail: String = transcript.chars().skip(lcp_chars).collect();
            tracing::debug!(
                "OpenAI Realtime tail revision (item {}): backspace {} chars, type {:?}",
                item_id,
                backspace,
                tail,
            );
            Some(StreamingEvent::Replace {
                backspace,
                text: tail,
                segment_id,
            })
        }
    }

    /// `...transcription.failed` — drop that item's partial. If any of it
    /// was already typed at the cursor, erase it with a pure-backspace
    /// `Replace` (empty replacement text) so the cursor doesn't show a
    /// half-finished utterance that will never be completed.
    fn process_failed(&mut self, item_id: &str) -> Option<StreamingEvent> {
        let state = self.items.remove(item_id)?;
        if state.typed_partial.is_empty() {
            return None;
        }
        Some(StreamingEvent::Replace {
            backspace: state.typed_partial.chars().count(),
            text: String::new(),
            segment_id: state.segment_id,
        })
    }
}

impl Transcriber for OpenaiRealtimeTranscriber {
    /// Run a one-shot (batch) transcription over the same realtime
    /// WebSocket protocol. See the module doc's "Runtime requirement" note
    /// on `SonioxTranscriber::transcribe` — the same sync→async bridge
    /// caveat applies here (multi-threaded tokio runtime required when
    /// called from inside an existing one).
    fn transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        if samples.is_empty() {
            return Err(TranscribeError::AudioFormat("Empty audio buffer".into()));
        }
        let run = self.batch_transcribe(samples);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(run)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        TranscribeError::InferenceFailed(format!("Failed to create runtime: {}", e))
                    })?;
                rt.block_on(run)
            }
        }
    }

    fn as_streaming(&self) -> Option<&dyn StreamingTranscriber> {
        self.config.streaming.then_some(self as _)
    }
}

impl OpenaiRealtimeTranscriber {
    async fn batch_transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        let request = self.connect_request()?;
        let (ws_stream, _) =
            tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .map_err(|_| {
                    TranscribeError::InferenceFailed("OpenAI Realtime: connect timeout".into())
                })?
                .map_err(|e| {
                    TranscribeError::InferenceFailed(format!(
                        "OpenAI Realtime: WS connect failed: {}",
                        e
                    ))
                })?;

        let (mut write, mut read) = ws_stream.split();

        write
            .send(Message::Text(self.session_update().to_string()))
            .await
            .map_err(|e| {
                TranscribeError::InferenceFailed(format!(
                    "OpenAI Realtime: send session.update failed: {}",
                    e
                ))
            })?;

        wait_for_session_updated(&mut read).await?;

        for chunk in samples.chunks(BATCH_INPUT_CHUNK_SAMPLES) {
            let bytes = encode_chunk(chunk);
            send_append(&mut write, &bytes).await?;
        }

        if self.config.turn_detection {
            let silence = vec![0.0_f32; (TARGET_SAMPLE_RATE * TRAILING_SILENCE_MS / 1000) as usize];
            let bytes = f32_to_s16le_bytes(&silence);
            send_append(&mut write, &bytes).await?;
        } else {
            write
                .send(Message::Text(
                    r#"{"type":"input_audio_buffer.commit"}"#.to_string(),
                ))
                .await
                .map_err(|e| {
                    TranscribeError::InferenceFailed(format!(
                        "OpenAI Realtime: send commit failed: {}",
                        e
                    ))
                })?;
        }

        let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(TranscribeError::InferenceFailed(
                    "OpenAI Realtime: batch timeout".into(),
                ));
            }
            let msg = match tokio::time::timeout(remaining, read.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    return Err(TranscribeError::InferenceFailed(format!(
                        "OpenAI Realtime: WS error: {}",
                        e
                    )))
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(TranscribeError::InferenceFailed(
                        "OpenAI Realtime: batch timeout".into(),
                    ))
                }
            };
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Ping(payload) => {
                    let _ = write.send(Message::Pong(payload)).await;
                    continue;
                }
                Message::Close(_) => break,
                _ => continue,
            };
            let parsed: serde_json::Value = match serde_json::from_str(&text) {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!("OpenAI Realtime: unparseable message ({}): {}", e, text);
                    continue;
                }
            };
            match parsed.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "conversation.item.input_audio_transcription.completed" => {
                    let transcript = parsed
                        .get("transcript")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let _ = write.send(Message::Close(None)).await;
                    return Ok(transcript);
                }
                "conversation.item.input_audio_transcription.failed" => {
                    let _ = write.send(Message::Close(None)).await;
                    return Err(TranscribeError::InferenceFailed(
                        "OpenAI Realtime: transcription item failed".into(),
                    ));
                }
                "error" => {
                    let _ = write.send(Message::Close(None)).await;
                    return Err(TranscribeError::InferenceFailed(format!(
                        "OpenAI Realtime error: {}",
                        parsed.get("error").unwrap_or(&parsed)
                    )));
                }
                _ => continue,
            }
        }

        Ok(String::new())
    }
}

/// Wait for `session.updated` after sending `session.update`. Any `error`
/// event before then is fatal and surfaced verbatim (the raw JSON value —
/// there's no fixed schema documented for OpenAI's error payloads worth
/// hand-parsing).
async fn wait_for_session_updated(
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
) -> Result<(), TranscribeError> {
    let deadline = tokio::time::Instant::now() + SESSION_UPDATE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(TranscribeError::InferenceFailed(
                "OpenAI Realtime: timed out waiting for session.updated".into(),
            ));
        }
        let msg = match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                return Err(TranscribeError::InferenceFailed(format!(
                    "OpenAI Realtime: WS error while awaiting session.updated: {}",
                    e
                )))
            }
            Ok(None) | Err(_) => {
                return Err(TranscribeError::InferenceFailed(
                    "OpenAI Realtime: connection closed/timed out awaiting session.updated".into(),
                ))
            }
        };
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => {
                return Err(TranscribeError::InferenceFailed(
                    "OpenAI Realtime: connection closed awaiting session.updated".into(),
                ))
            }
            _ => continue,
        };
        let parsed: serde_json::Value = match serde_json::from_str(&text) {
            Ok(p) => p,
            Err(_) => continue,
        };
        match parsed.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "session.updated" => return Ok(()),
            "error" => {
                return Err(TranscribeError::InferenceFailed(format!(
                    "OpenAI Realtime: fatal error during session configuration: {}",
                    parsed.get("error").unwrap_or(&parsed)
                )))
            }
            _ => continue, // e.g. session.created — ignore and keep waiting
        }
    }
}

async fn send_append(
    write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    bytes: &[u8],
) -> Result<(), TranscribeError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let frame = serde_json::json!({
        "type": "input_audio_buffer.append",
        "audio": BASE64.encode(bytes),
    })
    .to_string();
    write.send(Message::Text(frame)).await.map_err(|e| {
        TranscribeError::InferenceFailed(format!("OpenAI Realtime: send audio failed: {}", e))
    })
}

impl StreamingTranscriber for OpenaiRealtimeTranscriber {
    fn start_stream(
        &self,
        samples_rx: mpsc::Receiver<Vec<f32>>,
    ) -> Result<StreamHandle, TranscribeError> {
        let (events_tx, events_rx) = mpsc::channel::<StreamingEvent>(64);
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let request = self.connect_request()?;
        let session_update = self.session_update().to_string();
        let type_partials = self.config.type_partials;
        let turn_detection = self.config.turn_detection;

        let task = tokio::spawn(async move {
            run_streaming_session(
                request,
                session_update,
                turn_detection,
                type_partials,
                samples_rx,
                events_tx,
                cancel_rx,
            )
            .await
        });

        Ok(StreamHandle {
            events: events_rx,
            cancel: cancel_tx,
            task,
        })
    }
}

/// Emit an `Error` followed by `Ended` so the daemon surfaces a
/// notification and cleanly resets to idle. Mirrors `soniox.rs::send_fatal`.
async fn send_fatal(events_tx: &mpsc::Sender<StreamingEvent>, msg: String) {
    tracing::error!("{}", msg);
    let _ = events_tx
        .send(StreamingEvent::Error(TranscribeError::InferenceFailed(msg)))
        .await;
    let _ = events_tx.send(StreamingEvent::Ended).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_streaming_session(
    request: tokio_tungstenite::tungstenite::http::Request<()>,
    session_update: String,
    turn_detection: bool,
    type_partials: bool,
    mut samples_rx: mpsc::Receiver<Vec<f32>>,
    events_tx: mpsc::Sender<StreamingEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<(), TranscribeError> {
    let ws_result =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request)).await;
    let ws_stream = match ws_result {
        Ok(Ok((s, _))) => s,
        Ok(Err(e)) => {
            send_fatal(
                &events_tx,
                format!("OpenAI Realtime: WS connect failed: {}", e),
            )
            .await;
            return Ok(());
        }
        Err(_) => {
            send_fatal(&events_tx, "OpenAI Realtime: connect timeout".into()).await;
            return Ok(());
        }
    };
    let (mut write, mut read) = ws_stream.split();

    tracing::debug!(target: "voxtype::openai_realtime::wire", "-> session.update {}", session_update);
    if let Err(e) = write.send(Message::Text(session_update)).await {
        send_fatal(
            &events_tx,
            format!("OpenAI Realtime: send session.update failed: {}", e),
        )
        .await;
        return Ok(());
    }

    if let Err(e) = wait_for_session_updated(&mut read).await {
        send_fatal(&events_tx, e.to_string()).await;
        return Ok(());
    }
    tracing::debug!("OpenAI Realtime: session.updated received, streaming audio");

    let mut reconciler = Reconciler::default();
    let mut pending: Vec<u8> = Vec::with_capacity(CHUNK_BYTES * 2);
    let mut samples_closed = false;
    let mut sent_stop_sequence = false;
    let mut drain_deadline: Option<tokio::time::Instant> = None;

    loop {
        let drain_timer = async {
            match drain_deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;

            // Highest priority: cancel signal from daemon.
            _ = &mut cancel_rx => {
                tracing::debug!("OpenAI Realtime streaming session cancelled");
                break;
            }

            // Bounded drain after end-of-turn. OpenAI has no
            // finished:true-equivalent terminal event, so this timeout is
            // the actual stop condition — trailing deltas/completions
            // received after it fires are lost by design (documented
            // ~3s budget from the protocol brief).
            _ = drain_timer, if drain_deadline.is_some() => {
                tracing::debug!(
                    "OpenAI Realtime drain window ({}s) elapsed after end-of-turn",
                    DRAIN_TIMEOUT.as_secs(),
                );
                break;
            }

            // Outgoing audio frames, coalesced to CHUNK_BYTES before send.
            chunk = samples_rx.recv(), if !samples_closed => {
                match chunk {
                    Some(c) if !c.is_empty() => {
                        let bytes = encode_chunk(&c);
                        pending.extend_from_slice(&bytes);
                        while pending.len() >= CHUNK_BYTES {
                            let frame_bytes: Vec<u8> = pending.drain(..CHUNK_BYTES).collect();
                            if let Err(e) = send_append(&mut write, &frame_bytes).await {
                                let _ = events_tx.send(StreamingEvent::Error(e)).await;
                                break;
                            }
                        }
                    }
                    Some(_) => { /* empty chunk, skip */ }
                    None => {
                        samples_closed = true;
                        if !sent_stop_sequence {
                            // Flush whatever's left in the coalescing buffer.
                            if !pending.is_empty() {
                                let tail = std::mem::take(&mut pending);
                                if let Err(e) = send_append(&mut write, &tail).await {
                                    tracing::warn!("OpenAI Realtime: flush-on-stop send failed: {}", e);
                                }
                            }
                            if turn_detection {
                                // Trailing silence nudges server VAD to finalize
                                // the turn ending exactly at record-stop.
                                let silence = vec![
                                    0.0_f32;
                                    (TARGET_SAMPLE_RATE * TRAILING_SILENCE_MS / 1000) as usize
                                ];
                                let bytes = f32_to_s16le_bytes(&silence);
                                if let Err(e) = send_append(&mut write, &bytes).await {
                                    tracing::warn!("OpenAI Realtime: trailing-silence send failed: {}", e);
                                }
                            } else {
                                let commit = r#"{"type":"input_audio_buffer.commit"}"#;
                                if let Err(e) = write.send(Message::Text(commit.to_string())).await {
                                    tracing::warn!("OpenAI Realtime: commit send failed: {}", e);
                                }
                            }
                            sent_stop_sequence = true;
                            drain_deadline = Some(tokio::time::Instant::now() + DRAIN_TIMEOUT);
                            tracing::debug!(
                                "OpenAI Realtime: end-of-turn signalled (turn_detection={}); draining (timeout {}s)",
                                turn_detection,
                                DRAIN_TIMEOUT.as_secs(),
                            );
                        }
                    }
                }
            }

            // Incoming server messages.
            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        let _ = events_tx.send(StreamingEvent::Error(
                            TranscribeError::InferenceFailed(format!("OpenAI Realtime: WS error: {}", e))
                        )).await;
                        break;
                    }
                    None => break,
                };
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Ping(payload) => {
                        let _ = write.send(Message::Pong(payload)).await;
                        continue;
                    }
                    Message::Close(_) => break,
                    _ => continue,
                };
                tracing::debug!(target: "voxtype::openai_realtime::wire", "<- {}", text);
                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("OpenAI Realtime: unparseable message ({}): {}", e, text);
                        continue;
                    }
                };

                let event_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let item_id = parsed.get("item_id").and_then(|v| v.as_str()).unwrap_or("");

                let event = match event_type {
                    "conversation.item.input_audio_transcription.delta" => {
                        let delta = parsed.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                        reconciler.process_delta(item_id, delta, type_partials)
                    }
                    "conversation.item.input_audio_transcription.completed" => {
                        let transcript = parsed.get("transcript").and_then(|v| v.as_str()).unwrap_or("");
                        reconciler.process_completed(item_id, transcript)
                    }
                    "conversation.item.input_audio_transcription.failed" => {
                        tracing::warn!("OpenAI Realtime: item {} failed", item_id);
                        reconciler.process_failed(item_id)
                    }
                    "error" => {
                        // Fatal: mirrors soniox.rs's error_message handling.
                        // Log verbatim (raw JSON value) — no fixed schema
                        // worth hand-parsing.
                        let err = parsed.get("error").unwrap_or(&parsed);
                        let _ = events_tx.send(StreamingEvent::Error(
                            TranscribeError::InferenceFailed(format!("OpenAI Realtime error: {}", err))
                        )).await;
                        break;
                    }
                    // session.created, input_audio_buffer.speech_started/
                    // speech_stopped, response.*, etc. — informational only.
                    _ => None,
                };

                if let Some(ev) = event {
                    if events_tx.send(ev).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    let _ = write.send(Message::Close(None)).await;
    let _ = events_tx.send(StreamingEvent::Ended).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_key(key: Option<&str>) -> OpenaiRealtimeConfig {
        OpenaiRealtimeConfig {
            api_key: key.map(|s| s.to_string()),
            api_key_file: None,
            model: "gpt-live-transcribe".into(),
            delay: "low".into(),
            prompt: None,
            keywords: Vec::new(),
            languages: vec!["en".into()],
            noise_reduction: "near_field".into(),
            turn_detection: true,
            vad_threshold: 0.5,
            vad_prefix_padding_ms: 300,
            vad_silence_duration_ms: 550,
            streaming: true,
            type_partials: true,
        }
    }

    // === API key resolution ===

    #[test]
    fn requires_api_key_from_config_file_or_env() {
        std::env::remove_var("OPENAI_API_KEY");
        let err = OpenaiRealtimeTranscriber::new(cfg_with_key(None)).unwrap_err();
        assert!(matches!(err, TranscribeError::ConfigError(_)));
    }

    #[test]
    fn accepts_config_api_key() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("test-key"))).unwrap();
        assert_eq!(t.api_key, "test-key");
    }

    #[test]
    fn accepts_api_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.txt");
        std::fs::write(&path, "file-key\n").unwrap();
        let mut cfg = cfg_with_key(None);
        cfg.api_key_file = Some(path);
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        assert_eq!(t.api_key, "file-key");
    }

    #[test]
    fn config_api_key_takes_priority_over_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.txt");
        std::fs::write(&path, "file-key").unwrap();
        let mut cfg = cfg_with_key(Some("config-key"));
        cfg.api_key_file = Some(path);
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        assert_eq!(t.api_key, "config-key");
    }

    #[test]
    fn unreadable_api_key_file_is_a_hard_error() {
        let mut cfg = cfg_with_key(None);
        cfg.api_key_file = Some("/nonexistent/path/key.txt".into());
        let err = OpenaiRealtimeTranscriber::new(cfg).unwrap_err();
        assert!(matches!(err, TranscribeError::ConfigError(_)));
    }

    // === keyword validation ===

    #[test]
    fn rejects_keyword_with_angle_bracket() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.keywords = vec!["<script>".into()];
        let err = OpenaiRealtimeTranscriber::new(cfg).unwrap_err();
        assert!(matches!(err, TranscribeError::ConfigError(_)));
    }

    #[test]
    fn rejects_keyword_with_newline() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.keywords = vec!["multi\nline".into()];
        let err = OpenaiRealtimeTranscriber::new(cfg).unwrap_err();
        assert!(matches!(err, TranscribeError::ConfigError(_)));
    }

    #[test]
    fn accepts_plain_keywords() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.keywords = vec!["Claude".into(), "voxtype".into()];
        assert!(OpenaiRealtimeTranscriber::new(cfg).is_ok());
    }

    // === session.update shape ===

    #[test]
    fn session_update_contains_required_fields() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let payload = t.session_update();
        assert_eq!(payload["type"], "session.update");
        assert_eq!(payload["session"]["type"], "transcription");
        let input = &payload["session"]["audio"]["input"];
        assert_eq!(input["format"]["type"], "audio/pcm");
        assert_eq!(input["format"]["rate"], 24000);
        assert_eq!(input["transcription"]["model"], "gpt-live-transcribe");
        assert_eq!(input["transcription"]["delay"], "low");
        assert_eq!(input["transcription"]["languages"][0], "en");
        assert!(input["transcription"].get("language").is_none());
    }

    #[test]
    fn session_update_omits_prompt_when_unset() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let payload = t.session_update();
        assert!(payload["session"]["audio"]["input"]["transcription"]
            .get("prompt")
            .is_none());
    }

    #[test]
    fn session_update_includes_prompt_when_set() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.prompt = Some("bias toward Rust jargon".into());
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        let payload = t.session_update();
        assert_eq!(
            payload["session"]["audio"]["input"]["transcription"]["prompt"],
            "bias toward Rust jargon"
        );
    }

    #[test]
    fn session_update_omits_keywords_when_empty() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let payload = t.session_update();
        assert!(payload["session"]["audio"]["input"]["transcription"]
            .get("keywords")
            .is_none());
    }

    #[test]
    fn session_update_includes_keywords_when_set() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.keywords = vec!["Claude".into(), "Hyprland".into()];
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        let payload = t.session_update();
        let keywords = payload["session"]["audio"]["input"]["transcription"]["keywords"]
            .as_array()
            .unwrap();
        assert_eq!(keywords.len(), 2);
        assert_eq!(keywords[0], "Claude");
    }

    #[test]
    fn session_update_omits_noise_reduction_when_empty_string() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.noise_reduction = String::new();
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        let payload = t.session_update();
        assert!(payload["session"]["audio"]["input"]
            .get("noise_reduction")
            .is_none());
    }

    #[test]
    fn session_update_includes_noise_reduction_when_set() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let payload = t.session_update();
        assert_eq!(
            payload["session"]["audio"]["input"]["noise_reduction"]["type"],
            "near_field"
        );
    }

    #[test]
    fn session_update_turn_detection_server_vad_when_enabled() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let payload = t.session_update();
        let td = &payload["session"]["audio"]["input"]["turn_detection"];
        assert_eq!(td["type"], "server_vad");
        assert_eq!(td["threshold"], 0.5);
        assert_eq!(td["prefix_padding_ms"], 300);
        assert_eq!(td["silence_duration_ms"], 550);
    }

    #[test]
    fn session_update_turn_detection_null_when_disabled() {
        let mut cfg = cfg_with_key(Some("k"));
        cfg.turn_detection = false;
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        let payload = t.session_update();
        assert!(payload["session"]["audio"]["input"]["turn_detection"].is_null());
    }

    #[test]
    fn ws_url_uses_model_query_param_not_intent() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        let url = t.ws_url();
        assert_eq!(
            url,
            "wss://api.openai.com/v1/realtime?model=gpt-live-transcribe"
        );
        assert!(!url.contains("intent"));
    }

    // === streaming gate ===

    #[test]
    fn as_streaming_respects_streaming_flag() {
        let t = OpenaiRealtimeTranscriber::new(cfg_with_key(Some("k"))).unwrap();
        assert!(t.as_streaming().is_some());

        let mut cfg = cfg_with_key(Some("k"));
        cfg.streaming = false;
        let t = OpenaiRealtimeTranscriber::new(cfg).unwrap();
        assert!(t.as_streaming().is_none());
    }

    // === sample conversion ===

    #[test]
    fn f32_to_s16le_round_trip_endpoints() {
        let samples = vec![-1.0_f32, 0.0, 1.0];
        let bytes = f32_to_s16le_bytes(&samples);
        assert_eq!(bytes.len(), 6);
        let s0 = i16::from_le_bytes([bytes[0], bytes[1]]);
        let s1 = i16::from_le_bytes([bytes[2], bytes[3]]);
        let s2 = i16::from_le_bytes([bytes[4], bytes[5]]);
        assert!(s0 <= -32700);
        assert_eq!(s1, 0);
        assert!(s2 >= 32700);
    }

    #[test]
    fn f32_to_s16le_clamps_out_of_range() {
        let bytes = f32_to_s16le_bytes(&[-2.0_f32, 2.0]);
        let s0 = i16::from_le_bytes([bytes[0], bytes[1]]);
        let s1 = i16::from_le_bytes([bytes[2], bytes[3]]);
        assert!(s0 <= -32700);
        assert!(s1 >= 32700);
    }

    #[test]
    fn resample_16k_to_24k_upsamples() {
        let samples = vec![0.0_f32; 1600]; // 100ms @ 16kHz
        let result = resample(&samples, 16000, 24000);
        // 16000 -> 24000 is 2:3 ratio, so 1600 samples -> 2400 samples.
        assert_eq!(result.len(), 2400);
    }

    #[test]
    fn resample_same_rate_is_noop() {
        let samples = vec![1.0, 2.0, 3.0];
        assert_eq!(resample(&samples, 24000, 24000), samples);
    }

    #[test]
    fn resample_empty_is_empty() {
        let samples: Vec<f32> = vec![];
        assert!(resample(&samples, 16000, 24000).is_empty());
    }

    #[test]
    fn encode_chunk_produces_24khz_pcm16_byte_count() {
        // 100ms @ 16kHz = 1600 samples in -> 100ms @ 24kHz = 2400 samples
        // out -> 4800 bytes (s16le).
        let samples = vec![0.0_f32; 1600];
        let bytes = encode_chunk(&samples);
        assert_eq!(bytes.len(), 4800);
    }

    // === Reconciler ===

    #[test]
    fn delta_emits_partial_with_stable_segment_id() {
        let mut r = Reconciler::default();
        let ev = r.process_delta("item_1", "hel", true).unwrap();
        match ev {
            StreamingEvent::Partial { text, segment_id } => {
                assert_eq!(text, "hel");
                assert_eq!(segment_id, 0);
            }
            _ => panic!("expected Partial"),
        }
        let ev2 = r.process_delta("item_1", "lo", true).unwrap();
        match ev2 {
            StreamingEvent::Partial { text, segment_id } => {
                assert_eq!(text, "lo");
                assert_eq!(segment_id, 0, "same item_id must keep the same segment_id");
            }
            _ => panic!("expected Partial"),
        }
    }

    #[test]
    fn distinct_items_get_distinct_segment_ids() {
        let mut r = Reconciler::default();
        let ev1 = r.process_delta("item_1", "a", true).unwrap();
        let ev2 = r.process_delta("item_2", "b", true).unwrap();
        let id1 = match ev1 {
            StreamingEvent::Partial { segment_id, .. } => segment_id,
            _ => panic!(),
        };
        let id2 = match ev2 {
            StreamingEvent::Partial { segment_id, .. } => segment_id,
            _ => panic!(),
        };
        assert_ne!(id1, id2);
    }

    #[test]
    fn completed_after_deltas_matching_prefix_emits_final_tail() {
        let mut r = Reconciler::default();
        r.process_delta("item_1", "hello", true);
        let ev = r.process_completed("item_1", "hello world").unwrap();
        match ev {
            StreamingEvent::Final { text, .. } => assert_eq!(text, " world"),
            _ => panic!("expected Final"),
        }
    }

    #[test]
    fn completed_equal_to_typed_partial_emits_nothing() {
        let mut r = Reconciler::default();
        r.process_delta("item_1", "hello", true);
        let ev = r.process_completed("item_1", "hello");
        assert!(ev.is_none());
    }

    #[test]
    fn completed_diverging_from_typed_partial_emits_replace() {
        let mut r = Reconciler::default();
        r.process_delta("item_1", "hello", true);
        let ev = r.process_completed("item_1", "goodbye").unwrap();
        match ev {
            StreamingEvent::Replace {
                backspace, text, ..
            } => {
                assert_eq!(backspace, 5);
                assert_eq!(text, "goodbye");
            }
            _ => panic!("expected Replace"),
        }
    }

    #[test]
    fn completed_without_prior_deltas_emits_full_final() {
        let mut r = Reconciler::default();
        let ev = r.process_completed("item_1", "hello").unwrap();
        match ev {
            StreamingEvent::Final { text, .. } => assert_eq!(text, "hello"),
            _ => panic!("expected Final"),
        }
    }

    #[test]
    fn completed_removes_item_state() {
        let mut r = Reconciler::default();
        r.process_delta("item_1", "hi", true);
        r.process_completed("item_1", "hi");
        assert!(!r.items.contains_key("item_1"));
    }

    #[test]
    fn out_of_order_completion_across_items_uses_correct_state() {
        // Protocol note: completion order across turns is not guaranteed.
        let mut r = Reconciler::default();
        r.process_delta("item_1", "first", true);
        r.process_delta("item_2", "second", true);
        // item_2 completes before item_1.
        let ev2 = r
            .process_completed("item_2", "second")
            .unwrap_or(StreamingEvent::Final {
                text: String::new(),
                segment_id: 999,
            });
        // "second" == typed_partial exactly -> emits nothing per the
        // no-op-on-exact-match rule; assert the *other* item is untouched.
        let _ = ev2;
        assert!(r.items.contains_key("item_1"));
        assert!(!r.items.contains_key("item_2"));
        let ev1 = r.process_completed("item_1", "first thing").unwrap();
        match ev1 {
            StreamingEvent::Final { text, .. } => assert_eq!(text, " thing"),
            _ => panic!("expected Final"),
        }
    }

    #[test]
    fn failed_with_typed_partial_emits_backspace_replace() {
        let mut r = Reconciler::default();
        r.process_delta("item_1", "hel", true);
        r.process_delta("item_1", "lo", true);
        let ev = r.process_failed("item_1").unwrap();
        match ev {
            StreamingEvent::Replace {
                backspace, text, ..
            } => {
                assert_eq!(backspace, 5);
                assert_eq!(text, "");
            }
            _ => panic!("expected Replace"),
        }
        assert!(!r.items.contains_key("item_1"));
    }

    #[test]
    fn failed_without_typed_partial_emits_nothing() {
        let mut r = Reconciler::default();
        // type_partials=false: item is tracked but nothing accumulated.
        r.process_delta("item_1", "hel", false);
        assert!(r.process_failed("item_1").is_none());
    }

    #[test]
    fn failed_for_unknown_item_emits_nothing() {
        let mut r = Reconciler::default();
        assert!(r.process_failed("never_seen").is_none());
    }

    #[test]
    fn type_partials_false_suppresses_partial_but_completed_still_finalizes() {
        let mut r = Reconciler::default();
        let ev = r.process_delta("item_1", "hello", false);
        assert!(ev.is_none(), "type_partials=false must not emit Partial");
        let ev = r.process_completed("item_1", "hello world").unwrap();
        match ev {
            StreamingEvent::Final { text, .. } => assert_eq!(text, "hello world"),
            _ => panic!("expected Final"),
        }
    }

    #[test]
    fn common_prefix_counts_unicode_scalars_not_bytes() {
        assert_eq!(common_prefix_char_count("áb", "ác"), 1);
        assert_eq!(common_prefix_char_count("hello", "hellp"), 4);
    }
}
