# OpenAI Realtime Backend

Voxtype supports [OpenAI Realtime](https://platform.openai.com/docs/guides/realtime) (GA transcription API, `gpt-live-transcribe`) as a cloud streaming speech-to-text backend. Like Soniox, nothing runs on your machine — audio streams to OpenAI's servers over WebSocket and transcription events stream back.

## What is OpenAI Realtime?

OpenAI Realtime's transcription mode is a WebSocket API purpose-built for live transcription:

- **Server-side VAD** — the server finalizes turns itself as you speak (progressive per-utterance finals), or you can drive turn boundaries manually
- **Delta/completed events** — incremental partial text (`delta`) as the model reconsiders audio, then a canonical `completed` transcript per utterance (`item`)
- **Configurable latency/quality tradeoff** — `delay` from `minimal` to `xhigh`
- **Vocabulary priming** — `prompt` (free-form) and `keywords` (literal high-value spellings)
- **Noise reduction** — `near_field` / `far_field` input preprocessing

## Privacy

Audio is sent to a third-party service over TLS. OpenAI processes the audio server-side per their [API data usage policy](https://openai.com/policies/api-data-usage-policies). **Use a local engine (Whisper, Parakeet, etc.) instead if your dictation contains anything you cannot send off-device.**

## Cost

OpenAI Realtime transcription is paid. `gpt-live-transcribe` runs ~$0.017/min as of GA (2026-08) — check [OpenAI's pricing page](https://openai.com/api/pricing/) before relying on it for high-volume dictation.

## Requirements

- voxtype built with the `openai-realtime` Cargo feature:
  ```bash
  cargo build --release --features openai-realtime
  ```
- An OpenAI API key with Realtime API access
- Outbound HTTPS / WebSocket (wss://) access

No local model files. No GPU.

## Quick Start

1. Get an API key at [platform.openai.com](https://platform.openai.com/api-keys).
2. Export it:
   ```bash
   export OPENAI_API_KEY="your-key-here"
   ```
3. Minimal config in `~/.config/voxtype/config.toml`:
   ```toml
   engine = "openairealtime"

   [hotkey]
   mode = "toggle"           # required when streaming (default)
   key = "SCROLLLOCK"

   [openai_realtime]
   languages = ["en"]        # adjust for your languages
   ```
4. Run voxtype, press the hotkey, dictate, press again to stop.

## Sample Rate

OpenAI Realtime's `audio/pcm` input format accepts **only 24 kHz mono s16le**. Voxtype's audio capture pipeline delivers 16 kHz mono (the fixed contract every streaming backend receives — see `crate::transcribe::streaming::StreamingTranscriber`). This backend resamples every chunk 16 kHz → 24 kHz before sending; you don't need to (and can't usefully) change `[audio] sample_rate`.

## Turn Detection (Server VAD)

`[openai_realtime] turn_detection = true` turns on server-side VAD — **only for models that support it; the default model `gpt-live-transcribe` rejects turn_detection outright, so the default is `false`**. When enabled: OpenAI finalizes each utterance on its own as you speak, and voxtype types progressive finals at the cursor — no different from holding the hotkey through several sentences. On record stop, voxtype sends ~700ms of trailing silence to nudge the server into finalizing whatever utterance was in progress, then drains events for a bounded ~3s.

```toml
[openai_realtime]
turn_detection = false            # default; gpt-live-transcribe rejects server VAD
vad_threshold = 0.5               # default
vad_prefix_padding_ms = 300       # default
vad_silence_duration_ms = 550     # default
```

With `turn_detection = false` (the default) voxtype sends an explicit `input_audio_buffer.commit` at record stop instead of trailing silence, ending the (single) turn itself.

## Streaming vs Batch

Mirrors Soniox's `streaming` knob:

```toml
[hotkey]
mode = "toggle"

[openai_realtime]
streaming = true            # default
type_partials = true        # default; live cursor feedback from delta events
```

To use OpenAI Realtime without live partial typing (only commits at `completed`):

```toml
[openai_realtime]
type_partials = false       # cursor stays still until finals arrive
```

To use it without streaming (one-shot WebSocket session on key release, push-to-talk safe):

```toml
[hotkey]
mode = "push_to_talk"

[openai_realtime]
streaming = false           # buffer locally, single WS round trip on release
```

Batch mode still speaks the same WebSocket protocol (OpenAI has no separate REST batch endpoint like Soniox's async API) — it just opens one session, sends the whole recorded buffer, ends the turn, and returns the first item's transcript.

## Vocabulary Priming

```toml
[openai_realtime]
prompt = "Podcast about Rust async runtimes and Wayland compositors"
keywords = ["Voxtype", "tokio-tungstenite", "Hyprland"]
```

`keywords` entries must not contain `<`, `>`, or newlines — OpenAI rejects the `session.update` otherwise. Voxtype validates this at startup so a bad keyword fails fast with a config error instead of an opaque server-side rejection mid-session.

## Configuration Reference

See [CONFIGURATION.md → [openai_realtime]](CONFIGURATION.md#openai_realtime) for the full field-by-field reference. Key settings:

| Field | Default | Notes |
|---|---|---|
| `api_key` | env: `OPENAI_API_KEY` | Required (or `api_key_file`) |
| `api_key_file` | none | Path to a file containing just the key |
| `model` | `gpt-live-transcribe` | GA live transcription model |
| `delay` | `low` | `minimal`\|`low`\|`medium`\|`high`\|`xhigh` |
| `prompt` | none | Free-form vocabulary bias |
| `keywords` | `[]` | Literal spellings to prime |
| `languages` | `["en"]` | ISO 639-1, plural array |
| `noise_reduction` | `near_field` | `""` disables |
| `turn_detection` | `false` | Server VAD on/off (unsupported by `gpt-live-transcribe`) |
| `vad_threshold` | `0.5` | Server VAD only |
| `vad_prefix_padding_ms` | `300` | Server VAD only |
| `vad_silence_duration_ms` | `550` | Server VAD only |
| `streaming` | `true` | Live WS vs batch-on-release |
| `type_partials` | `true` | Type delta text at cursor; realtime only |

## Troubleshooting

See [TROUBLESHOOTING.md → OpenAI Realtime Backend Issues](TROUBLESHOOTING.md#openai-realtime-backend-issues) for:
- Auth errors
- Connect failures
- Fatal errors during session configuration
- PTT auto-promoted to toggle
- Tail-revision divergence at completion

## Comparing to Soniox

| | OpenAI Realtime | Soniox realtime | Soniox async |
|---|---|---|---|
| Quality (EN) | Excellent | Excellent | Excellent |
| Finality model | Per-item delta/completed (completed is canonical, replaces partial) | Per-token `is_final` (cumulative finals) | n/a (single final transcript) |
| Turn ending | Server VAD (default) or manual commit | Server-side endpoint detection or manual `finalize` | n/a |
| Sample rate | 24 kHz (resampled from 16 kHz capture) | 16 kHz (no resample) | 16 kHz (no resample) |
| Privacy | Cloud | Cloud | Cloud |
| Cost | Paid | Paid | Paid |
| Offline | No | No | No |

## Limitations

- **No on-prem option.** Cloud only.
- **Internet dependency.** No fallback to a local engine if the network drops mid-session — voxtype surfaces a `Streaming Error` notification and returns to idle.
- **No REST batch endpoint.** Unlike Soniox's async API, `streaming = false` still opens a WebSocket session (OpenAI doesn't offer a separate upload-and-poll transcription endpoint), so meeting mode's chunk-batching would pay per-chunk connect latency — meeting mode currently keeps its Soniox-specific async routing and does not special-case this engine.
- **Completion order across turns is not guaranteed** by the protocol; voxtype reconciles per `item_id` rather than assuming turns complete in the order they started.
- **Drain window on stop is a fixed ~3s.** There's no `finished:true`-equivalent terminal signal (unlike Soniox), so any transcript event arriving after the bounded drain window is lost by design.
