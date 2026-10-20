//! Recorded-audio transcription and acoustic word alignment. Dictation remains
//! on its configured streaming engine; this path is explicitly requested.
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use voxtype::config::OpenaiRealtimeConfig;

const RATE: usize = 16_000;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Word {
    pub text: String,
    pub start: f64,
    pub end: f64,
    pub score: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Transcript {
    pub text: String,
    pub engine: String,
    pub model: String,
    pub timing: Option<String>,
    pub words: Vec<Word>,
}

#[derive(Serialize)]
struct Segment {
    text: String,
    start: f64,
    end: f64,
}

/// Contiguous bounded chunks, cutting at a quiet 100ms window in the final
/// five seconds where possible. No overlap, dropped samples or silence removal.
fn chunks(samples: &[f32]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < samples.len() {
        let limit = (start + 30 * RATE).min(samples.len());
        let end = if limit < samples.len() {
            ((limit - 5 * RATE)..(limit - RATE / 10))
                .step_by(RATE / 10)
                .min_by(|&a, &b| {
                    let energy = |i| samples[i..i + RATE / 10].iter().map(|s| s * s).sum::<f32>();
                    energy(a).total_cmp(&energy(b))
                })
                .map(|i| i + RATE / 20)
                .unwrap_or(limit)
        } else {
            limit
        };
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn wav(samples: &[f32]) -> Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());
    let mut writer = hound::WavWriter::new(
        &mut buffer,
        hound::WavSpec {
            channels: 1,
            sample_rate: RATE as u32,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        },
    )?;
    for &sample in samples {
        ensure!(sample.is_finite(), "Audio contains non-finite samples");
        writer.write_sample((sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16)?;
    }
    writer.finalize()?;
    Ok(buffer.into_inner())
}

async fn request(
    client: &reqwest::Client,
    config: &OpenaiRealtimeConfig,
    key: &str,
    model: &str,
    samples: &[f32],
) -> Result<String> {
    let mut form = reqwest::multipart::Form::new()
        .text("model", model.to_owned())
        .text("response_format", "json")
        .part(
            "file",
            reqwest::multipart::Part::bytes(wav(samples)?)
                .file_name("audio.wav")
                .mime_str("audio/wav")?,
        );
    if let Some(prompt) = config.prompt.as_ref().filter(|s| !s.is_empty()) {
        form = form.text("prompt", prompt.clone());
    }
    for keyword in &config.keywords {
        form = form.text("keywords[]", keyword.clone());
    }
    for language in &config.languages {
        form = form.text("languages[]", language.clone());
    }
    let response = client
        .post("https://api.openai.com/v1/audio/transcriptions")
        .bearer_auth(key)
        .multipart(form)
        .send()
        .await?;
    let status = response.status();
    let body: serde_json::Value = response
        .json()
        .await
        .context("Transcription API returned invalid JSON")?;
    if !status.is_success() {
        // Log only the API's code/type, never request credentials or a body dump.
        bail!(
            "File transcription API failed ({status}): {}",
            body["error"]["code"].as_str().unwrap_or("unknown error")
        );
    }
    Ok(body["text"]
        .as_str()
        .context("Transcription API omitted text")?
        .trim()
        .to_owned())
}

fn align(audio: &Path, segments: &[Segment]) -> Result<Vec<Word>> {
    let mut child = Command::new("voxtype-align")
        .arg(audio)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Could not start voxtype-align (install Voxtype's alignment package)")?;
    let result = (|| {
        let mut stdin = child.stdin.take().context("aligner stdin missing")?;
        serde_json::to_writer(&mut stdin, segments)?;
        stdin.flush()?;
        Ok::<_, anyhow::Error>(())
    })();
    if let Err(error) = result {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    // Each invocation processes bounded 30s chunks; the model is loaded once.
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "Acoustic alignment failed; no guessed timestamps were emitted"
    );
    serde_json::from_slice(&output.stdout).context("Invalid alignment JSON")
}

pub(super) fn run(
    config: &OpenaiRealtimeConfig,
    samples: &[f32],
    model: &str,
    timestamps: bool,
) -> Result<Transcript> {
    ensure!(model == "gpt-transcribe", "Unsupported file model: {model}");
    ensure!(!samples.is_empty(), "Empty audio");
    if timestamps {
        ensure!(config.languages.as_slice() == ["en"], "This packaged alignment model supports English; refusing to guess another language's timestamps");
    }
    let key = voxtype::transcribe::openai_realtime::resolve_api_key(config)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let transcribe = async {
        let mut segments = Vec::new();
        for (start, end) in chunks(samples) {
            let text = request(&client, config, &key, model, &samples[start..end]).await?;
            segments.push(Segment {
                text,
                start: start as f64 / RATE as f64,
                end: end as f64 / RATE as f64,
            });
        }
        Ok::<_, anyhow::Error>(segments)
    };
    let segments =
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(transcribe))?;
    let text = segments
        .iter()
        .map(|s| s.text.as_str())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let words = if timestamps && !text.is_empty() {
        let mut audio = tempfile::NamedTempFile::new()?;
        audio.write_all(&wav(samples)?)?;
        align(audio.path(), &segments)?
    } else {
        Vec::new()
    };
    validate_words(
        &text,
        &words,
        samples.len() as f64 / RATE as f64,
        timestamps,
    )?;
    Ok(Transcript {
        text,
        engine: "openai-file".into(),
        model: model.into(),
        timing: timestamps.then(|| "whisperx:wav2vec2-base-960h".into()),
        words,
    })
}

fn validate_words(text: &str, words: &[Word], duration: f64, required: bool) -> Result<()> {
    if !required {
        return Ok(());
    }
    let expected = text.split_whitespace().collect::<Vec<_>>();
    ensure!(
        expected == words.iter().map(|w| w.text.as_str()).collect::<Vec<_>>(),
        "Alignment did not preserve every transcript word"
    );
    let mut previous = 0.0;
    for word in words {
        ensure!(
            word.start.is_finite()
                && word.end.is_finite()
                && word.score.is_finite()
                && word.start >= previous
                && word.end > word.start
                && word.end <= duration + 0.01
                && (0.0..=1.0).contains(&word.score),
            "Invalid acoustic timing for {:?}",
            word.text
        );
        previous = word.end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunks_preserve_all_samples_and_are_bounded() {
        let audio = vec![0.0; RATE * 97 + 3];
        let ranges = chunks(&audio);
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, audio.len());
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
        assert!(ranges.iter().all(|(a, b)| b > a && b - a <= 30 * RATE));
    }
    #[test]
    fn missing_reordered_or_invalid_words_are_errors() {
        let word = |text: &str, start, end| Word {
            text: text.into(),
            start,
            end,
            score: 0.8,
        };
        assert!(validate_words("hello world", &[word("hello", 0.1, 0.4)], 1.0, true).is_err());
        assert!(validate_words("hello", &[word("hello", 0.5, 0.2)], 1.0, true).is_err());
        assert!(validate_words("hello", &[word("hello", 0.1, 0.4)], 1.0, true).is_ok());
        assert!(validate_words("hello", &[word("hello", f64::NAN, 0.4)], 1.0, true).is_err());
    }
}
