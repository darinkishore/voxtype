//! Band-limited sample-rate conversion.
//!
//! Replaces the linear interpolation this module used to do inline. Linear
//! interpolation has no anti-aliasing filter, so downsampling 48kHz to 16kHz
//! folded every component above 8kHz back into the passband — on the most
//! common capture configuration there is, we handed the model audio dirtier
//! than the microphone produced (#641).
//!
//! Two things matter beyond swapping in a better kernel:
//!
//! 1. **State is carried across chunks.** The old function was called
//!    per-callback and started fresh every time, so the fractional read
//!    position reset at each chunk boundary and produced a discontinuity
//!    roughly a hundred times a second. A resampler has to live as long as
//!    the stream does.
//! 2. **The tail is flushed.** Whatever sits in the input buffer when
//!    recording stops is still speech. `flush` pads and drains it rather than
//!    dropping up to one chunk of audio off the end of every recording.

use rubato::{FftFixedIn, Resampler};

/// Input frames handed to rubato per conversion step. Small enough that the
/// tail lost to `flush` padding is inaudible, large enough that the FFT is not
/// dominated by per-call overhead.
const CHUNK: usize = 1024;

/// Streaming resampler for one capture session.
///
/// Feed it whatever the audio callback produces with [`push`](Self::push) and
/// call [`flush`](Self::flush) once when the stream stops.
pub struct StreamResampler {
    inner: Option<FftFixedIn<f32>>,
    /// Input frames not yet consumed by a full conversion step.
    pending: Vec<f32>,
    from_rate: u32,
    to_rate: u32,
    /// Real input frames accepted, and output frames emitted so far.
    ///
    /// FftFixedIn lags its input by a chunk: the first `process` call returns
    /// nothing while the filter fills. Tracking both sides lets `flush` work
    /// out exactly how much audio is still owed and drain precisely that,
    /// rather than guessing from the pending buffer alone.
    frames_in: u64,
    frames_out: u64,
    /// Discard the filter startup delay, then drain an equal amount at EOF.
    delay_remaining: usize,
}

impl StreamResampler {
    /// Build a resampler for a rate pair. Matching rates produce a
    /// pass-through that copies rather than converting.
    pub fn new(from_rate: u32, to_rate: u32) -> Result<Self, String> {
        if from_rate == 0 || to_rate == 0 {
            return Err(format!(
                "invalid sample rates: {} -> {}",
                from_rate, to_rate
            ));
        }

        let inner = if from_rate == to_rate {
            None
        } else {
            Some(
                FftFixedIn::<f32>::new(from_rate as usize, to_rate as usize, CHUNK, 1, 1).map_err(
                    |e| format!("could not build resampler {from_rate} -> {to_rate}: {e}"),
                )?,
            )
        };

        let delay_remaining = inner.as_ref().map_or(0, |r| r.output_delay());
        Ok(Self {
            inner,
            pending: Vec::with_capacity(CHUNK * 2),
            from_rate,
            to_rate,
            frames_in: 0,
            frames_out: 0,
            delay_remaining,
        })
    }

    /// Convert as much of `samples` as forms whole chunks, returning the
    /// output produced. Leftover input is retained for the next call.
    pub fn push(&mut self, samples: &[f32]) -> Vec<f32> {
        let Some(inner) = self.inner.as_mut() else {
            return samples.to_vec();
        };

        self.pending.extend_from_slice(samples);
        self.frames_in += samples.len() as u64;

        let mut out = Vec::new();
        while self.pending.len() >= CHUNK {
            let chunk: Vec<f32> = self.pending.drain(..CHUNK).collect();
            match inner.process(&[chunk], None) {
                Ok(mut converted) => {
                    if let Some(channel) = converted.pop() {
                        let skip = self.delay_remaining.min(channel.len());
                        self.delay_remaining -= skip;
                        out.extend(channel.into_iter().skip(skip));
                    }
                }
                Err(e) => {
                    // Dropping the chunk is better than dying inside an audio
                    // callback; the gap is 1024 frames and the next chunk
                    // recovers on its own.
                    tracing::warn!("resampler step failed, dropping a chunk: {}", e);
                }
            }
        }
        self.frames_out += out.len() as u64;
        out
    }

    /// Convert whatever input remains, zero-padding to a whole chunk.
    ///
    /// Call once when the stream stops. Without it the final partial chunk —
    /// up to 1024 frames, about 21ms at 48kHz — is silently discarded from
    /// every recording.
    pub fn flush(&mut self) -> Vec<f32> {
        let ratio = self.to_rate as f64 / self.from_rate as f64;
        let Some(inner) = self.inner.as_mut() else {
            let rest = std::mem::take(&mut self.pending);
            self.frames_out += rest.len() as u64;
            return rest;
        };

        // Everything the input accounts for, minus what has already come out.
        let expected = (self.frames_in as f64 * ratio).round() as u64;
        let owed = expected.saturating_sub(self.frames_out) as usize;
        if owed == 0 && self.pending.is_empty() {
            return Vec::new();
        }

        // Pad the partial chunk, then feed silence until the delay line has
        // given back everything owed. The bound is a safety stop: two extra
        // chunks is far more than the one chunk of latency this resampler has.
        let mut out = Vec::with_capacity(owed);
        let mut pending = std::mem::take(&mut self.pending);
        for _ in 0..3 {
            if out.len() >= owed {
                break;
            }
            pending.resize(CHUNK, 0.0);
            match inner.process(&[std::mem::take(&mut pending)], None) {
                Ok(mut converted) => {
                    if let Some(channel) = converted.pop() {
                        let skip = self.delay_remaining.min(channel.len());
                        self.delay_remaining -= skip;
                        out.extend(channel.into_iter().skip(skip));
                    }
                }
                Err(e) => {
                    tracing::warn!("resampler flush failed, dropping the tail: {}", e);
                    break;
                }
            }
        }

        out.truncate(owed);
        self.frames_out += out.len() as u64;
        out
    }
}

/// One-shot conversion of a complete buffer.
///
/// For callers that already hold the whole recording, such as
/// `voxtype transcribe <file>`. Streaming callers want [`StreamResampler`]
/// so state carries across chunks.
pub fn resample_buffer(samples: &[f32], from_rate: u32, to_rate: u32) -> Vec<f32> {
    if from_rate == to_rate || samples.is_empty() {
        return samples.to_vec();
    }

    match StreamResampler::new(from_rate, to_rate) {
        Ok(mut r) => {
            let mut out = r.push(samples);
            out.extend(r.flush());
            out
        }
        Err(e) => {
            tracing::error!("{e}; passing audio through unconverted");
            samples.to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32::consts::TAU;

    fn tone(freq: f32, rate: u32, len: usize) -> Vec<f32> {
        (0..len)
            .map(|i| (TAU * freq * i as f32 / rate as f32).sin())
            .collect()
    }

    /// Energy near `freq`, by direct correlation. Enough to tell a present
    /// tone from an absent one without pulling in an FFT crate.
    fn energy_at(samples: &[f32], freq: f32, rate: u32) -> f32 {
        let (mut re, mut im) = (0.0f32, 0.0f32);
        for (i, s) in samples.iter().enumerate() {
            let phase = TAU * freq * i as f32 / rate as f32;
            re += s * phase.cos();
            im += s * phase.sin();
        }
        ((re * re + im * im).sqrt()) / samples.len() as f32
    }

    /// The point of #641. A 12kHz tone sampled at 48kHz cannot be represented
    /// at 16kHz (Nyquist 8kHz). A band-limited resampler attenuates it; linear
    /// interpolation folds it down to 4kHz, where the model hears a tone that
    /// was never spoken.
    #[test]
    fn downsampling_does_not_fold_energy_above_nyquist() {
        let input = tone(12_000.0, 48_000, 48_000);
        let out = resample_buffer(&input, 48_000, 16_000);

        let alias = energy_at(&out, 4_000.0, 16_000);
        let reference = energy_at(&input, 12_000.0, 48_000);

        assert!(
            alias < reference * 0.1,
            "12kHz folded to 4kHz at {alias:.4} against a source amplitude of \
             {reference:.4}; the anti-aliasing filter is not working"
        );
    }

    /// A tone comfortably inside the passband must survive.
    #[test]
    fn downsampling_preserves_audible_speech_band() {
        let input = tone(1_000.0, 48_000, 48_000);
        let out = resample_buffer(&input, 48_000, 16_000);

        assert!(
            energy_at(&out, 1_000.0, 16_000) > 0.2,
            "a 1kHz tone should pass through largely intact"
        );
    }

    /// Chunked input must produce the same length as one-shot input. The old
    /// implementation reset its read position every chunk, so this is the
    /// property that regressed silently.
    #[test]
    fn streaming_matches_one_shot_length() {
        let input = tone(440.0, 48_000, 48_000);

        let one_shot = resample_buffer(&input, 48_000, 16_000);

        let mut r = StreamResampler::new(48_000, 16_000).unwrap();
        let mut streamed = Vec::new();
        for block in input.chunks(577) {
            streamed.extend(r.push(block));
        }
        streamed.extend(r.flush());

        let diff = (one_shot.len() as i64 - streamed.len() as i64).abs();
        assert!(
            diff <= CHUNK as i64,
            "chunked and one-shot lengths diverged by {diff} samples"
        );
    }

    /// FftFixedIn lags its input by a full chunk: the first `process` call
    /// returns nothing while the filter fills. Before `flush` accounted for
    /// that, every recording silently lost its tail and anything shorter than
    /// one chunk produced no audio at all.
    #[test]
    fn flush_drains_the_delay_line() {
        // Well under one chunk: the streaming path emits nothing until flush.
        let mut r = StreamResampler::new(48_000, 16_000).unwrap();
        assert!(
            r.push(&vec![0.5; 300]).is_empty(),
            "a sub-chunk push cannot have produced output yet"
        );
        let tail = r.flush();
        assert_eq!(
            tail.len(),
            100,
            "300 frames at 3:1 owe 100 samples, all of which flush must return"
        );

        // And over several chunks the totals still reconcile.
        let mut r = StreamResampler::new(48_000, 16_000).unwrap();
        let mut total = 0usize;
        for _ in 0..4 {
            total += r.push(&vec![0.25; CHUNK]).len();
        }
        total += r.flush().len();
        let expected = (4 * CHUNK) / 3;
        assert!(
            (total as i64 - expected as i64).abs() <= 2,
            "4 chunks in should yield about {expected} out, got {total}"
        );
    }

    /// Length alone misses a delayed signal truncated to the expected length:
    /// it can contain startup silence while the actual final speech is lost.
    #[test]
    fn final_ten_milliseconds_survive_filter_delay_and_flush() {
        for rate in [44_100, 48_000] {
            let mut input = vec![0.0; rate as usize];
            let tail = tone(1_000.0, rate, rate as usize / 100);
            let start = input.len() - tail.len();
            input[start..].copy_from_slice(&tail);
            let mut r = StreamResampler::new(rate, 16_000).unwrap();
            let mut output = Vec::new();
            for chunk in input.chunks(577) {
                output.extend(r.push(chunk));
            }
            output.extend(r.flush());
            assert_eq!(output.len(), 16_000);
            assert!(
                energy_at(&output[15_840..], 1_000.0, 16_000) > 0.3,
                "the final 10ms of speech disappeared at {rate} Hz"
            );
            assert!(r.flush().is_empty());
        }
    }

    #[test]
    fn matching_rates_pass_through_untouched() {
        let input = tone(440.0, 16_000, 4_000);
        assert_eq!(resample_buffer(&input, 16_000, 16_000), input);

        let mut r = StreamResampler::new(16_000, 16_000).unwrap();
        assert_eq!(r.push(&input), input);
        assert!(r.flush().is_empty());
    }

    #[test]
    fn upsampling_produces_proportionally_more_samples() {
        let input = tone(440.0, 8_000, 8_000);
        let out = resample_buffer(&input, 8_000, 16_000);
        let expected = input.len() * 2;
        assert!(
            (out.len() as i64 - expected as i64).abs() <= CHUNK as i64,
            "8k -> 16k produced {} samples, expected about {expected}",
            out.len()
        );
    }

    #[test]
    fn empty_and_invalid_inputs_are_handled() {
        assert!(resample_buffer(&[], 48_000, 16_000).is_empty());
        assert!(StreamResampler::new(0, 16_000).is_err());
        assert!(StreamResampler::new(48_000, 0).is_err());
    }
}
