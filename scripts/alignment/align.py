#!/usr/bin/env python3
"""Acoustic word alignment for `voxtype transcribe --timestamps`.

Reads bounded, contiguous transcript segments on stdin. Stdout is one JSON
array. All words must have acoustic timings and scores; no interpolation or
proportional word timing is accepted as successful alignment.
"""
import argparse
import contextlib
import json
import math
import sys


def validate(expected, words, duration):
    tokens = expected.split()
    if tokens != [word.get("word") for word in words]:
        raise ValueError("alignment omitted or changed transcript words")
    end = 0.0
    result = []
    for word in words:
        # WhisperX interpolated words have no acoustic score. Reject them too.
        start, stop, score = (word.get(k) for k in ("start", "end", "score"))
        if not all(isinstance(x, (int, float)) and math.isfinite(x) for x in (start, stop, score)):
            raise ValueError(f"word lacks acoustic alignment: {word.get('word')!r}")
        if not (end <= start < stop <= duration + 0.01 and 0 <= score <= 1):
            raise ValueError(f"invalid acoustic word boundaries: {word.get('word')!r}")
        result.append({"text": word["word"], "start": start, "end": stop, "score": score})
        end = stop
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audio")
    parser.add_argument("--model-dir")
    args = parser.parse_args()
    segments = json.load(sys.stdin)
    # Library progress/download notices must never corrupt the JSON protocol.
    with contextlib.redirect_stdout(sys.stderr):
        import torch
        import whisperx

        torch.set_num_threads(4)
        torch.set_num_interop_threads(1)
        audio = whisperx.load_audio(args.audio)
        duration = len(audio) / 16_000
        selected = []
        previous = 0.0
        for segment in segments:
            start, end = segment["start"], segment["end"]
            if not (math.isfinite(start) and math.isfinite(end) and
                    previous <= start < end <= duration + 0.01 and end - start <= 30.01):
                raise ValueError("invalid or unbounded input segment")
            previous = end
            text = " ".join(segment["text"].split())
            if text:
                selected.append({"start": start, "end": end, "text": text})
        if not selected:
            print("[]", file=sys.__stdout__)
            return
        model, metadata = whisperx.load_align_model("en", "cpu", model_dir=args.model_dir)
        aligned = whisperx.align(selected, model, metadata, audio, "cpu",
                                 interpolate_method="ignore", print_progress=False)
        text = " ".join(segment["text"] for segment in selected)
        result = validate(text, aligned["word_segments"], duration)
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"voxtype-align: {error}", file=sys.stderr)
        sys.exit(1)
