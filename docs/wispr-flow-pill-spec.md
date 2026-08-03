# Wispr Flow pill — extracted visual spec (reference for the native OSD)

Extracted 2026-08-03 from Wispr Flow v1.6.326 `app.asar`
(`.webpack/renderer/status/index.js` — the StatusIndicator widget owns the
flow bar; `overlay/index.js` holds design tokens). This is the design target
for `voxtype-osd-native`'s Wispr-style pill.

## State machine (`St.fw` enum)

`HIDDEN, RESTING, CONTEXT_MENU, INITIALIZING, READY, ACTIVE_PTT,
ACTIVE_POPO, PROCESSING, POLISH_PROCESSING, POLISH_COMPLETED_ACTION,
POLISH_COMPLETED_NA, POLISH_FAILED, AUTO_CLEANUP_COMPLETED,
HOTKEY_REMINDER, INSTRUCT_COMPLETED, ERROR`

## The clarity mechanism: every state is a DIFFERENT pill

`StatusIndicator/styles.module.scss` `.thoughtBubble` — base chrome
`%pill-base`: `background: $shade-black (#000); border-radius: 22.5px;
border: 1px solid $vast-900`. State geometry (base/column orientation;
side docks flip some to horizontal):

| state                | size (w×h) | notes |
|----------------------|-----------|-------|
| RESTING              | 8×40      | rgba(0,0,0,.5) bg, 1px rgba(255,255,255,.5) border, radius 6px; waveform bars `opacity: 0` |
| READY (hover)        | 30×50     | `$flow-bar-thickness × $flow-bar-length` |
| ACTIVE_PTT           | 30×73     | waveform, bars solid white |
| ACTIVE_POPO          | 30×102.5  | waveform + 18×18 fully-round stop button (`.flowBarButton.activePopo`), rowContainer gap 8 |
| PROCESSING           | 30×98     | padding 12px 6px, gap 6px; dots/shimmer, no waveform |
| ERROR                | 30×91     | border 1.5px `$destructive-500 (#ee6a6a)` |
| POLISH_PROCESSING    | 136×30    | horizontal; shimmer label |
| POLISH_COMPLETED     | 30×152    | clickable, hover #4a4a4a |

State morph transition: **`all 0.1s cubic-bezier(0.05, 0.6, 0.4, 0.95)`** —
the silhouette snaps between shapes in 100ms. Instruct/polish size morphs:
250–400ms `cubic-bezier(0.4, 0, 0.2, 1)`. Follow-up pill entrance:
`opacity 0→1 + scale(0.9→1)` over 280ms `cubic-bezier(0.2, 0.7, 0.3, 1)`.

## Waveform (`Waveform/styles.module.scss` + React bar component)

- Bar: `width 2px; min-height 2px; border-radius 0.5px;
  background rgba(255,255,255,0.4)`; **`.micActive` → rgba(255,255,255,1)`**.
  Hue is reserved for modes: command `#ffa946`, instruct `var(--signal)`.
  `.resting` → opacity 0.
- Count: `barCount` default 10 (flow bar), mini variant 5.
- Center bulge: `--bar-height-scale = max(0, 1 − p²·(bulge/48))`
  (p = |center − i|, bulgeCoefficient default 1; linear falloff `1 − p·(b/48)`
  when b ≥ 2).
- Stagger: `animationDelay = i < ceil(n/2) ? 0.1·i : 0.1·(i − n)` seconds.
- Transform: `scaleY(max(1, --audio-scale × --bar-level-gain) ×
  --bar-height-scale × waveKeyframe)`.
- Wave keyframes (1s ease-in-out infinite): ×1 (0%), ×1.2 (20%), ×1.5 (40%),
  ×1.1 (80%), ×1.3 (90%), ×1 (100%).
- Mini per-bar level gains: `[0.8, 1, 1.2, 1, 0.8]`, delays
  `[.2, .3, .4, −.5, −.4]`.
- Audio scale: rAF loop, one-pole `next = 0.85·prev + 0.15·level`
  (floored to 2 decimals), then `--audio-scale = gain × smoothed`
  (mini gain = 5), floored at 1 when `floor` is set.

## Processing dots (`AnimatingDots/styles.module.scss`, bounce variant)

Three dots, `animation: bounce 1.4s ease-in-out infinite`, delays
0 / 0.2s / 0.4s. Keyframes: `translateY(0)` at 0%/60%/100%,
`translateY(-4px)` at 30%.

## Misc tokens

- Shadows: sm `0 1px 4px rgba(0,0,0,0.06)`, drag `0 2px 8px rgba(0,0,0,0.12)`.
- Fade in/out: 280ms; `pulse` keyframe: 50% → opacity .05.
- Rainbow ring (instruct only): 1px conic-gradient ring, radius 22.5,
  `rainbow-pill-ring-spin` full rotation.
- Docking: `[data-bar-position="bottom" | "left" | "right"]`; bottom is
  row-oriented (historical default), side docks flip the waveform to
  horizontal bars (`scaleX`, `wave-horizontal`).

## What the native OSD ports (and what it deliberately skips)

Ported: black capsule chrome, 10-bar white waveform with exact wave/bulge/
stagger/level math, solid-vs-40% mic signal, recording→processing shape
morph with bouncing dots, 280ms fades, scale-pop entrance.

Skipped (no pointer on a layer-shell overlay): hover expansion, stop/cancel
buttons, pickers, tooltips, dock dragging; polish/instruct/meeting states
(voxtype has no such modes).
