//! Wayland + wgpu + egui-wgpu glue for `voxtype-osd-native`.
//!
//! The whole rendering stack is collapsed into one file because the borrow
//! relationships between SCTK state, the wgpu device/queue, the surface
//! configuration, and the egui-wgpu renderer are awkward to split without
//! introducing references with non-trivial lifetimes. Each piece is small,
//! and keeping them together makes the lifecycle (`create_surface_if_needed`
//! / `tear_down_surface`) easy to follow.

use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context as _};
use raw_window_handle::{
    RawDisplayHandle, RawWindowHandle, WaylandDisplayHandle, WaylandWindowHandle,
};
use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState, Region},
    delegate_compositor, delegate_layer, delegate_output, delegate_registry,
    output::{OutputHandler, OutputState},
    reexports::{
        calloop::{self, EventLoop},
        calloop_wayland_source::WaylandSource,
        client::{
            globals::registry_queue_init,
            protocol::{wl_output, wl_surface::WlSurface},
            Connection, Proxy, QueueHandle,
        },
    },
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
    shell::{
        wlr_layer::{
            Anchor, KeyboardInteractivity, Layer, LayerShell, LayerShellHandler, LayerSurface,
            LayerSurfaceConfigure,
        },
        WaylandSurface,
    },
};

use voxtype::osd::config::{OsdConfig, OsdPosition};
use voxtype::osd::ipc::FrameRing;
use voxtype::osd::visual::PeakHold;

/// State shared between the IPC thread and the render thread.
#[derive(Clone)]
pub struct SharedState {
    pub ring: Arc<Mutex<FrameRing>>,
    pub peak_hold: Arc<Mutex<PeakHold>>,
    /// Wall-clock timestamp of the most recent frame. Used to drive idle
    /// teardown when no frames have arrived for a while.
    pub last_frame_at: Arc<Mutex<Option<Instant>>>,
    pub config: OsdConfig,
    /// Path to the daemon's state file (`recording` / `transcribing` / …).
    /// Drives bar brightness: solid white while recording, dimmed while
    /// the drain finishes transcription.
    pub state_path: std::path::PathBuf,
}

/// How long to keep the surface alive after the last frame arrived, before
/// destroying it. The daemon stops emitting between recordings; this value
/// controls how quickly the OSD disappears after that.
/// Idle threshold for tearing down the layer-shell + wgpu surface. Set
/// short enough that the OSD disappears immediately when the user releases
/// the hotkey, but long enough that the destroy+recreate cost on the next
/// recording isn't visible. 0.5s is the sweet spot: humans perceive sub-
/// second as "instant," and 0.5s is well above the recording boundary
/// gaps the daemon naturally produces.
const IDLE_TEARDOWN_SECS: f32 = 0.5;
/// Target render rate. 60 Hz is enough for a smooth scrolling waveform; we
/// can't render faster than the underlying frame rate (100 Hz IPC) gains us.
const REDRAW_INTERVAL_MS: u64 = 16;

/// Outer state owned by the calloop event loop. Implements the SCTK delegate
/// traits via `delegate_*` macros.
pub struct App {
    registry_state: RegistryState,
    output_state: OutputState,
    compositor_state: CompositorState,
    layer_shell: LayerShell,

    qh: QueueHandle<App>,
    conn: Connection,

    shared: SharedState,
    surface: Option<RenderSurface>,
}

/// All state tied to the live layer-shell surface. Dropped (via
/// `Option::take`) when we tear down for idle.
struct RenderSurface {
    layer: LayerSurface,
    wl_surface: WlSurface,

    /// Last accepted size from the compositor's configure. We use this to
    /// configure the wgpu surface.
    width: u32,
    height: u32,
    /// Whether we've received the first configure (and thus may render).
    configured: bool,
    /// When this surface appeared; drives the fade-in animation and the
    /// idle-wave phase clock.
    shown_at: Instant,
    /// Smoothed mic level (linear 0..1), for calm bar attack/decay.
    level_smooth: f32,
    /// Recording→processing morph progress (0 = recording pill with
    /// waveform, 1 = compact processing pill with bouncing dots).
    proc_blend: f32,

    // wgpu plumbing.
    _instance: wgpu::Instance,
    surface: wgpu::Surface<'static>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    surface_format: wgpu::TextureFormat,

    // egui plumbing.
    egui_ctx: egui::Context,
    egui_renderer: egui_wgpu::Renderer,
}

/// Run the event loop. Returns when the user closes the surface or the
/// loop exits via signal.
pub fn run(
    shared: SharedState,
    frame_ping_source: calloop::ping::PingSource,
) -> anyhow::Result<()> {
    let conn =
        Connection::connect_to_env().context("connect to Wayland; is WAYLAND_DISPLAY set?")?;
    let (globals, event_queue) = registry_queue_init::<App>(&conn).context("init registry")?;
    let qh = event_queue.handle();

    let mut event_loop: EventLoop<'static, App> =
        EventLoop::try_new().context("create calloop event loop")?;
    let loop_handle = event_loop.handle();

    let compositor_state =
        CompositorState::bind(&globals, &qh).context("compositor protocol unavailable")?;
    let layer_shell =
        LayerShell::bind(&globals, &qh).context("wlr-layer-shell protocol unavailable")?;
    let output_state = OutputState::new(&globals, &qh);
    let registry_state = RegistryState::new(&globals);

    WaylandSource::new(conn.clone(), event_queue)
        .insert(loop_handle.clone())
        .map_err(|e| anyhow!("insert WaylandSource: {}", e))?;

    let mut app = App {
        registry_state,
        output_state,
        compositor_state,
        layer_shell,
        qh: qh.clone(),
        conn: conn.clone(),
        shared,
        surface: None,
    };

    // Wake on each incoming audio frame: create the surface if needed,
    // request a redraw.
    loop_handle
        .insert_source(frame_ping_source, move |_, _, app: &mut App| {
            app.on_frame_ping();
        })
        .map_err(|e| anyhow!("insert ping source: {}", e))?;

    // Periodic redraw timer + idle teardown. Re-arms each fire.
    let timer = calloop::timer::Timer::from_duration(Duration::from_millis(REDRAW_INTERVAL_MS));
    loop_handle
        .insert_source(timer, |_deadline, _, app: &mut App| {
            app.tick();
            calloop::timer::TimeoutAction::ToDuration(Duration::from_millis(REDRAW_INTERVAL_MS))
        })
        .map_err(|e| anyhow!("insert redraw timer: {}", e))?;

    tracing::info!("entering event loop");
    loop {
        if let Err(e) = event_loop.dispatch(Some(Duration::from_secs(1)), &mut app) {
            tracing::error!("event loop dispatch failed: {}", e);
            break;
        }
    }

    drop(app);
    drop(conn);
    Ok(())
}

impl App {
    fn on_frame_ping(&mut self) {
        if self.surface.is_none() {
            if let Err(e) = self.create_surface() {
                tracing::warn!("Failed to create OSD surface: {:#}", e);
            }
        }
    }

    fn tick(&mut self) {
        let last_frame = self.shared.last_frame_at.lock().ok().and_then(|g| *g);
        let idle = match last_frame {
            Some(t) => t.elapsed().as_secs_f32() >= IDLE_TEARDOWN_SECS,
            None => true,
        };

        if idle && self.surface.is_some() {
            tracing::info!("Idle for {}s, tearing down surface", IDLE_TEARDOWN_SECS);
            self.tear_down_surface();
            return;
        }

        if self.surface.is_some() && !idle {
            if let Err(e) = self.render_frame() {
                tracing::warn!("render failed: {:#}", e);
            }
        }
    }

    fn create_surface(&mut self) -> anyhow::Result<()> {
        tracing::info!("Creating OSD layer surface");

        let wl_surface = self.compositor_state.create_surface(&self.qh);
        let layer = self.layer_shell.create_layer_surface(
            &self.qh,
            wl_surface.clone(),
            Layer::Overlay,
            Some("voxtype-osd"),
            None,
        );

        let cfg = &self.shared.config;
        let (anchor, margin_top, margin_bottom, margin_left, margin_right) =
            position_to_anchor_and_margins(cfg.position, cfg.margin_px as i32);
        layer.set_anchor(anchor);
        layer.set_margin(margin_top, margin_right, margin_bottom, margin_left);
        layer.set_size(cfg.width_px, cfg.height_px);
        layer.set_keyboard_interactivity(KeyboardInteractivity::None);
        layer.set_exclusive_zone(0);

        // Empty input region — clicks pass through. SCTK's Region helper
        // owns the wl_region and destroys it on drop. The wl_region must
        // outlive the commit that activates it; we let it drop after.
        let region = Region::new(&self.compositor_state)
            .map_err(|e| anyhow!("create input region: {}", e))?;
        wl_surface.set_input_region(Some(region.wl_region()));

        layer.commit();
        drop(region);

        // wgpu instance + surface.
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::VULKAN | wgpu::Backends::GL,
            flags: wgpu::InstanceFlags::default(),
            memory_budget_thresholds: wgpu::MemoryBudgetThresholds::default(),
            backend_options: wgpu::BackendOptions::default(),
            display: None,
        });

        // Raw handles. With wayland-client's `system` feature + wayland-backend
        // `client_system`, ObjectId/Backend expose libwayland pointers.
        let display_ptr = NonNull::new(self.conn.backend().display_ptr() as *mut std::ffi::c_void)
            .ok_or_else(|| anyhow!("null wl_display ptr"))?;
        let surface_ptr = NonNull::new(wl_surface.id().as_ptr() as *mut std::ffi::c_void)
            .ok_or_else(|| anyhow!("null wl_surface ptr"))?;

        let raw_display = RawDisplayHandle::Wayland(WaylandDisplayHandle::new(display_ptr));
        let raw_window = RawWindowHandle::Wayland(WaylandWindowHandle::new(surface_ptr));

        // SAFETY: the `wl_display` and `wl_surface` outlive the wgpu surface
        // because `RenderSurface` keeps them alive (Connection is held in
        // `App`; wl_surface is held in RenderSurface).
        let surface = unsafe {
            instance.create_surface_unsafe(wgpu::SurfaceTargetUnsafe::RawHandle {
                raw_display_handle: Some(raw_display),
                raw_window_handle: raw_window,
            })
        }
        .context("create wgpu surface")?;

        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::LowPower,
            compatible_surface: Some(&surface),
            force_fallback_adapter: false,
        }))
        .context("request wgpu adapter")?;

        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("voxtype-osd-device"),
            required_features: wgpu::Features::empty(),
            required_limits: wgpu::Limits::downlevel_defaults(),
            experimental_features: wgpu::ExperimentalFeatures::default(),
            memory_hints: wgpu::MemoryHints::Performance,
            trace: wgpu::Trace::Off,
        }))
        .context("request wgpu device")?;

        let surface_caps = surface.get_capabilities(&adapter);
        let surface_format = surface_caps
            .formats
            .iter()
            .copied()
            .find(|f| matches!(f, wgpu::TextureFormat::Bgra8UnormSrgb))
            .or_else(|| surface_caps.formats.first().copied())
            .ok_or_else(|| anyhow!("no surface formats available"))?;

        let egui_ctx = egui::Context::default();
        let egui_renderer = egui_wgpu::Renderer::new(
            &device,
            surface_format,
            egui_wgpu::RendererOptions {
                msaa_samples: 1,
                depth_stencil_format: None,
                dithering: false,
                predictable_texture_filtering: false,
            },
        );

        self.surface = Some(RenderSurface {
            layer,
            wl_surface,
            width: cfg.width_px,
            height: cfg.height_px,
            configured: false,
            shown_at: Instant::now(),
            level_smooth: 0.0,
            proc_blend: 0.0,
            _instance: instance,
            surface,
            device,
            queue,
            surface_format,
            egui_ctx,
            egui_renderer,
        });

        Ok(())
    }

    fn tear_down_surface(&mut self) {
        if let Some(rs) = self.surface.take() {
            let RenderSurface {
                layer,
                wl_surface,
                surface,
                device,
                queue,
                egui_renderer,
                _instance,
                ..
            } = rs;
            // Drop wgpu state first, then the wl_surface. LayerSurface drops
            // the role on drop; we then explicitly destroy the wl_surface.
            drop(egui_renderer);
            drop(queue);
            drop(device);
            drop(surface);
            drop(_instance);
            drop(layer);
            wl_surface.destroy();
        }
    }

    fn render_frame(&mut self) -> anyhow::Result<()> {
        let rs = match self.surface.as_mut() {
            Some(s) if s.configured => s,
            _ => return Ok(()),
        };

        let cst = rs.surface.get_current_texture();
        let surface_texture: wgpu::SurfaceTexture = match cst {
            wgpu::CurrentSurfaceTexture::Success(t)
            | wgpu::CurrentSurfaceTexture::Suboptimal(t) => t,
            wgpu::CurrentSurfaceTexture::Outdated | wgpu::CurrentSurfaceTexture::Lost => {
                reconfigure_surface(rs);
                return Ok(());
            }
            other => {
                tracing::debug!("acquire frame skipped: {:?}", other);
                return Ok(());
            }
        };

        let view = surface_texture
            .texture
            .create_view(&wgpu::TextureViewDescriptor::default());

        let raw_input = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(rs.width as f32, rs.height as f32),
            )),
            ..Default::default()
        };

        // Live mic level → perceptual "voice" 0..1. Three stages, each
        // fighting a specific artifact:
        //  1. RMS over the last ~80ms of 10ms frames — a single frame's
        //     peak is spiky and made the bars jitter at steady state.
        //  2. dBFS map (-40 quiet .. -14 loud speech) so response doesn't
        //     depend on absolute mic gain.
        //  3. Smoothstep expander: squashes the bottom of the range so
        //     room tone / mic self-noise sits still, while speech-level
        //     input passes through nearly untouched.
        // Then Wispr's calm one-pole (0.85 retain per frame).
        let target_voice = {
            let ring = self.shared.ring.lock().expect("ring poisoned");
            let amps: Vec<f32> = ring.iter().map(|f| f.max.abs().max(f.min.abs())).collect();
            let tail = &amps[amps.len().saturating_sub(8)..];
            let rms = if tail.is_empty() {
                0.0
            } else {
                (tail.iter().map(|a| a * a).sum::<f32>() / tail.len() as f32).sqrt()
            };
            let amp_db = 20.0 * rms.max(1e-4).log10();
            let x = ((amp_db + 40.0) / 26.0).clamp(0.0, 1.0);
            x * x * (3.0 - 2.0 * x)
        };
        rs.level_smooth += (target_voice - rs.level_smooth) * 0.15;
        let voice = rs.level_smooth;

        // Wispr's state signal is a SHAPE change, not a tint: recording is
        // the waveform pill; the moment you stop, it morphs (~100ms, their
        // cubic-bezier(.05,.6,.4,.95)) into a compact capsule with three
        // bouncing dots until delivery. The daemon's state file lives on
        // tmpfs; a 60 Hz read is nothing.
        // Streaming sessions write "streaming"; batch/hotkey paths write
        // "recording". Both are the mic-hot waveform face; everything
        // else ("transcribing", drain) is the dots face.
        let recording = std::fs::read_to_string(&self.shared.state_path)
            .map(|s| matches!(s.trim(), "recording" | "streaming"))
            .unwrap_or(true);
        let proc_target = if recording { 0.0 } else { 1.0 };
        rs.proc_blend += (proc_target - rs.proc_blend) * 0.30;
        let proc = rs.proc_blend;

        // Fade in on appear, fade out as the frame stream goes quiet before
        // idle teardown. 280ms, matching Wispr Flow's opacity transition.
        let t = rs.shown_at.elapsed().as_secs_f32();
        let since_frame = self
            .shared
            .last_frame_at
            .lock()
            .ok()
            .and_then(|g| *g)
            .map(|i| i.elapsed().as_secs_f32())
            .unwrap_or(f32::MAX);
        let fade_in = ease_in_out((t / 0.28).clamp(0.0, 1.0));
        let fade_out = ease_in_out(
            (1.0 - (since_frame - (IDLE_TEARDOWN_SECS - 0.28)) / 0.28).clamp(0.0, 1.0),
        );
        let ui_alpha = fade_in * fade_out;
        // Entrance: the pill pops from 94% scale as it fades in.
        let appear = 0.94 + 0.06 * fade_in;

        let width_px = rs.width;
        let height_px = rs.height;
        let gain = self.shared.config.waveform_gain;
        let full_output = rs.egui_ctx.run_ui(raw_input, |ui| {
            draw_ui(
                ui, width_px, height_px, voice, gain, t, ui_alpha, appear, proc,
            );
        });

        let primitives = rs
            .egui_ctx
            .tessellate(full_output.shapes, full_output.pixels_per_point);

        let screen_descriptor = egui_wgpu::ScreenDescriptor {
            size_in_pixels: [rs.width, rs.height],
            pixels_per_point: full_output.pixels_per_point,
        };

        for (id, image_delta) in &full_output.textures_delta.set {
            rs.egui_renderer
                .update_texture(&rs.device, &rs.queue, *id, image_delta);
        }

        let mut encoder = rs
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("voxtype-osd-encoder"),
            });

        rs.egui_renderer.update_buffers(
            &rs.device,
            &rs.queue,
            &mut encoder,
            &primitives,
            &screen_descriptor,
        );

        {
            // Transparent clear: the pill is drawn by egui, so everything
            // outside its rounded capsule stays see-through.
            let mut rpass = encoder
                .begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: Some("voxtype-osd-pass"),
                    color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                        view: &view,
                        resolve_target: None,
                        ops: wgpu::Operations {
                            load: wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT),
                            store: wgpu::StoreOp::Store,
                        },
                        depth_slice: None,
                    })],
                    depth_stencil_attachment: None,
                    timestamp_writes: None,
                    occlusion_query_set: None,
                    multiview_mask: None,
                })
                .forget_lifetime();

            rs.egui_renderer
                .render(&mut rpass, &primitives, &screen_descriptor);
        }

        for id in &full_output.textures_delta.free {
            rs.egui_renderer.free_texture(id);
        }

        rs.queue.submit(Some(encoder.finish()));
        rs.wl_surface.frame(&self.qh, rs.wl_surface.clone());
        surface_texture.present();
        Ok(())
    }
}

fn reconfigure_surface(rs: &mut RenderSurface) {
    let surface_config = wgpu::SurfaceConfiguration {
        usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
        format: rs.surface_format,
        width: rs.width.max(1),
        height: rs.height.max(1),
        present_mode: wgpu::PresentMode::Fifo,
        alpha_mode: wgpu::CompositeAlphaMode::PreMultiplied,
        view_formats: vec![],
        desired_maximum_frame_latency: 2,
    };
    rs.surface.configure(&rs.device, &surface_config);
}

fn position_to_anchor_and_margins(pos: OsdPosition, margin: i32) -> (Anchor, i32, i32, i32, i32) {
    // (anchor, top, bottom, left, right)
    match pos {
        OsdPosition::BottomCenter => (Anchor::BOTTOM, 0, margin, 0, 0),
        OsdPosition::TopCenter => (Anchor::TOP, margin, 0, 0, 0),
        OsdPosition::BottomLeft => (Anchor::BOTTOM | Anchor::LEFT, 0, margin, margin, 0),
        OsdPosition::BottomRight => (Anchor::BOTTOM | Anchor::RIGHT, 0, margin, 0, margin),
        OsdPosition::TopLeft => (Anchor::TOP | Anchor::LEFT, margin, 0, margin, 0),
        OsdPosition::TopRight => (Anchor::TOP | Anchor::RIGHT, margin, 0, 0, margin),
    }
}

/// Render the egui UI: a Wispr-Flow-style dictation pill — solid black
/// capsule, ten slim white bars breathing on a staggered wave animation,
/// scaled live by mic level with a center bulge.
///
/// Geometry and motion transcribed from Wispr Flow's flow-bar waveform
/// (`Waveform/styles.module.scss` + its React bar component): 10 bars,
/// center bulge `1 - p²/48`, keyframes ×1 → ×1.2 → ×1.5 → ×1.1 → ×1.3 → ×1
/// over 1s ease-in-out, 0.1s stagger fanning out from center, bars white
/// at 40% opacity when quiet and brightening with voice.
#[allow(clippy::too_many_arguments)]
fn draw_ui(
    ui: &mut egui::Ui,
    width: u32,
    height: u32,
    voice: f32,
    gain: f32,
    t: f32,
    alpha: f32,
    appear: f32,
    proc: f32,
) {
    use egui::{pos2, vec2, Color32, Rect, StrokeKind};
    let painter = ui.painter().clone();
    let w = width as f32;
    let h = height as f32;

    // Black capsule with a hairline ring, scale-popping in around center.
    // Recording→processing morphs the capsule ~38% narrower (Wispr changes
    // the pill's SILHOUETTE per state — that shape change is what makes
    // the mode legible at a glance).
    let full = Rect::from_min_size(egui::Pos2::ZERO, vec2(w, h));
    let pill_w = w * (1.0 - 0.38 * ease_in_out(proc));
    let pill = Rect::from_center_size(full.center(), vec2(pill_w, h) * appear).shrink(1.0);
    let radius = pill.height() * 0.5;
    painter.rect_filled(
        pill,
        radius,
        mul_alpha(Color32::from_rgba_unmultiplied(0, 0, 0, 245), alpha),
    );
    painter.rect_stroke(
        pill,
        radius,
        egui::Stroke::new(
            1.0,
            mul_alpha(Color32::from_rgba_unmultiplied(255, 255, 255, 22), alpha),
        ),
        StrokeKind::Inside,
    );

    const N: usize = 10;
    // Wispr's mini-waveform per-bar level gains, extended to 10 bars:
    // center reacts hardest, edges sway.
    const BAR_GAIN: [f32; N] = [0.8, 0.9, 1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 0.9, 0.8];
    // Wispr bar geometry, 1:1: bars are 2px-wide DOTS with an intrinsic
    // 2px height; ALL height comes from the scaleY-equivalent multiplier.
    // At rest that's a subtle shimmering dotted line (~2-3px); speech
    // stretches the dots into bars (their pill is 30px thick; ours 36 —
    // near-1:1 scale, so the raw pixel values carry over).
    let bar_w = 2.0_f32;
    let gap = 2.5_f32;
    let total = N as f32 * bar_w + (N as f32 - 1.0) * gap;
    let x0 = pill.center().x - total * 0.5 + bar_w * 0.5;
    let base_h = 2.0_f32 * appear;
    let max_h = pill.height() * 0.62;
    let center = (N as f32 - 1.0) / 2.0;
    let half = N.div_ceil(2);

    // Wispr's multiplicative scale: `max(1, gain × level × bar_gain)`,
    // on the perceptual voice level. Silence sits at ×1 (dots + idle
    // wave); speech ~×2-3.5; loud voice near the ×5.5 cap stretches a
    // dot to ~15px, matching their proportions. `gain` ([osd]
    // waveform_gain, default 10) is a trim: 10 → 1.0×.
    let audio = 5.5 * voice * (gain / 10.0);

    // Waveform (recording face) — crossfades out as the pill morphs to
    // processing. Wispr `.micActive`: bars solid white while the mic is
    // hot; no hue games.
    let bars_alpha = (1.0 - ease_in_out(proc)) * alpha;
    if bars_alpha > 0.02 {
        for i in 0..N {
            let p = (center - i as f32).abs();
            let bulge = (1.0 - (p * p) / 48.0).max(0.0);
            let delay = if i < half {
                0.1 * i as f32
            } else {
                0.1 * (i as f32 - N as f32)
            };
            let wave = wave_multiplier(t - delay);
            let audio_scale = (audio * BAR_GAIN[i]).max(1.0).min(5.5);
            let bar_h = (base_h * bulge * wave * audio_scale).clamp(base_h, max_h);
            let x = x0 + i as f32 * (bar_w + gap);
            let rect = Rect::from_center_size(pos2(x, pill.center().y), vec2(bar_w, bar_h));
            // Wispr: border-radius 0.5px on a 2px bar — near-square caps.
            painter.rect_filled(rect, 0.5, mul_alpha(Color32::WHITE, bars_alpha));
        }
    }

    // Processing face: Wispr's AnimatingDots — three dots on a 1.4s
    // ease-in-out bounce (up 4px at the 30% mark), staggered 0.2s.
    let dots_alpha = ease_in_out(proc) * alpha;
    if dots_alpha > 0.02 {
        let dot_r = 2.5_f32;
        let dot_gap = 10.0_f32;
        for i in 0..3 {
            let dy = dots_bounce(t - 0.2 * i as f32);
            let x = pill.center().x + (i as f32 - 1.0) * dot_gap;
            painter.circle_filled(
                pos2(x, pill.center().y + dy),
                dot_r,
                mul_alpha(Color32::WHITE, dots_alpha),
            );
        }
    }
}

/// Wispr's AnimatingDots bounce keyframe: 1.4s ease-in-out infinite,
/// `translateY(0)` at 0%/60%/100%, `translateY(-4px)` at 30%.
fn dots_bounce(t: f32) -> f32 {
    let p = t.rem_euclid(1.4) / 1.4;
    if p < 0.30 {
        -4.0 * ease_in_out(p / 0.30)
    } else if p < 0.60 {
        -4.0 * (1.0 - ease_in_out((p - 0.30) / 0.30))
    } else {
        0.0
    }
}

/// Wispr Flow's `@keyframes wave`, as a phase function: stops at
/// (0, ×1) (0.2, ×1.2) (0.4, ×1.5) (0.8, ×1.1) (0.9, ×1.3) (1, ×1),
/// ease-in-out between stops, looping at 1 Hz.
fn wave_multiplier(phase: f32) -> f32 {
    const STOPS: [(f32, f32); 6] = [
        (0.0, 1.0),
        (0.2, 1.2),
        (0.4, 1.5),
        (0.8, 1.1),
        (0.9, 1.3),
        (1.0, 1.0),
    ];
    let p = phase.rem_euclid(1.0);
    for w in STOPS.windows(2) {
        let (t0, v0) = w[0];
        let (t1, v1) = w[1];
        if p <= t1 {
            let f = ((p - t0) / (t1 - t0)).clamp(0.0, 1.0);
            return v0 + (v1 - v0) * ease_in_out(f);
        }
    }
    1.0
}

fn ease_in_out(f: f32) -> f32 {
    let f = f.clamp(0.0, 1.0);
    f * f * (3.0 - 2.0 * f)
}

fn mul_alpha(c: egui::Color32, a: f32) -> egui::Color32 {
    egui::Color32::from_rgba_unmultiplied(
        c.r(),
        c.g(),
        c.b(),
        (c.a() as f32 * a.clamp(0.0, 1.0)) as u8,
    )
}

// ---------------------------------------------------------------------------
// SCTK delegate trait impls
// ---------------------------------------------------------------------------

impl CompositorHandler for App {
    fn scale_factor_changed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &WlSurface,
        _new_factor: i32,
    ) {
    }

    fn transform_changed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &WlSurface,
        _new_transform: wl_output::Transform,
    ) {
    }

    fn frame(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &WlSurface,
        _time: u32,
    ) {
    }

    fn surface_enter(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &WlSurface,
        _output: &wl_output::WlOutput,
    ) {
    }

    fn surface_leave(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &WlSurface,
        _output: &wl_output::WlOutput,
    ) {
    }
}

impl OutputHandler for App {
    fn output_state(&mut self) -> &mut OutputState {
        &mut self.output_state
    }

    fn new_output(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }

    fn update_output(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }

    fn output_destroyed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }
}

impl LayerShellHandler for App {
    fn closed(&mut self, _conn: &Connection, _qh: &QueueHandle<Self>, layer: &LayerSurface) {
        if let Some(rs) = self.surface.as_ref() {
            if rs.layer.wl_surface().id() == layer.wl_surface().id() {
                tracing::info!("Compositor closed the layer surface");
                self.tear_down_surface();
            }
        }
    }

    fn configure(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        layer: &LayerSurface,
        configure: LayerSurfaceConfigure,
        _serial: u32,
    ) {
        let rs = match self.surface.as_mut() {
            Some(s) => s,
            None => return,
        };
        if rs.layer.wl_surface().id() != layer.wl_surface().id() {
            return;
        }
        let (mut w, mut h) = configure.new_size;
        if w == 0 {
            w = self.shared.config.width_px;
        }
        if h == 0 {
            h = self.shared.config.height_px;
        }
        rs.width = w;
        rs.height = h;
        rs.configured = true;
        reconfigure_surface(rs);
        if let Err(e) = self.render_frame() {
            tracing::warn!("initial render after configure failed: {:#}", e);
        }
    }
}

delegate_compositor!(App);
delegate_output!(App);
delegate_layer!(App);

impl ProvidesRegistryState for App {
    fn registry(&mut self) -> &mut RegistryState {
        &mut self.registry_state
    }
    registry_handlers![OutputState];
}

delegate_registry!(App);
