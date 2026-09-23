//! Handwriting conversion (`POST /convert/v1/handwriting`, legacy `/api/v1/page`).
//!
//! The tablet sends MyScript iink batch JSON (strokes as x/y arrays) and expects JIIX
//! back. The real cloud forwards to MyScript; here strokes are rasterised and read with
//! the local `tesseract` binary, so no MyScript keys are needed. Tesseract is a print OCR
//! engine: neat print works, cursive is poor.
//!
//! Set `HWR_CAPTURE_DIR` to save every request/response pair (we have no real captures yet).

use std::{path::PathBuf, process::Stdio, time::Duration};

use axum::{extract::State, http::{header, HeaderMap, StatusCode}, response::{IntoResponse, Response}};
use serde::Deserialize;
use serde_json::{json, Value};
use tiny_skia::{LineCap, LineJoin, Paint, PathBuilder, Pixmap, Stroke, Transform};
use tokio::io::AsyncWriteExt;

use crate::{api::AppState, error::{Result, ServerError}};

const JIIX_CONTENT_TYPE: &str = "application/vnd.myscript.jiix";
/// Padding around the ink, in output pixels.
const PAD: f32 = 40.0;
/// Pen width in output pixels.
const PEN: f32 = 5.0;
/// Longest side of the rendered image; larger pages are scaled down.
const MAX_SIDE: f32 = 3000.0;
const TESSERACT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
struct Request {
    #[serde(default)]
    configuration: Configuration,
    #[serde(rename = "strokeGroups", default)]
    stroke_groups: Vec<StrokeGroup>,
}

#[derive(Deserialize, Default)]
struct Configuration { #[serde(default)] lang: Option<String> }

#[derive(Deserialize)]
struct StrokeGroup { #[serde(default)] strokes: Vec<InkStroke> }

#[derive(Deserialize)]
struct InkStroke { x: Vec<f32>, y: Vec<f32> }

/// Maps rendered-image pixels back to the tablet's coordinate space.
struct Frame { min_x: f32, min_y: f32, scale: f32 }

impl Frame {
    fn to_ink(&self, px: f32, py: f32) -> (f32, f32) {
        ((px - PAD) / self.scale + self.min_x, (py - PAD) / self.scale + self.min_y)
    }
}

/// A stroke as (x, y) points in the tablet's page coordinates.
pub(crate) type Points = Vec<(f32, f32)>;

fn render(strokes: &[Points]) -> Result<(Vec<u8>, Frame)> {
    let (mut min_x, mut min_y, mut max_x, mut max_y) = (f32::MAX, f32::MAX, f32::MIN, f32::MIN);
    for &(x, y) in strokes.iter().flatten() {
        min_x = min_x.min(x); min_y = min_y.min(y); max_x = max_x.max(x); max_y = max_y.max(y);
    }
    let (w, h) = ((max_x - min_x).max(1.0), (max_y - min_y).max(1.0));
    let scale = (MAX_SIDE / w.max(h)).min(1.0);
    let frame = Frame { min_x, min_y, scale };

    let mut pixmap = Pixmap::new((w * scale + 2.0 * PAD) as u32, (h * scale + 2.0 * PAD) as u32)
        .ok_or_else(|| ServerError::Internal("empty ink bounds".into()))?;
    pixmap.fill(tiny_skia::Color::WHITE);
    let mut paint = Paint::default();
    paint.set_color_rgba8(0, 0, 0, 255);
    paint.anti_alias = true;
    let pen = Stroke { width: PEN, line_cap: LineCap::Round, line_join: LineJoin::Round, ..Default::default() };

    for s in strokes {
        let mut pb = PathBuilder::new();
        let mut pts = s.iter().map(|&(x, y)| ((x - min_x) * scale + PAD, (y - min_y) * scale + PAD));
        let Some((x0, y0)) = pts.next() else { continue };
        pb.move_to(x0, y0);
        pb.line_to(x0 + 0.01, y0); // dots / single-point strokes still leave ink
        for (x, y) in pts { pb.line_to(x, y); }
        if let Some(path) = pb.finish() {
            pixmap.stroke_path(&path, &paint, &pen, Transform::identity(), None);
        }
    }
    let png = pixmap.encode_png().map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok((png, frame))
}

/// Tesseract language pack. Only `eng` is installed on this host, so every MyScript
/// locale (`en_US`, `de_DE`, ...) is read as English for now.
const TESSERACT_LANG: &str = "eng";

async fn tesseract(png: &[u8], lang: &str) -> Result<String> {
    let mut child = tokio::process::Command::new("tesseract")
        .args(["stdin", "stdout", "-l", lang, "--psm", "6", "tsv"])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| ServerError::Internal(format!("tesseract not available: {e}")))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    stdin.write_all(png).await?;
    drop(stdin);
    let out = tokio::time::timeout(TESSERACT_TIMEOUT, child.wait_with_output()).await
        .map_err(|_| ServerError::Internal("tesseract timed out".into()))??;
    if !out.status.success() {
        return Err(ServerError::Internal(format!("tesseract exited with {}", out.status)));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// A recognised word, boxed in the tablet's page coordinates.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Word {
    pub text: String,
    pub x: f32,
    pub y: f32,
    pub w: f32,
    pub h: f32,
    /// Tesseract (block, paragraph, line), to tell line breaks from spaces.
    pub line: (u32, u32, u32),
}

/// Tesseract TSV (level 5 = word rows) -> words mapped back to ink coordinates.
fn parse_tsv(tsv: &str, frame: &Frame) -> Vec<Word> {
    tsv.lines().skip(1).filter_map(|row| {
        let f: Vec<&str> = row.split('\t').collect();
        if f.len() < 12 || f[0] != "5" { return None; }
        let text = f[11].trim();
        if text.is_empty() { return None; }
        let num = |i: usize| f[i].parse::<f32>().unwrap_or(0.0);
        let (x, y) = frame.to_ink(num(6), num(7));
        let (x2, y2) = frame.to_ink(num(6) + num(8), num(7) + num(9));
        Some(Word { text: text.to_owned(), x, y, w: x2 - x, h: y2 - y, line: (num(2) as u32, num(3) as u32, num(4) as u32) })
    }).collect()
}

/// Recognise handwriting in `strokes` (page coordinates) with the local engine.
pub(crate) async fn recognize(strokes: &[Points]) -> Result<Vec<Word>> {
    let (png, frame) = render(strokes)?;
    Ok(parse_tsv(&tesseract(&png, TESSERACT_LANG).await?, &frame))
}

/// Words -> JIIX text block (spaces/newlines as separator words, like MyScript).
fn to_jiix(words: &[Word]) -> Value {
    let mut items = Vec::new();
    let mut label = String::new();
    for (i, w) in words.iter().enumerate() {
        if i > 0 {
            let sep = if words[i - 1].line == w.line { " " } else { "\n" };
            label.push_str(sep);
            items.push(json!({ "label": sep }));
        }
        label.push_str(&w.text);
        items.push(json!({
            "label": w.text,
            "candidates": [w.text],
            "bounding-box": { "x": w.x, "y": w.y, "width": w.w, "height": w.h },
        }));
    }
    json!({ "type": "Text", "label": label, "words": items, "version": "3", "id": "MainBlock" })
}

fn capture(name: &str, body: &[u8]) {
    let Some(dir) = std::env::var_os("HWR_CAPTURE_DIR").map(PathBuf::from) else { return };
    let path = dir.join(format!("{}-{name}", chrono::Utc::now().format("%Y%m%dT%H%M%S%.3f")));
    if let Err(e) = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, body)) {
        tracing::warn!("could not save handwriting capture {}: {e}", path.display());
    }
}

pub async fn convert(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Result<Response> {
    state.auth_user(&headers)?;
    capture("request.json", &body);
    let req: Request = serde_json::from_slice(&body)?;
    let strokes: Vec<Points> = req.stroke_groups.iter().flat_map(|g| &g.strokes)
        .filter(|s| !s.x.is_empty() && s.x.len() == s.y.len())
        .map(|s| s.x.iter().copied().zip(s.y.iter().copied()).collect())
        .collect();
    if strokes.is_empty() {
        return Err(ServerError::Config("no strokes in request".into()));
    }

    if let Some(lang) = req.configuration.lang.as_deref().filter(|l| !l.starts_with("en")) {
        tracing::warn!(lang, "no tesseract pack for this language; reading as English");
    }
    let jiix = to_jiix(&recognize(&strokes).await?);
    tracing::info!(strokes = strokes.len(), text = %jiix["label"].as_str().unwrap_or_default(), "handwriting converted");

    let out = serde_json::to_vec(&jiix)?;
    capture("response.jiix", &out);
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, JIIX_CONTENT_TYPE)], out).into_response())
}
