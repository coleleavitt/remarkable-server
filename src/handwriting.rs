//! Handwriting conversion (`POST /convert/v1/handwriting`, legacy `/api/v1/page`).
//!
//! The tablet sends MyScript iink batch JSON (strokes as x/y arrays) and expects JIIX
//! back. The real cloud forwards to MyScript; here strokes are rasterised and read with
//! the local `tesseract` binary, so no MyScript keys are needed. Tesseract is a print OCR
//! engine: neat print works, cursive is poor.
//!
//! Set `HWR_COMMAND` to use a different recogniser (see `tesseract()` below and
//! contrib/hwr/trocr_hwr.py). Set `HWR_CAPTURE_DIR` to save every request/response pair (we have no real captures yet).

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use tiny_skia::{LineCap, LineJoin, Paint, PathBuilder, Pixmap, Stroke, Transform};
use tokio::io::AsyncWriteExt;

use crate::api::AppState;
use crate::error::{Result, ServerError};

const JIIX_CONTENT_TYPE: &str = "application/vnd.myscript.jiix";
/// Padding around the ink, in output pixels.
const PAD: f32 = 40.0;
/// Pen width in output pixels.
const PEN: f32 = 5.0;
/// Longest side of the rendered image; larger pages are scaled down.
const MAX_SIDE: f32 = 3000.0;
const TESSERACT_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest request body (over it is a 413; read only once the caller is authenticated).
/// A request is the ink of one conversion (a selection, at most a page): per stroke,
/// `x`/`y` (and optionally `t`/`p`) arrays of JSON numbers. Written as shortest
/// round-trip doubles, as Qt's JSON writer does with a float widened to double
/// (`123.45f` becomes `123.44999694824219`), a number is up to ~20 bytes, so a point is
/// up to ~80. A very dense page is some 200k-500k points (its .rm file, at 14 bytes a
/// point, runs to a few MB), which is 16-40 MB of JSON at worst. 64 MiB covers that;
/// the body and its parse into point vectors are held in memory, so the 1 GiB of the
/// blob routes is no limit for it.
pub(crate) const MAX_BODY: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct Request {
    #[serde(default)]
    configuration: Configuration,
    #[serde(rename = "strokeGroups", default)]
    stroke_groups: Vec<StrokeGroup>,
}

#[derive(Deserialize, Default)]
struct Configuration {
    #[serde(default)]
    lang: Option<String>,
}

#[derive(Deserialize)]
struct StrokeGroup {
    #[serde(default)]
    strokes: Vec<InkStroke>,
}

#[derive(Deserialize)]
struct InkStroke {
    x: Vec<f32>,
    y: Vec<f32>,
}

/// Maps rendered-image pixels back to the tablet's coordinate space.
struct Frame {
    min_x: f32,
    min_y: f32,
    scale: f32,
}

impl Frame {
    fn to_ink(&self, px: f32, py: f32) -> (f32, f32) {
        (
            (px - PAD) / self.scale + self.min_x,
            (py - PAD) / self.scale + self.min_y,
        )
    }
}

/// A stroke as (x, y) points in the tablet's page coordinates.
pub(crate) type Points = Vec<(f32, f32)>;

fn render(strokes: &[Points]) -> Result<(Vec<u8>, Frame)> {
    let (mut min_x, mut min_y, mut max_x, mut max_y) = (f32::MAX, f32::MAX, f32::MIN, f32::MIN);
    for &(x, y) in strokes.iter().flatten() {
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
    }
    let (w, h) = ((max_x - min_x).max(1.0), (max_y - min_y).max(1.0));
    let scale = (MAX_SIDE / w.max(h)).min(1.0);
    let frame = Frame {
        min_x,
        min_y,
        scale,
    };

    let mut pixmap = Pixmap::new(
        (w * scale + 2.0 * PAD) as u32,
        (h * scale + 2.0 * PAD) as u32,
    )
    .ok_or_else(|| ServerError::Internal("empty ink bounds".into()))?;
    pixmap.fill(tiny_skia::Color::WHITE);
    let mut paint = Paint::default();
    paint.set_color_rgba8(0, 0, 0, 255);
    paint.anti_alias = true;
    let pen = Stroke {
        width: PEN,
        line_cap: LineCap::Round,
        line_join: LineJoin::Round,
        ..Default::default()
    };

    for s in strokes {
        let mut pb = PathBuilder::new();
        let mut pts = s
            .iter()
            .map(|&(x, y)| ((x - min_x) * scale + PAD, (y - min_y) * scale + PAD));
        let Some((x0, y0)) = pts.next() else { continue };
        pb.move_to(x0, y0);
        pb.line_to(x0 + 0.01, y0); // dots / single-point strokes still leave ink
        for (x, y) in pts {
            pb.line_to(x, y);
        }
        if let Some(path) = pb.finish() {
            pixmap.stroke_path(&path, &paint, &pen, Transform::identity(), None);
        }
    }
    let png = pixmap
        .encode_png()
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok((png, frame))
}

/// Tesseract language pack. Only `eng` is installed on this host, so every MyScript
/// locale (`en_US`, `de_DE`, ...) is read as English for now.
const TESSERACT_LANG: &str = "eng";

async fn tesseract(png: &[u8], lang: &str) -> Result<String> {
    // `HWR_COMMAND` swaps in another recogniser (e.g. contrib/hwr/trocr_hwr.py). It is run
    // via `sh -c`, gets the PNG on stdin and must print Tesseract-format TSV on stdout.
    let mut cmd = match std::env::var("HWR_COMMAND")
        .ok()
        .filter(|c| !c.trim().is_empty())
    {
        Some(c) => {
            let mut cmd = tokio::process::Command::new("sh");
            cmd.arg("-c").arg(c).env("HWR_LANG", lang);
            cmd
        }
        None => {
            let mut cmd = tokio::process::Command::new("tesseract");
            cmd.args(["stdin", "stdout", "-l", lang, "--psm", "6", "tsv"]);
            cmd
        }
    };
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| ServerError::Internal(format!("recogniser not available: {e}")))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    stdin.write_all(png).await?;
    drop(stdin);
    let out = tokio::time::timeout(TESSERACT_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| ServerError::Internal("recogniser timed out".into()))??;
    if !out.status.success() {
        return Err(ServerError::Internal(format!(
            "recogniser exited with {}",
            out.status
        )));
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
    tsv.lines()
        .skip(1)
        .filter_map(|row| {
            let f: Vec<&str> = row.split('\t').collect();
            if f.len() < 12 || f[0] != "5" {
                return None;
            }
            let text = f[11].trim();
            if text.is_empty() {
                return None;
            }
            let num = |i: usize| f[i].parse::<f32>().unwrap_or(0.0);
            let (x, y) = frame.to_ink(num(6), num(7));
            let (x2, y2) = frame.to_ink(num(6) + num(8), num(7) + num(9));
            Some(Word {
                text: text.to_owned(),
                x,
                y,
                w: x2 - x,
                h: y2 - y,
                line: (num(2) as u32, num(3) as u32, num(4) as u32),
            })
        })
        .collect()
}

/// Recognise handwriting in `strokes` (page coordinates) with the local engine.
pub(crate) async fn recognize(strokes: &[Points]) -> Result<Vec<Word>> {
    let (png, frame) = render(strokes)?;
    Ok(parse_tsv(&tesseract(&png, TESSERACT_LANG).await?, &frame))
}

/// Words -> JIIX, in the shape xochitl's parser reads (sub_4A155C, 3.3.2):
/// `root.elements[]` with `type == "Text"`, each with a `bounding-box {x,y,width,height}`
/// and `words[]`; a line break is a word labelled `"\n"` whose `reflow-label` is used
/// when text is reflowed (MyScript sets it to a space).
fn to_jiix(words: &[Word]) -> Value {
    let mut items = Vec::new();
    let mut label = String::new();
    for (i, w) in words.iter().enumerate() {
        if i > 0 {
            if words[i - 1].line == w.line {
                label.push(' ');
                items.push(json!({ "label": " " }));
            } else {
                label.push('\n');
                items.push(json!({ "label": "\n", "reflow-label": " " }));
            }
        }
        label.push_str(&w.text);
        items.push(json!({
            "label": w.text,
            "candidates": [w.text],
            "bounding-box": { "x": w.x, "y": w.y, "width": w.w, "height": w.h },
        }));
    }
    let (x0, y0) = words
        .iter()
        .fold((f32::MAX, f32::MAX), |(x, y), w| (x.min(w.x), y.min(w.y)));
    let (x1, y1) = words.iter().fold((f32::MIN, f32::MIN), |(x, y), w| {
        (x.max(w.x + w.w), y.max(w.y + w.h))
    });
    let bbox = if words.is_empty() {
        json!({ "x": 0, "y": 0, "width": 0, "height": 0 })
    } else {
        json!({ "x": x0, "y": y0, "width": x1 - x0, "height": y1 - y0 })
    };
    json!({
        "type": "Raw Content",
        "version": "3",
        "id": "MainBlock",
        "elements": [{ "type": "Text", "id": "text-1", "label": label, "bounding-box": bbox, "words": items }],
    })
}

fn capture(name: &str, body: &[u8]) {
    let Some(dir) = std::env::var_os("HWR_CAPTURE_DIR").map(PathBuf::from) else {
        return;
    };
    let path = dir.join(format!(
        "{}-{name}",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%.3f")
    ));
    if let Err(e) = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, body)) {
        tracing::warn!("could not save handwriting capture {}: {e}", path.display());
    }
}

pub async fn convert(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response> {
    state.auth_user(&headers)?;
    let body = crate::upload::read_body(&headers, body, MAX_BODY as u64).await?;
    capture("request.json", &body);
    let req: Request = serde_json::from_slice(&body)?;
    let strokes: Vec<Points> = req
        .stroke_groups
        .iter()
        .flat_map(|g| &g.strokes)
        .filter(|s| !s.x.is_empty() && s.x.len() == s.y.len())
        .map(|s| s.x.iter().copied().zip(s.y.iter().copied()).collect())
        .collect();
    if strokes.is_empty() {
        return Err(ServerError::Config("no strokes in request".into()));
    }

    if let Some(lang) = req
        .configuration
        .lang
        .as_deref()
        .filter(|l| !l.starts_with("en"))
    {
        tracing::warn!(
            lang,
            "no tesseract pack for this language; reading as English"
        );
    }
    let jiix = to_jiix(&recognize(&strokes).await?);
    tracing::info!(strokes = strokes.len(), text = %jiix["elements"][0]["label"].as_str().unwrap_or_default(), "handwriting converted");

    let out = serde_json::to_vec(&jiix)?;
    capture("response.jiix", &out);
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, JIIX_CONTENT_TYPE)],
        out,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror of xochitl 3.3.2's JIIX reader (sub_4A155C / sub_4A0D18): anything this
    /// can't find, the tablet can't either.
    fn read_like_firmware(jiix: &Value) -> Option<(String, [f64; 4])> {
        let el = jiix["elements"]
            .as_array()?
            .iter()
            .find(|e| e["type"] == "Text")?;
        let b = &el["bounding-box"];
        let bbox = [
            b["x"].as_f64()?,
            b["y"].as_f64()?,
            b["width"].as_f64()?,
            b["height"].as_f64()?,
        ];
        let text = el["words"]
            .as_array()?
            .iter()
            .map(|w| {
                let l = w["label"].as_str().unwrap_or_default();
                if l == "\n" {
                    w["reflow-label"].as_str().unwrap_or("\n").to_owned()
                } else {
                    l.to_owned()
                }
            })
            .collect();
        Some((text, bbox))
    }

    fn word(text: &str, x: f32, line: u32) -> Word {
        Word {
            text: text.into(),
            x,
            y: 5.0,
            w: 10.0,
            h: 4.0,
            line: (1, 1, line),
        }
    }

    #[test]
    fn jiix_is_readable_by_firmware_parser() {
        let jiix = to_jiix(&[
            word("hello", 0.0, 1),
            word("world", 20.0, 1),
            word("again", 0.0, 2),
        ]);
        let (text, bbox) = read_like_firmware(&jiix).expect("firmware would find no text");
        assert_eq!(text, "hello world again"); // line break reflows to a space
        assert_eq!(bbox, [0.0, 5.0, 30.0, 4.0]);
        assert_eq!(jiix["elements"][0]["label"], "hello world\nagain");
    }

    #[test]
    fn empty_page_still_well_formed() {
        let (text, _) = read_like_firmware(&to_jiix(&[])).expect("well-formed");
        assert!(text.is_empty());
    }

    mod route {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        use axum::body::Body;
        use axum::http::Request;
        use futures_util::StreamExt;
        use tower::ServiceExt;

        use super::*;
        use crate::device::DeviceManager;
        use crate::storage::Storage;

        /// `body` to `uri`; `auth` "" means a valid token.
        async fn post(
            uri: &str,
            auth: &str,
            len: Option<usize>,
            body: Body,
        ) -> (StatusCode, String) {
            let tmp = tempfile::TempDir::new().unwrap();
            let storage = Storage::new(tmp.path().join("storage")).unwrap();
            let devices =
                DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
            let auth = match auth {
                "" => format!("Bearer {}", devices.create_user_token("u@test").unwrap()),
                a => a.to_owned(),
            };
            let mut req = Request::post(uri)
                .header("authorization", auth)
                .header("content-type", "application/json");
            if let Some(len) = len {
                req = req.header("content-length", len);
            }
            let resp = crate::create_router(AppState::new(storage, devices))
                .oneshot(req.body(body).unwrap())
                .await
                .unwrap();
            let status = resp.status();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, String::from_utf8_lossy(&body).into_owned())
        }

        /// A request of exactly `len` bytes with no strokes in it.
        fn no_strokes(len: usize) -> Vec<u8> {
            let head =
                br#"{"configuration":{"lang":"en_US"},"strokeGroups":[{"strokes":[]}],"pad":""#;
            let mut body = head.to_vec();
            body.resize(len - 2, b'x');
            body.extend_from_slice(b"\"}");
            body
        }

        /// `body` as a request body that notes whether it was ever read.
        fn watched(body: Vec<u8>) -> (Body, Arc<AtomicBool>) {
            let read = Arc::new(AtomicBool::new(false));
            let flag = read.clone();
            let frames = futures_util::stream::iter([body]).map(move |frame| {
                flag.store(true, Ordering::SeqCst);
                Ok::<_, std::io::Error>(frame)
            });
            (Body::from_stream(frames), read)
        }

        #[tokio::test]
        async fn body_limit_is_explicit_and_read_after_auth() {
            for uri in ["/convert/v1/handwriting", "/api/v1/page"] {
                // Unauthenticated: 401 without reading the body at all.
                let (body, read) = watched(no_strokes(1024));
                let (status, _) = post(uri, "Bearer nope", None, body).await;
                assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
                assert!(!read.load(Ordering::SeqCst), "{uri}: body read before auth");
                // Declared over the limit: 413 before the body is read.
                let (body, read) = watched(no_strokes(1024));
                let (status, _) = post(uri, "", Some(MAX_BODY + 1), body).await;
                assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{uri}");
                assert!(!read.load(Ordering::SeqCst), "{uri}");
            }
            // Both routes share the handler; the full-size cases once.
            let uri = "/convert/v1/handwriting";
            // Over the limit while reading (no content-length): 413.
            let (status, body) = post(uri, "", None, Body::from(no_strokes(MAX_BODY + 1))).await;
            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
            // At the limit (far past axum's 2 MiB default): parsed as before.
            let (status, body) = post(uri, "", None, Body::from(no_strokes(MAX_BODY))).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(body.contains("no strokes"), "{body}");
        }
    }
}
