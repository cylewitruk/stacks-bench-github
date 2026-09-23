use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_MATCH, RANGE};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::get;
use tempfile::tempdir;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const ETAG_VALUE: &str = "\"ripcat-test-v1\"";

#[derive(Clone)]
struct ServerState {
    data: Arc<Vec<u8>>,
    truncate_once: Arc<AtomicBool>,
    change_after_probe: bool,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    ranges: Arc<Mutex<Vec<(usize, usize)>>>,
}

async fn object(State(state): State<ServerState>, headers: HeaderMap) -> Response<Body> {
    let (start, end) = parse_range(
        headers
            .get(RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
    );
    let is_probe = start == 0 && end == 0 && !headers.contains_key(IF_MATCH);
    if state.change_after_probe && !is_probe {
        return Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(Body::empty())
            .unwrap();
    }
    if let Some(value) = headers.get(IF_MATCH)
        && value != ETAG_VALUE
    {
        return Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(Body::empty())
            .unwrap();
    }

    state
        .ranges
        .lock()
        .unwrap()
        .push((start, end));
    let active = state
        .active
        .fetch_add(1, Ordering::SeqCst)
        + 1;
    state
        .max_active
        .fetch_max(active, Ordering::SeqCst);
    if !is_probe {
        sleep(if start == 0 { Duration::from_millis(30) } else { Duration::from_millis(5) }).await;
    }
    state
        .active
        .fetch_sub(1, Ordering::SeqCst);

    let expected_len = end - start + 1;
    let truncate = !is_probe
        && start == 0
        && end > 0
        && state
            .truncate_once
            .swap(false, Ordering::SeqCst);
    let body_end = if truncate { start + expected_len / 2 } else { end + 1 };
    let payload = Bytes::copy_from_slice(&state.data[start..body_end]);
    let body = if truncate {
        Body::from_stream(futures::stream::iter([
            Ok::<_, std::io::Error>(payload),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "injected truncated response",
            )),
        ]))
    } else {
        Body::from(payload)
    };
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(CONTENT_RANGE, format!("bytes {start}-{end}/{}", state.data.len()))
        .header(CONTENT_LENGTH, expected_len)
        .header(ETAG, ETAG_VALUE)
        .body(body)
        .unwrap()
}

fn parse_range(value: &str) -> (usize, usize) {
    let range = value
        .strip_prefix("bytes=")
        .unwrap();
    let (start, end) = range.split_once('-').unwrap();
    (start.parse().unwrap(), end.parse().unwrap())
}

async fn spawn_server(state: ServerState) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/archive", get(object))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap();
    });
    (format!("http://{address}/archive"), task)
}

fn state(data: Vec<u8>) -> ServerState {
    ServerState {
        data: Arc::new(data),
        truncate_once: Arc::new(AtomicBool::new(false)),
        change_after_probe: false,
        active: Arc::new(AtomicUsize::new(0)),
        max_active: Arc::new(AtomicUsize::new(0)),
        ranges: Arc::new(Mutex::new(Vec::new())),
    }
}

#[tokio::test]
async fn retries_partial_ranges_and_emits_in_order() {
    let data: Vec<_> = (0..2 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let state = state(data.clone());
    state
        .truncate_once
        .store(true, Ordering::SeqCst);
    let observations = state.clone();
    let (url, server) = spawn_server(state).await;
    let spool_parent = tempdir().unwrap();
    let output_dir = tempdir().unwrap();
    let output_path = output_dir
        .path()
        .join("output");
    let mut output = fs::File::create(&output_path)
        .await
        .unwrap();
    let options = ripcat::DownloadOptions {
        connections: 4,
        window_bytes: 512 * 1024,
        max_retries: 3,
        retry_base_delay: Duration::from_millis(1),
        spool_dir: Some(spool_parent.path().to_owned()),
        ..ripcat::DownloadOptions::default()
    };
    let progress = Mutex::new(Vec::<ripcat::DownloadProgress>::new());

    let report = ripcat::stream_url_with_progress(&url, &mut output, options, |snapshot| {
        progress
            .lock()
            .unwrap()
            .push(snapshot);
    })
    .await
    .unwrap();
    drop(output);
    server.abort();

    assert_eq!(
        fs::read(output_path)
            .await
            .unwrap(),
        data
    );
    assert_eq!(report.bytes, 2 * 1024 * 1024);
    assert!(report.retries >= 1);
    let progress = progress.into_inner().unwrap();
    assert!(
        progress
            .iter()
            .any(|snapshot| snapshot.active_chunks > 1)
    );
    assert!(
        progress
            .iter()
            .all(|snapshot| snapshot.active_chunks <= 4)
    );
    assert_eq!(
        progress
            .last()
            .unwrap()
            .active_chunks,
        0
    );
    assert!(
        observations
            .max_active
            .load(Ordering::SeqCst)
            > 1
    );
    let first_chunk_attempts = observations
        .ranges
        .lock()
        .unwrap()
        .iter()
        .filter(|&&(start, end)| start < 128 * 1024 && end == 128 * 1024 - 1)
        .count();
    assert_eq!(first_chunk_attempts, 2);
    assert_eq!(
        spool_parent
            .path()
            .read_dir()
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn fails_closed_when_the_remote_object_changes() {
    let data: Vec<_> = (0..256 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let mut state = state(data);
    state.change_after_probe = true;
    let (url, server) = spawn_server(state).await;
    let spool_parent = tempdir().unwrap();
    let output_dir = tempdir().unwrap();
    let output_path = output_dir
        .path()
        .join("output");
    let mut output = fs::File::create(&output_path)
        .await
        .unwrap();
    let options = ripcat::DownloadOptions {
        connections: 2,
        window_bytes: 128 * 1024,
        spool_dir: Some(spool_parent.path().to_owned()),
        ..ripcat::DownloadOptions::default()
    };

    let error = ripcat::stream_url(&url, &mut output, options)
        .await
        .unwrap_err();
    drop(output);
    server.abort();

    assert!(
        error
            .to_string()
            .contains("HTTP 412"),
        "{error}"
    );
    assert!(
        fs::read(output_path)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        spool_parent
            .path()
            .read_dir()
            .unwrap()
            .count(),
        0
    );
}
