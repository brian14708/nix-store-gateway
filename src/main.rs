#![warn(clippy::pedantic)]

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::status::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::StreamExt;
use tower_http::trace::TraceLayer;

mod app;
mod error;
mod sign;

use crate::app::{App, Config};
use crate::error::Error;

type AppState = Arc<App>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    if std::env::args().len() != 2 {
        eprintln!("Usage: {} <config.toml>", std::env::args().next().unwrap());
        std::process::exit(1);
    }

    let config = Config::load(std::env::args().nth(1).unwrap())?;
    let listener = TcpListener::bind(&config.listen).await?;

    let state = App::from_config(config)?;
    let app = Router::new()
        .route("/nix-cache-info", get(info))
        .route("/{*key}", get(fetch).head(check).put(upload).delete(delete))
        .with_state(AppState::new(state))
        .layer(TraceLayer::new_for_http());
    axum::serve(listener, app).await?;
    Ok(())
}

async fn info() -> &'static str {
    "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n"
}

async fn check(State(app): State<AppState>, request: Request) -> Response {
    let u = app.get_mirror(request.uri().path()).await;
    if let Some(u) = u {
        return Response::builder()
            .status(StatusCode::OK)
            .header("location", u)
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let o = app.get_origin(request.uri().path()).await;
    if let Some((u, _)) = o {
        return Response::builder()
            .status(StatusCode::OK)
            .header("location", u)
            .body(axum::body::Body::empty())
            .unwrap();
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn fetch(State(app): State<AppState>, request: Request) -> Response {
    let u = app.get_mirror(request.uri().path()).await;
    if let Some(url) = u {
        return Redirect::temporary(&url).into_response();
    }

    let o = app.get_origin(request.uri().path()).await;
    if let Some((_, resp)) = o {
        let headers = resp.headers().clone();
        let size = headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().parse().unwrap());

        let (tx, rx) = mpsc::channel::<reqwest::Result<Bytes>>(64);
        let (tx2, rx2) = mpsc::channel::<Result<Bytes, Error>>(64);
        tokio::spawn(async move {
            let mut b = resp.bytes_stream();
            while let Some(v) = b.next().await {
                match v {
                    Ok(buf) => {
                        if tx.send(Ok(buf.clone())).await.is_ok() {
                            let _ = tx2.send(Ok(buf)).await;
                        } else {
                            let _ = tx2.send(Err(Error::new("send error"))).await;
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx2.send(Err(Error::new(e.to_string()))).await;
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        tokio::spawn(app.upload(
            request.uri().path(),
            size,
            tokio_stream::wrappers::ReceiverStream::new(rx2),
        ));

        let mut r = Response::new(axum::body::Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ));
        *r.headers_mut() = headers;

        return r;
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn upload(State(app): State<AppState>, request: Request) -> Response {
    let size = request
        .headers()
        .get("content-length")
        .map(|v| v.to_str().unwrap().parse().unwrap());
    let path = request.uri().path().to_string();
    let body = request.into_body();

    if let Err(err) = app.upload(&path, size, body.into_data_stream()).await {
        tracing::error!("{} upload error: {:?}", path, err);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

async fn delete(State(app): State<AppState>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    if let Err(err) = app.delete(&path).await {
        tracing::error!("{} delete error: {:?}", path, err);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}
