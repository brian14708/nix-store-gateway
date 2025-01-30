use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::status::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{get, head},
    Router,
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

mod app;
mod error;
mod sign;

use crate::app::{App, Config};
use crate::error::Error;

#[tokio::main]
async fn main() {
    let config = Config::load("config.toml");
    let state = App::from_config(config);

    let app = Router::new()
        .route("/nix-cache-info", get(root))
        .route("/{*key}", head(fetch).get(fetch_redir).put(upload))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n"
}

async fn fetch(State(app): State<Arc<App>>, request: Request) -> Response {
    let u = app.get_mirror(request.uri().path()).await;
    if u.is_some() {
        StatusCode::OK.into_response()
    } else {
        println!("not found {:?}", request.uri().path());
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn fetch_redir(State(app): State<Arc<App>>, request: Request) -> Response {
    let u = app.get_mirror(request.uri().path()).await;
    if let Some(url) = u {
        println!("ok {:?}", request.uri().path());
        return Redirect::temporary(&url).into_response();
    }

    let o = app.get_origin(request.uri().path()).await;
    if let Some(resp) = o {
        println!("ok {:?}", request.uri().path());
        let headers = resp.headers().clone();
        let size: Option<usize> = headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().parse().unwrap());
        let b = resp.bytes_stream();

        let (tx, rx) = mpsc::channel::<reqwest::Result<Bytes>>(64);
        let (tx2, rx2) = mpsc::channel::<Result<Bytes, Error>>(64);
        tokio::spawn(async move {
            let mut b = b;
            while let Some(v) = b.next().await {
                match v {
                    Ok(buf) => {
                        if tx.send(Ok(buf.clone())).await.is_ok() {
                            tx2.send(Ok(buf)).await.unwrap();
                        } else {
                            let _ = tx2.send(Err(Error::new("send error"))).await;
                            break;
                        }
                    }
                    Err(e) => {
                        tx2.send(Err(Error::new(e.to_string())))
                            .await
                            .expect("send error");
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        tokio::spawn(app.upload(
            request.uri().path().to_string(),
            size,
            tokio_stream::wrappers::ReceiverStream::new(rx2),
        ));

        let mut r = Response::new(axum::body::Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ));
        *r.headers_mut() = headers;

        return r;
    }

    println!("not found {:?}", request.uri().path());
    StatusCode::NOT_FOUND.into_response()
}

async fn upload(State(app): State<Arc<App>>, request: Request) -> Response {
    let size = request
        .headers()
        .get("content-length")
        .map(|v| v.to_str().unwrap().parse().unwrap());
    let path = request.uri().path().to_string();
    let body = request.into_body();
    println!("upload {:?}", path);
    let _ = app.upload(path, size, body.into_data_stream()).await;
    StatusCode::OK.into_response()
}
