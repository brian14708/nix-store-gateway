use std::{error::Error, future::Future, pin::Pin, sync::Arc, time::Duration};

use axum::{
    extract::{Request, State},
    http::status::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{get, head},
    Router,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::{redirect::Policy, Client, Url};
use serde::Deserialize;
use tokio::sync::mpsc;

mod sign;

#[derive(Debug)]
struct MyError {
    details: String,
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for MyError {
    fn description(&self) -> &str {
        &self.details
    }
}

struct AppState {
    client: reqwest::Client,
    mirror: Vec<Url>,
    origin: Vec<Url>,
    aws_endpoint: Url,
    aws_signer: sign::AwsSigner,
    cache: moka::future::Cache<String, Option<String>>,
}

impl AppState {
    fn from_config(config: Config) -> Self {
        let client = Client::builder().redirect(Policy::none()).build().unwrap();

        let mirror: Vec<Url> = config.mirrors.into_iter().map(|m| m.url).collect();
        let origin: Vec<Url> = config.origins.into_iter().map(|o| o.url).collect();

        let aws_signer = sign::AwsSigner::new(
            config.s3.access_key_id,
            config.s3.access_key_secret,
            config.s3.region,
            "s3".to_string(),
        );

        let aws_endpoint = {
            let scheme = config.s3.endpoint.scheme();
            let host = config.s3.endpoint.host_str().unwrap();
            let full_url = format!("{}://{}.{}", scheme, config.s3.bucket, host);
            full_url.parse::<Url>().unwrap()
        };

        let cache = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .build();

        AppState {
            client,
            mirror,
            origin,
            aws_signer,
            aws_endpoint,
            cache,
        }
    }

    async fn get_mirror(&self, path: &str) -> Option<String> {
        if let Some(s) = self.cache.get(path).await {
            return s;
        }

        let mut tasks = Vec::<Pin<Box<dyn Future<Output = Result<String, ()>> + Send>>>::new();
        for mirror in &self.mirror {
            let url = mirror.join(path.trim_start_matches('/')).unwrap();
            let req = self.client.get(url.clone()).build().unwrap();
            tasks.push(Box::pin(async move {
                if let Ok(resp) = self.client.execute(req).await {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        Ok(url.to_string())
                    } else {
                        Err(())
                    }
                } else {
                    Err(())
                }
            }));
        }
        let url = self
            .aws_endpoint
            .join(path.trim_start_matches('/'))
            .unwrap();
        let sign = self
            .aws_signer
            .sign(self.client.head(url.clone()).build().unwrap());
        tasks.push(Box::pin(async move {
            if let Ok(resp) = self.client.execute(sign).await {
                let status = resp.status().as_u16();
                if (200..300).contains(&status) {
                    let sign = self.aws_signer.sign_url(url, Duration::from_secs(600));
                    Ok(sign.to_string())
                } else {
                    Err(())
                }
            } else {
                Err(())
            }
        }));

        let v = futures::future::select_ok(tasks.into_iter()).await;
        if let Ok((url, _)) = v {
            self.cache.insert(path.to_string(), Some(url.clone())).await;
            return Some(url);
        } else {
            self.cache.insert(path.to_string(), None).await;
        }
        None
    }

    async fn get_origin(&self, path: &str) -> Option<reqwest::Response> {
        let mut tasks =
            Vec::<Pin<Box<dyn Future<Output = Result<reqwest::Response, ()>> + Send>>>::new();
        for origin in &self.origin {
            let url = origin.join(path.trim_start_matches('/')).unwrap();
            println!("{:?}", url);
            tasks.push(Box::pin(async move {
                let mut url = url;
                loop {
                    let req = self.client.get(url).build().unwrap();
                    if let Ok(resp) = self.client.execute(req).await {
                        let status = resp.status().as_u16();
                        if let (true, Some(location)) =
                            ((300..400).contains(&status), resp.headers().get("location"))
                        {
                            url = location.to_str().unwrap().parse().unwrap();
                            continue;
                        } else if (200..300).contains(&status) {
                            return Ok(resp);
                        } else {
                            return Err(());
                        }
                    }
                    break;
                }
                Err(())
            }));
        }
        let v = futures::future::select_ok(tasks.into_iter()).await;
        if let Ok((resp, _)) = v {
            Some(resp)
        } else {
            None
        }
    }

    fn upload<E>(
        &self,
        path: String,
        size: Option<usize>,
        data: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    ) -> impl Future<Output = reqwest::Result<()>>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let url = self
            .aws_endpoint
            .join(path.trim_start_matches('/'))
            .unwrap();
        let mut req = self
            .client
            .put(url.clone())
            .body(reqwest::Body::wrap_stream(data));
        if let Some(size) = size {
            req = req.header("content-length", size);
        }
        let sign = self.aws_signer.sign(req.build().unwrap());
        let client = self.client.clone();
        let cache = self.cache.clone();
        let geturl = self.aws_signer.sign_url(url, Duration::from_secs(600));
        let p = path.to_string();
        async move {
            cache.remove(&p).await;
            let resp = client.execute(sign).await?;
            if resp.error_for_status().is_ok() {
                cache.insert(p, Some(geturl.to_string())).await;
            } else {
                cache.insert(p, None).await;
            }
            Ok(())
        }
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    mirrors: Vec<Mirror>,
    origins: Vec<Origin>,
    s3: S3,
}

#[derive(Deserialize, Debug)]
struct Mirror {
    url: Url,
}

#[derive(Deserialize, Debug)]
struct Origin {
    url: Url,
}

#[derive(Deserialize, Debug)]
struct S3 {
    endpoint: Url,
    bucket: String,
    region: String,
    access_key_id: String,
    access_key_secret: String,
}

#[tokio::main]
async fn main() {
    // read from config file
    let config = std::fs::read_to_string("config.toml").unwrap();
    let config: Config = toml::from_str(&config).unwrap();
    let state = AppState::from_config(config);

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

async fn fetch(State(app): State<Arc<AppState>>, request: Request) -> Response {
    let u = app.get_mirror(request.uri().path()).await;
    if u.is_some() {
        StatusCode::OK.into_response()
    } else {
        println!("not found {:?}", request.uri().path());
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn fetch_redir(State(app): State<Arc<AppState>>, request: Request) -> Response {
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
        let (tx2, rx2) = mpsc::channel::<Result<Bytes, MyError>>(64);
        tokio::spawn(async move {
            let mut b = b;
            while let Some(v) = b.next().await {
                match v {
                    Ok(buf) => {
                        if tx.send(Ok(buf.clone())).await.is_ok() {
                            tx2.send(Ok(buf)).await.unwrap();
                        } else {
                            let _ = tx2
                                .send(Err(MyError {
                                    details: "send error".to_string(),
                                }))
                                .await;
                            break;
                        }
                    }
                    Err(e) => {
                        tx2.send(Err(MyError {
                            details: e.to_string(),
                        }))
                        .await
                        .unwrap();
                        tx.send(Err(e)).await.unwrap();
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

async fn upload(State(app): State<Arc<AppState>>, request: Request) -> Response {
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
