use std::{future::Future, path::Path, pin::Pin, time::Duration};

use bytes::Bytes;
use futures::Stream;
use reqwest::{redirect::Policy, Client, Url};
use serde::Deserialize;

use crate::sign::AwsSigner;

#[derive(Deserialize, Debug)]
pub struct Config {
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

impl Config {
    pub fn load(config: impl AsRef<Path>) -> Self {
        let config = std::fs::read_to_string(config).unwrap();
        toml::from_str(&config).unwrap()
    }
}

pub struct App {
    client: reqwest::Client,
    mirrors: Vec<Mirror>,
    origins: Vec<Origin>,
    aws_endpoint: Url,
    aws_signer: AwsSigner,
    cache: moka::future::Cache<String, Option<String>>,
}

impl App {
    pub fn from_config(config: Config) -> Self {
        let client = Client::builder().redirect(Policy::none()).build().unwrap();

        let aws_signer = AwsSigner::new(
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

        Self {
            client,
            mirrors: config.mirrors,
            origins: config.origins,
            aws_signer,
            aws_endpoint,
            cache,
        }
    }

    pub async fn get_mirror(&self, path: &str) -> Option<String> {
        if let Some(s) = self.cache.get(path).await {
            return s;
        }

        let mut tasks = Vec::<Pin<Box<dyn Future<Output = Result<String, ()>> + Send>>>::new();
        for mirror in &self.mirrors {
            let url = mirror.url.join(path.trim_start_matches('/')).unwrap();
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

    pub async fn get_origin(&self, path: &str) -> Option<reqwest::Response> {
        let mut tasks =
            Vec::<Pin<Box<dyn Future<Output = Result<reqwest::Response, ()>> + Send>>>::new();
        for origin in &self.origins {
            let url = origin.url.join(path.trim_start_matches('/')).unwrap();
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

    pub fn upload<E>(
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
