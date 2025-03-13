use std::{future::Future, path::Path, time::Duration};

use anyhow::anyhow;
use bytes::Bytes;
use futures::Stream;
use reqwest::{Client, Url, redirect::Policy};
use serde::Deserialize;

use crate::sign::AwsSigner;

#[derive(Deserialize)]
pub struct Config {
    mirrors: Vec<Mirror>,
    origins: Vec<Origin>,
    s3: S3,
}

#[derive(Deserialize)]
struct Mirror {
    url: Url,
}

#[derive(Deserialize)]
struct Origin {
    url: Url,
}

#[derive(Deserialize)]
struct S3 {
    endpoint: Url,
    bucket: String,
    region: String,
    access_key_id: String,
    access_key_secret: String,
}

impl Config {
    pub fn load(config: impl AsRef<Path>) -> anyhow::Result<Self> {
        let config = std::fs::read_to_string(config)?;
        Ok(toml::from_str(&config)?)
    }
}

#[derive(Clone)]
enum CacheItem {
    Mirror(String),
    Origin(String),
    NotExistMirror,
    NotExistOrigin,
}

pub struct App {
    client: reqwest::Client,
    mirrors: Vec<Mirror>,
    origins: Vec<Origin>,
    aws_endpoint: Url,
    aws_signer: AwsSigner,
    cache: moka::future::Cache<String, CacheItem>,
}

impl App {
    const TTL: Duration = Duration::from_secs(5 * 60);

    pub fn from_config(config: Config) -> anyhow::Result<Self> {
        let client = Client::builder().redirect(Policy::none()).build()?;

        let aws_signer = AwsSigner::new(
            config.s3.access_key_id,
            config.s3.access_key_secret,
            config.s3.region,
            "s3".to_string(),
        );

        let aws_endpoint = {
            let mut u = config.s3.endpoint.clone();
            u.set_host(Some(&format!(
                "{}.{}",
                config.s3.bucket,
                u.host_str().ok_or_else(|| anyhow!("missing s3 host"))?
            )))?;
            u
        };

        let cache = moka::future::Cache::builder()
            .time_to_live(Self::TTL)
            .build();

        Ok(Self {
            client,
            mirrors: config.mirrors,
            origins: config.origins,
            aws_signer,
            aws_endpoint,
            cache,
        })
    }

    pub async fn get_mirror(&self, path: &str) -> Option<String> {
        match self.cache.get(path).await {
            Some(CacheItem::Mirror(s)) => return Some(s),
            Some(CacheItem::Origin(_) | CacheItem::NotExistOrigin | CacheItem::NotExistMirror) => {
                return None;
            }
            None => {}
        }

        let tasks = self
            .mirrors
            .iter()
            .map(|mirror| {
                let url = mirror.url.join(path.trim_start_matches('/')).unwrap();
                let req = self.client.get(url.clone()).build().unwrap();
                (req, url)
            })
            .chain(Some({
                let url = self
                    .aws_endpoint
                    .join(path.trim_start_matches('/'))
                    .unwrap();
                let sign = self
                    .aws_signer
                    .sign(self.client.head(url.clone()).build().unwrap());
                let geturl = self.aws_signer.sign_url(url, Self::TTL * 5);
                (sign, geturl)
            }))
            .rev()
            .map(|(req, url)| {
                Box::pin(async move {
                    if let Ok(resp) = self.client.execute(req).await {
                        let status = resp.status().as_u16();
                        if (200..300).contains(&status) {
                            return Ok(url.to_string());
                        }
                    }
                    Err(())
                })
            });

        let t = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            futures::future::select_ok(tasks),
        );
        let v = t.await;
        if let Ok(Ok((url, _))) = v {
            self.cache
                .insert(path.to_string(), CacheItem::Mirror(url.clone()))
                .await;
            Some(url)
        } else {
            self.cache
                .insert(path.to_string(), CacheItem::NotExistMirror)
                .await;
            None
        }
    }

    pub async fn get_origin(&self, path: &str) -> Option<(String, reqwest::Response)> {
        match self.cache.get(path).await {
            Some(CacheItem::Mirror(u) | CacheItem::Origin(u)) => {
                let req = self.client.get(u.clone()).build().unwrap();
                if let Ok(resp) = self.client.execute(req).await {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        return Some((u, resp));
                    }
                }
            }
            Some(CacheItem::NotExistOrigin) => {
                return None;
            }
            Some(CacheItem::NotExistMirror) | None => {}
        }

        let tasks = self.origins.iter().map(|origin| {
            Box::pin(async move {
                let mut url = origin.url.join(path.trim_start_matches('/')).unwrap();
                loop {
                    let req = self.client.get(url.clone()).build().unwrap();
                    if let Ok(resp) = self.client.execute(req).await {
                        match (resp.status().as_u16(), resp.headers().get("location")) {
                            (200..=299, _) => return Ok((url.to_string(), resp)),
                            (300..=399, Some(location)) => {
                                url = location.to_str().unwrap().parse().unwrap();
                            }
                            _ => return Err(()),
                        };
                    }
                }
            })
        });
        let v = futures::future::select_ok(tasks).await;
        if let Ok(((url, resp), _)) = v {
            self.cache
                .insert(path.to_string(), CacheItem::Origin(url.clone()))
                .await;
            Some((url, resp))
        } else {
            self.cache
                .insert(path.to_string(), CacheItem::NotExistOrigin)
                .await;
            None
        }
    }

    pub fn upload<E, T>(
        &self,
        path: &str,
        size: Option<u64>,
        data: T,
    ) -> impl Future<Output = anyhow::Result<()>> + use<E, T>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
        T: Stream<Item = Result<Bytes, E>> + Send + 'static,
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
        let geturl = self.aws_signer.sign_url(url, Self::TTL * 5);
        let p = path.to_string();
        async move {
            let _ = client.execute(sign).await?.error_for_status()?;
            cache.insert(p, CacheItem::Mirror(geturl.to_string())).await;
            Ok(())
        }
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let url = self
            .aws_endpoint
            .join(path.trim_start_matches('/'))
            .unwrap();
        let req = self.client.delete(url.clone());
        let sign = self.aws_signer.sign(req.build().unwrap());
        let _ = self.client.execute(sign).await?.error_for_status()?;
        self.cache.remove(path).await;
        Ok(())
    }
}
