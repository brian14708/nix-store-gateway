use chrono::Utc;
use hmac::{digest::FixedOutput, Hmac, Mac};
use itertools::Itertools;
use percent_encoding::AsciiSet;
use reqwest::{Request, Url};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, collections::BTreeMap, time::Duration};

pub struct AwsSigner {
    access_key: String,
    access_secret: String,
    region: String,
    service: String,
}

pub const SET: &AsciiSet = &percent_encoding::CONTROLS
    .add(b' ')
    .add(b'/')
    .add(b':')
    .add(b',')
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b';')
    .add(b'=')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'^')
    .add(b'`')
    .add(b'\\');

fn percent_encode(input: &str) -> Cow<str> {
    percent_encoding::percent_encode(input.as_bytes(), SET).into()
}

fn hmac(key: impl AsRef<[u8]>, data: &str) -> impl AsRef<[u8]> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_ref()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    mac.finalize_fixed()
}

impl AwsSigner {
    pub fn new(
        access_key: String,
        access_secret: impl AsRef<str>,
        region: String,
        service: String,
    ) -> Self {
        AwsSigner {
            access_key,
            access_secret: format!("AWS4{}", access_secret.as_ref()),
            region,
            service,
        }
    }

    fn generate_signing_key(&self, date: &str) -> impl AsRef<[u8]> + '_ {
        let tag = hmac(&self.access_secret, date);
        let tag = hmac(tag, &self.region);
        let tag = hmac(tag, &self.service);
        hmac(tag, "aws4_request")
    }

    pub fn sign(&self, mut req: Request) -> Request {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_scope = now.format("%Y%m%d").to_string();

        let payload: Cow<'static, str> = req.body().map_or(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            |b| {
                b.as_bytes().map_or("UNSIGNED-PAYLOAD".into(), |s| {
                    let mut hasher = Sha256::new();
                    hasher.update(s);
                    hex::encode(hasher.finalize()).into()
                })
            },
        );

        {
            let headers = req.headers_mut();
            headers.insert("x-amz-content-sha256", payload.parse().unwrap());
            headers.insert("x-amz-date", amz_date.parse().unwrap());

            if !headers.contains_key("host") {
                let url = req.url();
                if let Some(host) = url.host_str() {
                    let port = url.port();
                    let host_header = match port {
                        Some(p) => format!("{host}:{p}"),
                        None => host.to_string(),
                    };
                    req.headers_mut()
                        .insert("host", host_header.parse().unwrap());
                }
            }
        }

        let url = req.url();
        let path = url.path();
        let encoded_path = if path.is_empty() { "/" } else { path };

        // Canonical query string
        let canonical_query = url
            .query_pairs()
            .sorted()
            .map(|(k, v)| format!("{}={}", percent_encode(&k), percent_encode(&v)))
            .join("&");

        // Prepare headers for canonical form
        let header_map = req
            .headers()
            .into_iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_lowercase(),
                    v.to_str().map(str::trim).unwrap_or(""),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let signed_headers = header_map.keys().join(";");
        let canonical_headers = header_map
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .join("\n");

        let canonical_request = format!(
            "{method}\n{uri}\n{query}\n{headers}\n\n{signed}\n{payload}",
            method = req.method(),
            uri = encoded_path,
            query = canonical_query,
            headers = canonical_headers,
            signed = signed_headers,
            payload = payload
        );
        let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{date}/{region}/{service}/aws4_request\n{hash}",
            timestamp = amz_date,
            date = date_scope,
            region = self.region,
            service = self.service,
            hash = canonical_hash
        );

        let signing_key = self.generate_signing_key(&date_scope);
        let signature = hex::encode(hmac(signing_key, &string_to_sign));

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request,SignedHeaders={},Signature={}",
            self.access_key, date_scope, self.region, self.service, signed_headers, signature
        );

        req.headers_mut()
            .insert("Authorization", auth_header.parse().unwrap());
        req
    }

    pub fn sign_url(&self, mut url: Url, expire: Duration) -> Url {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_scope = now.format("%Y%m%d").to_string();

        let host = if let Some(host) = url.host_str() {
            let port = url.port();
            match port {
                Some(p) => format!("{host}:{p}"),
                None => host.to_string(),
            }
        } else {
            panic!("URL must have a host");
        };

        // Prepare headers for canonical form
        let header_map = [("host", host)];
        let signed_headers = header_map.iter().map(|(k, _)| k).join(";");
        let canonical_headers = header_map
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .join("\n");

        {
            let mut q = url.query_pairs_mut();
            q.append_pair("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
            q.append_pair(
                "X-Amz-Credential",
                &format!(
                    "{}/{}/{}/{}/aws4_request",
                    self.access_key, date_scope, self.region, self.service
                ),
            );
            q.append_pair("X-Amz-Date", &amz_date);
            q.append_pair("X-Amz-Expires", expire.as_secs().to_string().as_str());
            q.append_pair("X-Amz-SignedHeaders", &signed_headers);
        }
        // Canonical query string
        let canonical_query = url
            .query_pairs()
            .sorted()
            .map(|(k, v)| format!("{}={}", percent_encode(&k), percent_encode(&v)))
            .join("&");

        let path = url.path();
        let encoded_path = if path.is_empty() { "/" } else { path };
        let canonical_request = format!(
            "{method}\n{uri}\n{query}\n{headers}\n\n{signed}\n{payload}",
            method = "GET",
            uri = encoded_path,
            query = canonical_query,
            headers = canonical_headers,
            signed = signed_headers,
            payload = "UNSIGNED-PAYLOAD"
        );
        let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{date}/{region}/{service}/aws4_request\n{hash}",
            timestamp = amz_date,
            date = date_scope,
            region = self.region,
            service = self.service,
            hash = canonical_hash
        );

        let signing_key = self.generate_signing_key(&date_scope);
        let signature = hex::encode(hmac(signing_key, &string_to_sign));

        {
            let mut q = url.query_pairs_mut();
            q.append_pair("X-Amz-Signature", &signature);
        }
        url
    }
}
