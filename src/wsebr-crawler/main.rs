use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::{env::args, time::Duration};
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, BufReader};

// We pass `Client` by value here. Because `reqwest::Client` internally 
// wraps its state in an Arc, cloning it is extremely cheap and is the 
// standard way to share it across async tasks.
async fn crawl(url: String, client: Client) {
    let mut parts = url.split('/').skip(2);
    let domain = parts.next().expect("couldn't retrieve domain from URL");
    let filename = parts.last().unwrap_or("index.html");

    if domain == "simturax.com" {
        println!("{} - blacklisted", domain);
        return; // Added return so it actually skips fetching
    }

    let directory = format!("./crawled/{}/", domain);
    let out_path = format!("{}{}", directory, filename);

    // Uncomment if you want to skip existing files
    // if tokio::fs::try_exists(&out_path).await.unwrap_or(false) {
    //     return;
    // }

    let res = match client.get(&url).send().await {
        Ok(response) => response,
        Err(err) => {
            println!("{} - Couldn't fetch: {}", url, err);
            return;
        }
    };

    if res.status().is_success() {
        // Use tokio::fs for non-blocking directory creation
        if let Err(e) = fs::create_dir_all(&directory).await {
            println!("{} - Failed to create directory: {}", url, e);
            return;
        }

        // Fetch the body bytes and write them to disk asynchronously
        match res.bytes().await {
            Ok(bytes) => {
                if let Err(e) = fs::write(&out_path, bytes).await {
                    println!("{} - Failed to write to file: {}", url, e);
                } else {
                    println!("{} - Fetched", url);
                }
            }
            Err(e) => println!("{} - Failed to read response body: {}", url, e),
        }
    } else {
        // println!("{} - Couldn't fetch, status {}", url, res.status());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url_path = args().nth(1).expect("Usage: ./crawler <urls_file>");

    let url_file = File::open(url_path).await.expect("Failed to open urls_file");
    let reader = BufReader::new(url_file);
    let mut lines = reader.lines();

    let mut urls = Vec::new();
    while let Some(line) = lines.next_line().await? {
        if !line.trim().is_empty() {
            urls.push(line);
        }
    }

    let global_client = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        .danger_accept_invalid_hostnames(true)
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(10))
        .tcp_keepalive(None)
        .build()?;

    let max_concurrent_requests = 100;

    stream::iter(urls)
        .map(|url| {
            let client = global_client.clone();
            async move {
                crawl(url, client).await;
            }
        })
        .buffer_unordered(max_concurrent_requests)
        .collect::<Vec<()>>()
        .await;

    Ok(())
}
