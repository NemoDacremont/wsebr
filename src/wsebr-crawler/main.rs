use argh::{self, FromArgs};
use chrono::Utc;
use deadpool_sqlite::{Config, Hook, Pool, Runtime};
use feed_rs::model::Feed;
use feed_rs::parser::{self};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use rusqlite::OptionalExtension;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, BufReader};
use wsebr::{WebPage, get_web_page_by_link};

#[derive(FromArgs, PartialEq, Debug)]
/// Crawler for wsebr
struct WSEBRCrawler {
    /// path to a file containing a list of url containing urls to RSS feeds
    #[argh(option, short = 'f', default = "String::from(\"./smallweb.txt\")")]
    rss_file: String,

    /// path to a file containing a list of url containing urls to RSS feeds
    #[argh(option, short = 'j', default = "100")]
    concurrent_requests: usize,

    #[argh(subcommand)]
    subcommand: SubCmds,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCmds {
    Increment(IncrementSubCmd),
    Local(LocalSubCmd),
}

#[derive(FromArgs, PartialEq, Debug)]
/// increment command, will insert new pages directly in the database
#[argh(subcommand, name = "increment")]
struct IncrementSubCmd {
    /// path to the `SQlite` database
    #[argh(option, short = 'd', default = "String::from(\"./database.db\")")]
    database: String,

    /// path to a file containing the stopwords to ignore
    #[argh(option, default = "\"stopwords.txt\".to_string()")]
    stop_words_file: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// local command, will save the results in an output directory
#[argh(subcommand, name = "local")]
struct LocalSubCmd {
    /// path to the directory where files will be saved
    #[argh(option, short = 'O', default = "String::from(\"./crawled\")")]
    output_dir: String,
}

// We pass `Client` by value here. Because `reqwest::Client` internally
// wraps its state in an Arc, cloning it is extremely cheap and is the
// standard way to share it across async tasks.
async fn local_crawl(url: String, out_dir: &str, client: Client) {
    let mut parts = url.split('/').skip(2);
    let domain = parts.next().expect("couldn't retrieve domain from URL");
    let filename = parts.last().unwrap_or("index.html");

    if domain == "simturax.com" {
        println!("{} - blacklisted", domain);
        return; // Added return so it actually skips fetching
    }

    let directory = format!("{}/{}/", out_dir, domain);
    let out_path = format!("{}{}", directory, filename);

    // Uncomment if you want to skip existing files :clown:
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

async fn increment_feed(
    pool: &Pool,
    feed: Feed,
    stop_words: Arc<HashSet<String>>,
) -> Result<(), rusqlite::Error> {
    let web_pages: Vec<WebPage> = feed
        .entries
        .iter()
        .map(|entry| WebPage::from(entry.clone()))
        .collect();

    let conn = pool.get().await;
    if conn.is_err() {
        println!("couldn't get conn");
        return Ok(());
    }

    let conn = conn.unwrap();

    for mut web_page in web_pages {
        let stop_words = stop_words.to_owned();
        let link = web_page.url.to_owned();
        let curr = conn
            .interact(move |connection| get_web_page_by_link(connection, &link))
            .await;

        if curr.is_err() {
            println!("Fail because of interact error");
            continue;
        }
        let curr = curr.unwrap().optional();

        if curr.is_err() {
            println!("Err during sql query to get");
            continue;
        }
        let curr = curr.unwrap();

        if let Some(curr) = curr {
            if web_page.publish_date == curr.last_update ||
                web_page.publish_date > Utc::now() ||
                !web_page.url.starts_with("http") {
                continue;
            }
        }

        let _ = conn
            .interact(move |connection| {
                let _ = web_page.build(connection, &stop_words);
            })
            .await;
    }

    Ok(())
}

// We pass `Client` by value here. Because `reqwest::Client` internally
// wraps its state in an Arc, cloning is cheap
async fn increment_crawl(
    url: String,
    pool: Arc<Pool>,
    client: Client,
    stop_words: Arc<HashSet<String>>,
) {
    let res = match client.get(&url).send().await {
        Ok(response) => response,
        Err(err) => {
            println!("{} - Couldn't fetch: {}", url, err);
            return;
        }
    };

    if res.status().is_success() {
        // Fetch the body bytes and write them to disk asynchronously
        match res.text().await {
            Ok(text) => {
                let feed = parser::parse(text.as_bytes());
                if feed.is_ok() {
                    let _ = increment_feed(&pool, feed.unwrap(), stop_words).await;
                }
            }
            Err(e) => println!("{} - Failed to read response body: {}", url, e),
        }
    } else {
        println!("{} - Couldn't fetch, status {}", url, res.status());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: WSEBRCrawler = argh::from_env();
    let url_path = args.rss_file;

    let url_file = File::open(url_path)
        .await
        .expect("Failed to open urls_file");
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

    let max_concurrent_requests = args.concurrent_requests;

    match args.subcommand {
        SubCmds::Local(subcmd) => {
            stream::iter(urls)
                .map(|url| {
                    let client = global_client.clone();
                    let output_dir = subcmd.output_dir.to_owned();

                    async move {
                        local_crawl(url, &output_dir, client).await;
                    }
                })
                .buffer_unordered(max_concurrent_requests)
                .collect::<Vec<()>>()
                .await;
        }
        SubCmds::Increment(subcmd) => {
            let client = global_client.clone();
            let cfg = Config::new(subcmd.database);
            let pool = cfg
                .builder(Runtime::Tokio1)
                .unwrap()
                .max_size(20)
                .post_create(Hook::async_fn(|conn, _| {
                    Box::pin(async move {
                        conn.interact(|c| rusqlite::vtab::array::load_module(c))
                            .await
                            .unwrap()
                            .unwrap();
                        Ok(())
                    })
                }))
                .build()
                .unwrap();
            let pool = Arc::new(pool);

            let mut stop_words = HashSet::with_capacity(10000);

            let file = File::open(subcmd.stop_words_file).await.unwrap();
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await.unwrap() {
                stop_words.insert(line);
            }
            let stop_words = Arc::new(stop_words);

            stream::iter(urls)
                .map(|url| async {
                    let stop_words = stop_words.clone();
                    let pool = pool.clone();
                    let client = client.clone();
                    increment_crawl(url, pool, client, stop_words).await;
                })
                .buffer_unordered(max_concurrent_requests)
                .collect::<Vec<()>>()
                .await;
        }
    }

    Ok(())
}
