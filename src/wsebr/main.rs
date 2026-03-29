use argh::FromArgs;
use chrono::Utc;
use feed_rs::parser;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;
use wsebr::*;

fn find_files(path: PathBuf) -> Vec<PathBuf> {
    if Path::is_file(&path) {
        return vec![path];
    }
    let mut paths = Vec::new();
    let dirent = fs::read_dir(path).expect("Can't read ./crawled");

    for dirent_res in dirent {
        match dirent_res {
            Ok(dirent_cur) => {
                let subpath = dirent_cur.path();
                if Path::is_file(&subpath) {
                    paths.push(subpath);
                } else if Path::is_dir(&subpath) {
                    paths.extend(find_files(subpath));
                }
            }
            Err(e) => println!("Couldn't read dirent; skipping : {e}"),
        }
    }

    paths
}

fn save_web_pages(connection: &Connection, paths: &str) -> Result<(), rusqlite::Error> {
    let paths = find_files(paths.parse().unwrap());

    let mut start = Instant::now();
    let mut i = 0;
    for path in paths {
        let file = File::open(&path).unwrap();
        let feed_res = parser::parse(BufReader::new(file));

        if feed_res.is_err() {
            continue;
        }

        let feed = feed_res.unwrap();

        let web_pages = feed
            .entries
            .iter()
            .map(|entry| WebPage::from(entry.clone()));

        let mut buffer = Vec::with_capacity(web_pages.len());
        for web_page in web_pages {
            i += 1;
            if i % 10000 == 0 {
                println!(
                    "{i} {} {}",
                    web_page.url,
                    (Instant::now() - start).as_secs_f32()
                );
                start = Instant::now();
            }

            if web_page.publish_date > Utc::now() || !web_page.url.starts_with("http") {
                continue;
            }
            buffer.push(web_page);
        }

        insert_web_pages(&connection, &buffer)?;
    }
    Ok(())
}

fn compute_idfs(connection: &Connection, threshold: f64) -> Result<(), rusqlite::Error> {
    // Counts the number of document in which each token appears
    let mut counts: HashMap<String, i64> = HashMap::new();

    let mut n = 0;
    retrieve_web_page(&connection, |web_page| {
        n += 1;
        let tokens: HashSet<String> = web_page.tokenize().collect();

        for token in tokens {
            counts.insert(token.clone(), counts.get(&token).unwrap_or(&0) + 1);
        }
    })?;
    let n = n as f64;

    let mut start = Instant::now();
    let mut i = 0;
    let mut tokens = Vec::with_capacity(counts.len());
    for (token, idf) in counts {
        let idf = idf as f64;
        let idf = (1. + (n - idf + 0.5) / (idf + 0.5)).log2();

        if idf < threshold {
            continue;
        }

        i += 1;
        if i % 15000 == 0 {
            println!(
                "iter: {i} - inserted {token} idf={idf:.4} in {:.2} secs",
                (Instant::now() - start).as_secs_f32()
            );
            start = Instant::now()
        }

        tokens.push(Token {
            token_id: -1,
            value: token,
            idf: idf,
        });
    }
    insert_tokens(&connection, &tokens)?;

    Ok(())
}

fn compute_scores(connection: &Connection, b: f64, k1: f64) -> Result<(), rusqlite::Error> {
    // Compute avgdl
    let mut avgdl = 0; // arbitrary, should be avg(|D|)
    let mut doc_count = 0;
    retrieve_web_page(connection, |web_page| {
        doc_count += 1;
        avgdl += web_page.tokenize().count();
    })?;
    let avgdl = avgdl as f64 / doc_count as f64;

    // Cache tokens idfs, ok since there is ~300k tokens
    let mut idfs: HashMap<String, Token> = HashMap::new();
    retrieve_tokens(connection, |token| {
        idfs.insert(token.value.to_string(), token);
    })?;

    let mut start = Instant::now();
    let mut k = 0;
    retrieve_web_page(connection, |web_page| {
        k += 1;
        if k % 10000 == 0 {
            println!(
                "{k} - Processing web_page {} in {:.2} secs",
                web_page.url,
                (Instant::now() - start).as_secs_f64()
            );
            start = Instant::now();
        }

        let mut counts: HashMap<String, f64> = HashMap::new();

        let mut n = 0;
        for token in web_page.tokenize() {
            n += 1;
            counts.insert(token.clone(), counts.get(&token).unwrap_or(&0f64) + 1f64);
        }
        let n = n as f64;

        let mut tfs = Vec::with_capacity(counts.len());
        for (token, count) in counts {
            let token = idfs.get(&token);
            // tokens with idf below threshold, will be missing from the db
            if token.is_none() {
                continue;
            }
            let token = token.unwrap();

            tfs.push(Tf {
                token_id: token.token_id,
                web_page_id: web_page.web_page_id,
                tf: token.idf
                    * ((count * (k1 + 1f64)) / (count + k1 * (1f64 - b + b * (n / avgdl)))),
            });
        }

        let res = insert_tfs(&connection, &tfs);
        if res.is_err() {
            println!("Couldn't insert tfs from {}", web_page.url);
            exit(1);
        }
    })?;

    Ok(())
}

#[derive(FromArgs, PartialEq, Debug)]
/// Worst Search Engine But in Rust - Search the small web !
struct WSEBR {
    /// path to the `SQlite` database
    #[argh(option, short = 'd', default = "String::from(\"./database.db\")")]
    database: String,

    #[argh(subcommand)]
    subcommand: SubCmds,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCmds {
    Build(BuildSubCmd),
    Search(SearchSubCmd),
}

#[derive(FromArgs, PartialEq, Debug)]
/// Build the wsebr database
#[argh(subcommand, name = "build")]
struct BuildSubCmd {
    #[argh(subcommand)]
    buildcmd: BuildSubCmds,

    /// rebuild from scratch, drop the table before build
    #[argh(switch)]
    rebuild: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum BuildSubCmds {
    WebPages(BuildWebPagesSubCmd),
    IDFs(BuildIDFsSubCmd),
    Okapi(BuildOkapiSubCmd),
}

#[derive(FromArgs, PartialEq, Debug)]
/// Populate the database with feed files from a directory
#[argh(subcommand, name = "webpages")]
struct BuildWebPagesSubCmd {
    #[argh(positional)]
    /// path to the directory with feed files
    path: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// Tokenize all web pages in the database and compute the IDF for each token
#[argh(subcommand, name = "idf")]
struct BuildIDFsSubCmd {
    /// tokens with idf below this threshold will be discarded
    #[argh(option, default = "5.")]
    threshold: f64,
}

#[derive(FromArgs, PartialEq, Debug)]
/// Build all Okapi BM25 values for each token in the database
#[argh(subcommand, name = "okapi")]
struct BuildOkapiSubCmd {
    /// b constant from the BM25 score
    #[argh(option, default = "0.75")]
    b: f64,

    /// k1 constant from the BM25 score
    #[argh(option, default = "1.6")]
    k1: f64,
}

#[derive(FromArgs, PartialEq, Debug)]
/// Search the wsebr database
#[argh(subcommand, name = "search")]
struct SearchSubCmd {
    #[argh(positional)]
    /// query to search for
    query: Vec<String>,
}

fn main() -> Result<(), rusqlite::Error> {
    let wsebr: WSEBR = argh::from_env();

    let connection = sqlite_init(&wsebr.database)?;

    match wsebr.subcommand {
        SubCmds::Build(build) => match build.buildcmd {
            BuildSubCmds::WebPages(args) => {
                if build.rebuild {
                    drop_web_pages(&connection)?;
                    create_web_page_table(&connection)?;
                }
                save_web_pages(&connection, &args.path)?;
            }

            BuildSubCmds::IDFs(args) => {
                if build.rebuild {
                    drop_tokens(&connection)?;
                    create_token_table(&connection)?;
                }
                compute_idfs(&connection, args.threshold)?;
            }

            BuildSubCmds::Okapi(args) => {
                if build.rebuild {
                    drop_tf(&connection)?;
                    create_tf_table(&connection)?;
                }
                compute_scores(&connection, args.b, args.k1)?;
            }
        },
        SubCmds::Search(args) => {
            let (web_pages, _count) = search_query(&connection, args.query, 1)?;

            let mut i = 1;
            for web_page in web_pages {
                println!("{i} {} {}", web_page.url, web_page.title);
                i += 1
            }
        }
    }

    Ok(())
}
