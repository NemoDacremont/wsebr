use argh::FromArgs;
use chrono::Utc;
use feed_rs::parser;
use rusqlite::Connection;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing_subscriber::EnvFilter;
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

fn save_web_pages(connection: &Connection, paths: &str, stop_words_file: &str) -> Result<(), rusqlite::Error> {
    let stop_words: HashSet<String> = {
        let file = File::open(stop_words_file).unwrap();
        let reader = BufReader::new(file);
        let words = reader.lines().map(|line| line.unwrap_or_default());
        HashSet::from_iter(words)
    };

    let paths = find_files(paths.parse().unwrap());

    let mut start = Instant::now();
    let mut i = 0;
    let ncount = paths.len();
    // Batch transaction
    connection.execute_batch("BEGIN;")?;

    for path in paths {
        i += 1;
        if i % 100 == 0 {
            connection.execute_batch("COMMIT;")?;
            connection.execute_batch("BEGIN;")?;
            println!(
                "{i}/{ncount} commit {}",
                (Instant::now() - start).as_secs_f32()
            );
            start = Instant::now();
        } else {
            println!("{i}/{ncount}");
        }

        let file = File::open(&path).unwrap();
        let feed = parser::parse(BufReader::new(file));

        // silently fail..... but idc
        if feed.is_err() {
            continue;
        }
        let feed = feed.unwrap();

        let web_pages = feed
            .entries
            .iter()
            .map(|entry| WebPage::from(entry.clone()));

        for mut web_page in web_pages {
            // Ignore invalid web page
            if web_page.publish_date > Utc::now() || !web_page.url.starts_with("http") {
                continue;
            }

            web_page.build(connection, &stop_words)?
        }
    }
    connection.execute_batch("COMMIT;")?;
    Ok(())
}

fn compute_idfs(connection: &Connection) -> Result<(), rusqlite::Error> {
    let stats = get_stats(connection)?;
    let web_pages_count = stats.web_pages_count as f64;

    let mut i = 0;
    connection.execute_batch("BEGIN;")?;
    retrieve_tokens(connection, |token| {
        i += 1;
        if i % 10_000 == 0 {
            connection.execute_batch("COMMIT;").unwrap();
            connection.execute_batch("BEGIN;").unwrap();
        }

        let token_count = token.count as f64;
        let idf = (1. + (web_pages_count - token_count + 0.5) / (token_count + 0.5)).ln();
        update_idf(connection, token.token_id, idf).unwrap();
    })?;
    connection.execute_batch("COMMIT;")?;
    Ok(())
}

fn compute_bm25(connection: &Connection, b: f64, k1: f64) -> Result<(), rusqlite::Error> {
    let stats = get_stats(connection)?;
    let web_page_count = stats.web_pages_count as f64;
    let avgdl = stats.web_page_token_count as f64 / web_page_count;

    recompute_bm25(connection, k1, b, avgdl)
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

    /// path to a file containing the stopwords to ignore
    #[argh(option, default = "\"stopwords.txt\".to_string()")]
    stop_words_file: String,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum BuildSubCmds {
    WebPages(BuildWebPagesSubCmd),
    IDFs(BuildIDFsSubCmd),
    Okapi(BuildOkapiSubCmd),
    ChampionList(BuildChampionListSubCmd),
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
struct BuildIDFsSubCmd {}

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
/// Build the champion list (top 1000 bm25 for each tokens)
#[argh(subcommand, name = "championlist")]
struct BuildChampionListSubCmd {}

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

    let connection = Connection::open(&wsebr.database)?;
    sqlite_init(&connection)?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match wsebr.subcommand {
        SubCmds::Build(build) => match build.buildcmd {
            BuildSubCmds::WebPages(args) => {
                if build.rebuild {
                    drop_web_pages(&connection)?;
                    create_web_page_table(&connection)?;
                }
                save_web_pages(&connection, &args.path, &build.stop_words_file)?;
            }

            BuildSubCmds::IDFs(_) => {
                compute_idfs(&connection)?;
            }

            BuildSubCmds::Okapi(args) => {
                compute_bm25(&connection, args.b, args.k1)?;
            }

            BuildSubCmds::ChampionList(_) => {
                recreate_champion_list(&connection)?;
            }
        },
        SubCmds::Search(args) => {
            let (web_pages, _count) = search_query(&connection, args.query, 1, None, None)?;

            let mut i = 1;
            for web_page in web_pages {
                println!("{i} {} {}", web_page.url, web_page.title);
                i += 1
            }
        }
    }

    Ok(())
}
