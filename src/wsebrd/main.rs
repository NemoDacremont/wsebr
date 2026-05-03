use argh::FromArgs;
use askama::Template;
use axum::{
    Router, extract::{Query, State}, middleware::map_response, response::{Html, IntoResponse, Response}, routing::get
};
use deadpool_sqlite::{Config, Hook, Pool, Runtime};
use reqwest::StatusCode;
use rust_stemmers::{Algorithm, Stemmer};
use std::{cmp::min, collections::HashMap, sync::Arc, time::Instant};
use tokio;
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::EnvFilter;
use wsebr::{
    IndexStats, WebPage, get_stats, latest_web_pages, random_web_pages, search_query, sqlite_init,
    tokenize_str,
};

#[derive(FromArgs, PartialEq, Debug)]
/// daemon server serving small web
struct Wsebrd {
    /// path to the sqlite databae
    #[argh(option, short = 'd', default = "String::from(\"./database.db\")")]
    database: String,
}

#[derive(Template)]
#[template(path = "search.html")]
struct SearchTemplate<'a> {
    current_page: i64,
    result_count: i64,
    page_count: i64,
    query: &'a str,
    title: &'a str,
    search_value: &'a str,
    search_duration: u128,
    search_placeholder: &'a str,
    web_pages: &'a Vec<WebPage>,
}

#[derive(Template)]
#[template(path = "latest.html")]
struct LatestTemplate<'a> {
    current_page: i64,
    result_count: i64,
    page_count: i64,
    title: &'a str,
    search_value: &'a str,
    search_duration: u128,
    search_placeholder: &'a str,
    web_pages: &'a Vec<WebPage>,
}

#[derive(Template)]
#[template(path = "about.html")]
struct AboutTemplate<'a> {
    index_stats: &'a IndexStats,
}

#[derive(Debug, displaydoc::Display, thiserror::Error)]
enum AppError {
    /// could not render template
    Render(#[from] askama::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        #[derive(Debug, Template)]
        #[template(path = "error.html")]
        struct Tmpl {}

        let status = match &self {
            AppError::Render(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let tmpl = Tmpl {};
        if let Ok(body) = tmpl.render() {
            (status, Html(body)).into_response()
        } else {
            (status, "Something went wrong").into_response()
        }
    }
}

#[derive(Clone)]
struct AppState {
    pool: Pool,
    index_stats: IndexStats,
    stemmer: Arc<Stemmer>,
}

#[tokio::main]
async fn main() -> Result<(), rusqlite::Error> {
    let args: Wsebrd = argh::from_env();

    let cfg = Config::new(args.database);
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

    let conn = pool.get().await.unwrap();
    conn.interact(|connection| {
        sqlite_init(&connection)?;

        connection.execute_batch(
            "
            pragma temp_store   = memory;
            pragma mmap_size    = 2873152512;
            ",
        )
    })
    .await
    .unwrap()
    .unwrap();

    let index_stats = conn
        .interact(|connection| get_stats(connection).unwrap())
        .await
        .unwrap();

    let stemmer = Stemmer::create(Algorithm::English);

    let app_state = AppState {
        pool: pool,
        index_stats: index_stats,
        stemmer: Arc::new(stemmer),
    };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let app = Router::new()
        .layer(TraceLayer::new_for_http())
        .nest_service("/assets", ServeDir::new("assets").precompressed_gzip())
        .route("/about", get(about))
        .route("/search", get(search))
        .route("/random", get(random))
        .route("/latest", get(latest))
        .nest_service("/robots.txt", ServeFile::new("templates/robots.txt"))
        .fallback_service(ServeFile::new("templates/home.html"))
        .with_state(app_state);

    tracing::info!("Server running on http://0.0.0.0:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app.layer(TraceLayer::new_for_http()))
        .await
        .unwrap();

    Ok(())
}

fn truncate_summary(web_pages: &mut Vec<WebPage>) {
    for web_page in web_pages.iter_mut() {
        web_page.summary = web_page
            .summary
            .split_whitespace()
            .take(30)
            .collect::<Vec<_>>()
            .join(" ");
    }
}

async fn random(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let conn = state.pool.get().await.unwrap();

    let start = Instant::now();
    let mut results = conn
        .interact(|connection| random_web_pages(&connection).unwrap())
        .await
        .unwrap();
    let end = Instant::now();
    truncate_summary(&mut results);

    Ok(Html(
        SearchTemplate {
            current_page: 1,
            result_count: 10,
            page_count: 2,
            query: "",
            search_duration: (end - start).as_millis(),
            search_value: "",
            search_placeholder: "You're being lucky :)",
            title: "Lucky pages :)",
            web_pages: &results,
        }
        .render()?,
    ))
}

async fn search(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let conn = state.pool.get().await.unwrap();

    let page = params
        .get("page")
        .map_or(1, |page| page.parse::<i64>().unwrap_or(1));

    let query_raw = params
        .get("query")
        .map_or("", |v| v);

    let query_str = query_raw.to_ascii_lowercase().to_string();

    let site = query_str
        .split_whitespace()
        .filter(|word| word.starts_with("site:"))
        .map(|word| word.strip_prefix("site:").unwrap().to_string())
        .next();

    let req_tokens: Vec<String> = query_str
        .split_whitespace()
        .filter(|word| {
            (word.starts_with('\'') && word.ends_with('\''))
                || (word.starts_with('"') && word.ends_with('"'))
        })
        .flat_map(|word| tokenize_str(word, &state.stemmer))
        .collect();

    let req_tokens = if req_tokens.is_empty() {
        None
    } else {
        Some(req_tokens)
    };

    let query_str = query_str
        .split_whitespace()
        .filter(|word| {
            !word.starts_with("site:")
                && !(word.starts_with('\'') && word.ends_with('\''))
                && !(word.starts_with('"') && word.ends_with('"'))
        })
        .collect::<Vec<&str>>()
        .join(" ");

    let query: Vec<String> = tokenize_str(&query_str, &state.stemmer).collect();
    tracing::debug!("{:?},{:?},{:?},{:?}", query_raw, query, site, req_tokens);

    let start = Instant::now();
    let (mut results, count) = conn
        .interact(move |connection| {
            search_query(&connection, query, page, site.as_deref(), req_tokens).unwrap()
        })
        .await
        .unwrap();
    let end = Instant::now();
    truncate_summary(&mut results);

    tracing::debug!("{count}");


    let title = format!("Results for {query_raw}");

    Ok(Html(
        SearchTemplate {
            current_page: page,
            result_count: count,
            page_count: 1 + min(count, 100) / 10 + (min(count, 100) % 10 != 0) as i64,
            query: &query_raw,
            search_duration: (end - start).as_millis(),
            search_value: &query_raw,
            search_placeholder: "",
            title: &title,
            web_pages: &results,
        }
        .render()?,
    ))
}

async fn latest(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let conn = state.pool.get().await.unwrap();

    let page = params
        .get("page")
        .map_or(1, |page| page.parse::<i64>().unwrap_or(1));

    let start = Instant::now();
    let mut results = conn
        .interact(move |connection| latest_web_pages(&connection, page).unwrap())
        .await
        .unwrap();
    let end = Instant::now();
    truncate_summary(&mut results);

    Ok(Html(
        LatestTemplate {
            current_page: page,
            result_count: 10,
            page_count: 11,
            search_duration: (end - start).as_millis(),
            search_value: "",
            search_placeholder: "These are the latest publication",
            title: "Latest publication",
            web_pages: &results,
        }
        .render()?,
    ))
}

async fn about(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    Ok(Html(
        AboutTemplate {
            index_stats: &state.index_stats,
        }
        .render()?,
    ))
}
