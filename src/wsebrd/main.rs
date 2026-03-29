use askama::Template;
use axum::{
    Router,
    extract::{Query, State},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use deadpool_sqlite::{Config, Hook, Pool, Runtime};
use reqwest::StatusCode;
use tracing_subscriber::EnvFilter;
use std::{cmp::min, collections::HashMap, time::Instant};
use tokio;
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use wsebr::{IndexStats, WebPage, get_stats, latest_web_pages, random_web_pages, search_query};

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
}

#[tokio::main]
async fn main() -> Result<(), rusqlite::Error> {
    let cfg = Config::new("./database.db");
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
    let index_stats = conn
        .interact(|connection| get_stats(connection).unwrap())
        .await
        .unwrap();

    let app_state = AppState {
        pool: pool,
        index_stats: index_stats,
    };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let app = Router::new()
        .layer(TraceLayer::new_for_http())
        .nest_service("/assets", ServeDir::new("assets"))
        .route("/about", get(about))
        .route("/search", get(search))
        .route("/random", get(random))
        .route("/latest", get(latest))
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

    let query_str = params
        .get("query")
        .map_or("".to_string(), |some| some.to_string());

    let page = params
        .get("page")
        .map_or(1, |page| page.parse::<i64>().unwrap_or(1));

    let query: Vec<String> = query_str
        .split(' ')
        .map(|token| token.to_string())
        .collect();

    let start = Instant::now();
    let (mut results, count) = conn
        .interact(move |connection| search_query(&connection, query, page).unwrap())
        .await
        .unwrap();
    let end = Instant::now();
    truncate_summary(&mut results);

    let title = format!("Results for {query_str}");

    Ok(Html(
        SearchTemplate {
            current_page: page,
            result_count: count,
            page_count: 1 + min(count, 100) / 10,
            query: &query_str,
            search_duration: (end - start).as_millis(),
            search_value: &query_str,
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
        SearchTemplate {
            current_page: 1,
            result_count: 10,
            page_count: 6,
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

async fn about(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    Ok(Html(
        AboutTemplate {
            index_stats: &state.index_stats,
        }
        .render()?,
    ))
}
