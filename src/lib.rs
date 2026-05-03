use chrono::{DateTime, Utc};
use feed_rs::model::Entry;
use htmlentity::entity::{self, ICodedDataTrait};
use nanohtml2text::html2text;
use rusqlite::types::Value;
use rusqlite::vtab::array::load_module;
use rusqlite::{Connection, OptionalExtension, params};
use rust_stemmers::{Algorithm, Stemmer};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

thread_local! {
    pub static EN_STEMMER: Stemmer = Stemmer::create(Algorithm::English);
}

pub fn sqlite_init(connection: &Connection) -> Result<(), rusqlite::Error> {
    // Enable the use of `rarray` in queries
    load_module(connection)?;

    create_web_page_table(connection)?;
    create_token_table(connection)?;
    create_tf_table(connection)?;
    create_index_stats_table(connection)?;

    connection.execute_batch(
        "
        pragma temp_store           = memory;
        pragma mmap_size            = 2873152512;
        pragma page_size            = 4096;
        pragma journal_mode         = WAL;
        pragma synchronous          = NORMAL;
        pragma journal_size_limit   = 134217728;
        pragma wal_autocheckpoint   = 20000;
        pragma foreign_keys         = ON;
    ",
    )
}

pub struct WebPage {
    pub web_page_id: i64,
    pub title: String,
    pub summary: String,
    pub url: String,
    pub token_count: i64,
    pub last_update: DateTime<Utc>,
    pub publish_date: DateTime<Utc>,
}

pub fn tokenize_str(s: &str, stemmer: &Stemmer) -> impl Iterator<Item = String> {
    s.split(|c: char| !c.is_ascii() || c.is_ascii_whitespace() || c.is_ascii_punctuation())
        .map(|word| {
            word.chars()
                .filter(|c| c.is_ascii_alphanumeric())
                .map(|c| c.to_ascii_lowercase())
                .collect::<String>()
        })
        .filter(|token| !token.is_empty())
        .map(move |token| stemmer.stem(&token).into_owned())
}

impl WebPage {
    pub fn tokenize(&self, stemmer: &Stemmer) -> impl Iterator<Item = String> {
        let title_iter = tokenize_str(&self.title, stemmer);
        let summary_iter = tokenize_str(&self.summary, stemmer);

        let url_parts = self.url.split('/').skip(2);

        let domain_iter = url_parts
            .clone()
            .take(1)
            .flat_map(|domain| tokenize_str(domain, stemmer));
        let path_iter = url_parts
            .skip(1)
            .flat_map(|part| tokenize_str(part, stemmer));

        title_iter
            .chain(summary_iter)
            .chain(domain_iter)
            .chain(path_iter)
    }

    // This method will update the internal token count
    pub fn build(
        &mut self,
        connection: &Connection,
        stop_words: &HashSet<String>,
    ) -> Result<(), rusqlite::Error> {
        if let Some(page) = get_web_page_by_link(connection, &self.url).optional()? {
            if page.publish_date >= self.publish_date {
                return Ok(());
            }
        }

        let mut count: HashMap<String, i64> = HashMap::with_capacity(300);
        let mut tokens_count = 0;

        EN_STEMMER.with(|stemmer| {
            for token in self
                .tokenize(&stemmer)
                .filter(|token| !stop_words.contains(token.as_str()))
            {
                tokens_count += 1;
                *count.entry(token).or_insert(0) += 1;
            }
        });

        self.token_count = tokens_count;
        let web_page_id = upsert_web_page(connection, self)?;

        for (token, count) in count {
            let token_id = upsert_token(connection, &token)?;
            upsert_tf(connection, web_page_id, token_id, &count)?;
        }

        Ok(())
    }
}

impl From<Entry> for WebPage {
    fn from(entry: Entry) -> Self {
        let title = match entry.title {
            Some(text) => entity::decode(html2text(text.content.as_str()).as_bytes())
                .to_string()
                .unwrap_or_default(),
            None => "".to_string(),
        };

        let summary = match entry.summary {
            Some(text) => entity::decode(html2text(text.content.as_str()).as_bytes())
                .to_string()
                .unwrap_or_default(),
            None => "".to_string(),
        };

        let url = match entry
            .links
            .iter()
            .find(|link| link.rel.as_ref().is_none_or(|val| val == "alternate"))
        {
            Some(link) => link.clone().href,
            None => "".to_string(),
        };

        Self {
            web_page_id: 0,
            title,
            summary,
            url,
            token_count: 0,
            last_update: Utc::now(),
            publish_date: entry.published.unwrap_or_default(),
        }
    }
}

pub fn create_index_stats_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS indexStat
        (
            indexStatId         INTEGER PRIMARY KEY,
            webPageCount        INTEGER,
            webPageTokenCount   INTEGER,
            tfCount             INTEGER,
            tokenCount          INTEGER
        );

        INSERT OR IGNORE INTO indexStat VALUES (1, 0, 0, 0, 0);

        CREATE TRIGGER IF NOT EXISTS insertWebPageStats
        AFTER INSERT ON webPage
        BEGIN
            UPDATE indexStat
            SET webPageCount        = webPageCount + 1,
                webPageTokenCount   = webPageTokenCount + NEW.tokenCount
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS deleteWebPageStats
        AFTER DELETE ON webPage
        BEGIN
            UPDATE indexStat
            SET webPageCount        = webPageCount - 1,
                webPageTokenCount   = webPageTokenCount - OLD.tokenCount
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS updateWebPageStats
        AFTER UPDATE ON webPage
        BEGIN
            UPDATE indexStat
            SET webPageTokenCount = webPageTokenCount + NEW.tokenCount - OLD.tokenCount
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS insertTokenStats
        AFTER INSERT ON token
        BEGIN
            UPDATE indexStat
            SET tokenCount = tokenCount + 1
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS deleteTokenStats
        AFTER DELETE ON token
        BEGIN
            UPDATE indexStat
            SET tokenCount = tokenCount - 1
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS insertTfStats
        AFTER INSERT ON tf
        BEGIN
            UPDATE indexStat
            SET tfCount = tfCount + 1
            WHERE indexStatId = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS deleteTfStats
        AFTER DELETE ON token
        BEGIN
            UPDATE indexStat
            SET tfCount = tfCount - 1
            WHERE indexStatId = 1;
        END;
    ",
    )
}

pub fn create_web_page_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_web_page = "
        CREATE TABLE IF NOT EXISTS webPage
        (
            webPageId   INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT    NOT NULL,
            summary     TEXT    NOT NULL,
            link        TEXT    NOT NULL,
            tokenCount  INTEGER NOT NULL,
            lastUpdate  INTEGER NOT NULL,
            publishDate INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS webPageLink ON webPage (link);
        CREATE INDEX IF NOT EXISTS webPagePublishDate ON webPage (publishDate DESC);
    ";
    connection.execute_batch(create_web_page)
}

pub fn drop_web_pages(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE webPage")
}

pub fn upsert_web_page(
    connection: &Connection,
    web_page: &WebPage,
) -> Result<i64, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO webPage
        (title, summary, link, tokenCount, lastUpdate, publishDate) VALUES
        (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(link) 
        DO UPDATE SET title=excluded.title, summary=excluded.summary, lastUpdate=excluded.lastUpdate, publishDate=excluded.publishDate
        RETURNING webPageId
        ",
    )?;

    stmt.query_row(
        (
            web_page.title.as_str(),
            web_page.summary.as_str(),
            web_page.url.as_str(),
            web_page.token_count,
            Utc::now().timestamp_millis(),
            web_page.publish_date.timestamp_millis(),
        ),
        |row| row.get(0),
    )
}

pub fn insert_web_pages(
    connection: &Connection,
    web_pages: &Vec<WebPage>,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO webPage
        (title, summary, link, tokenCount, lastUpdate, publishDate) VALUES
        (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(link)
        DO UPDATE SET title=excluded.title, summary=excluded.summary, lastUpdate=excluded.lastUpdate, publishDate=excluded.publishDate
        ",
    )?;
    connection.execute("BEGIN", [])?;
    for web_page in web_pages {
        stmt.execute((
            web_page.title.as_str(),
            web_page.summary.as_str(),
            web_page.url.as_str(),
            web_page.token_count,
            Utc::now().timestamp_millis(),
            web_page.publish_date.timestamp_millis(),
        ))?;
    }
    connection.execute("COMMIT", [])
}

pub fn get_web_page_by_link(
    connection: &Connection,
    link: &str,
) -> Result<WebPage, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
            SELECT webPageId, title, summary, tokenCount, publishDate, lastUpdate
            FROM webPage
            WHERE link = ?1
        ",
    )?;

    stmt.query_one((link,), |row| {
        Ok(WebPage {
            web_page_id: row.get(0)?,
            title: row.get(1)?,
            summary: row.get(2)?,
            url: link.to_string(),
            token_count: row.get(3)?,
            publish_date: DateTime::from_timestamp_millis(row.get(4)?).unwrap(),
            last_update: DateTime::from_timestamp_millis(row.get(5)?).unwrap(),
        })
    })
}

pub fn retrieve_web_page<F: FnMut(WebPage)>(
    connection: &Connection,
    mut cb: F,
) -> Result<(), rusqlite::Error> {
    let batch_size: i64 = 1000;
    let mut last_id: i64 = 0;

    let mut i = 0;
    loop {
        i += 1;
        if i > 10000 {
            break;
        }

        let batch: Vec<WebPage> = {
            let mut stmt = connection.prepare_cached(
                "
            SELECT webPageId, title, summary, link, tokenCount, publishDate, lastUpdate
            FROM webPage
            WHERE webPageId > ?
            ORDER BY webPageId asc
            LIMIT ?
        ",
            )?;

            let web_pages = stmt.query_map(params![last_id, batch_size], |row| {
                Ok(WebPage {
                    web_page_id: row.get(0)?,
                    title: row.get(1)?,
                    summary: row.get(2)?,
                    url: row.get(3)?,
                    token_count: row.get(4)?,
                    publish_date: DateTime::from_timestamp_millis(row.get(5)?).unwrap(),
                    last_update: DateTime::from_timestamp_millis(row.get(6)?).unwrap(),
                })
            })?;

            let mut current_batch = Vec::with_capacity(batch_size as usize);
            for web_page in web_pages {
                current_batch.push(web_page?);
            }

            current_batch
        };

        if batch.is_empty() {
            break;
        }

        last_id = batch.last().unwrap().web_page_id;
        for web_page in batch {
            cb(web_page);
        }
    }

    Ok(())
}

pub struct Token {
    pub token_id: i64,
    pub value: String,
    pub idf: f64,
    pub count: i64,
}

impl Clone for Token {
    fn clone(&self) -> Token {
        Token {
            token_id: self.token_id,
            value: self.value.clone(),
            idf: self.idf,
            count: self.count,
        }
    }
}

pub fn create_token_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_token = "
        CREATE TABLE IF NOT EXISTS token
        (
            tokenId INTEGER PRIMARY KEY AUTOINCREMENT,
            value BLOB,
            idf REAL,
            count INTEGER
        );
        CREATE UNIQUE INDEX IF NOT EXISTS token_value ON token (value);
    ";
    connection.execute_batch(create_token)
}

pub fn drop_tokens(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE token")
}

pub fn retrieve_token(connection: &Connection, value: &str) -> Result<Token, rusqlite::Error> {
    let mut stmt =
        connection.prepare_cached("select tokenId, idf, count from tokens where value = ?1")?;
    stmt.query_row((value,), |row| {
        Ok(Token {
            token_id: row.get(0)?,
            value: value.to_string(),
            idf: row.get(1)?,
            count: row.get(2)?,
        })
    })
}

pub fn retrieve_tokens<F: FnMut(Token)>(
    connection: &Connection,
    mut cb: F,
) -> Result<(), rusqlite::Error> {
    let batch_size: i64 = 10000;
    let mut last_id: i64 = 0;

    let mut i = 0;
    loop {
        i += 1;
        if i > 10000 {
            break;
        }

        let batch: Vec<Token> = {
            let mut stmt = connection.prepare_cached(
                "
            SELECT tokenId, value, idf, count
            FROM token
            WHERE tokenId > ?1
            ORDER BY tokenId asc
            LIMIT ?2
        ",
            )?;

            let web_pages = stmt.query_map(params![last_id, batch_size], |row| {
                Ok(Token {
                    token_id: row.get(0)?,
                    value: row.get(1)?,
                    idf: row.get(2)?,
                    count: row.get(3)?,
                })
            })?;

            let mut current_batch = Vec::with_capacity(batch_size as usize);
            for web_page in web_pages {
                current_batch.push(web_page?);
            }

            current_batch
        };

        if batch.is_empty() {
            break;
        }

        last_id = batch.last().unwrap().token_id;
        for web_page in batch {
            cb(web_page);
        }
    }

    Ok(())
}

pub fn upsert_token(connection: &Connection, value: &str) -> Result<i64, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO token
        (value, idf, count) VALUES
        (?1, 0, 1)
        ON CONFLICT(value)
        DO UPDATE SET count=count+1
        RETURNING tokenId
        ",
    )?;

    stmt.query_row((value,), |row| row.get(0))
}

pub fn update_idf(
    connection: &Connection,
    token_id: i64,
    idf: f64,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare_cached("UPDATE token SET idf = ?2 WHERE tokenId = ?1")?;

    stmt.execute((token_id, idf))
}

#[derive(Debug)]
pub struct Tf {
    pub token_id: i64,
    pub web_page_id: i64,
    pub tf: f64,
    pub count: i64,
}

pub fn create_tf_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_tf = "
        CREATE TABLE IF NOT EXISTS tf
        (
            tokenId INTEGER,
            webPageId INTEGER,
            tf REAL,
            count INTEGER,
            FOREIGN KEY(tokenId) REFERENCES token(tokenId),
            FOREIGN KEY(webPageId) REFERENCES webPage(webPageId)
            PRIMARY KEY (tokenId, webPageId)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS idx_tf_webpage_tf ON tf(webPageId, tf);
    ";
    connection.execute_batch(create_tf)
}

pub fn drop_tf(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE tf")
}

pub fn update_bm25(
    connection: &Connection,
    token_id: i64,
    web_page_id: i64,
    bm25: f64,
) -> Result<usize, rusqlite::Error> {
    let mut stmt =
        connection.prepare_cached("UPDATE tf SET tf = ?3 WHERE tokenId = ?1 AND webPageId = ?2")?;

    stmt.execute((token_id, web_page_id, bm25))
}

pub fn recreate_champion_list(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS champion_bm25;
    CREATE TABLE champion_bm25 AS
        SELECT tokenId, webPageId, tf, rn FROM (
            SELECT tokenId, webPageId, tf,
                   ROW_NUMBER() OVER(PARTITION BY tokenId ORDER BY tf DESC) as rn
            FROM tf
        ) WHERE rn <= 10000;
    COMMIT;
    CREATE UNIQUE INDEX idx_champion ON champion_bm25(tokenId, webPageId);
    ",
    )
}

pub fn create_tmptf_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_tmptf = "
        PRAGMA foreign_keys = OFF;
        DROP TABLE IF EXISTS tmptf;
        CREATE TABLE IF NOT EXISTS tmptf
        (
            tokenId INTEGER,
            webPageId INTEGER,
            tf REAL,
            count INTEGER,
            FOREIGN KEY(tokenId) REFERENCES token(tokenId),
            FOREIGN KEY(webPageId) REFERENCES webPage(webPageId)
            PRIMARY KEY (tokenId, webPageId)
        ) WITHOUT ROWID;
    ";
    connection.execute_batch(create_tmptf)
}

pub fn upsert_tmptf(
    connection: &Connection,
    web_page_id: i64,
    token_id: i64,
    bm25: f64,
    count: i64,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO tf
        (tokenId, webPageId, tf, count) VALUES
        (?1, ?2, ?3, ?4)
        ON CONFLICT(tokenId, webPageId)
        DO UPDATE SET count = excluded.count, tf = excluded.tf
        ",
    )?;

    stmt.execute((token_id, web_page_id, bm25, count))
}

pub fn move_tmptf_to_tf(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "
        BEGIN TRANSACTION;
        DROP INDEX IF EXISTS idx_tf_webpage_tf;
        DROP TABLE tf;
        ALTER TABLE tmptf RENAME TO tf;
        COMMIT;

        CREATE INDEX IF NOT EXISTS idx_tf_webpage_tf ON tf(webPageId, tf);
        PRAGMA foreign_keys = ON;
        ",
    )
}

// Highly specialized function that uses powerful optimizations
pub fn recompute_bm25(
    connection: &Connection,
    k1: f64,
    b: f64,
    avgdl: f64,
) -> Result<(), rusqlite::Error> {
    create_tmptf_table(connection)?;

    let batch_size: i64 = 100_000;
    let mut last_token_id: i64 = 0;
    let mut last_web_page_id: i64 = 0;
    if let Ok((t_id, w_id)) = connection.query_row(
        "SELECT tokenId, webPageId FROM tf ORDER BY tokenId ASC, webPageId ASC LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ) {
        last_token_id = t_id;
        last_web_page_id = w_id;
    }

    let mut start = Instant::now();
    let mut i = 0;
    connection.execute_batch("BEGIN TRANSACTION;")?;
    loop {
        i += 1;
        if i > 10000 {
            break; // Failsafe
        }

        // 2. Use Row Value Comparison to paginate safely
        let mut stmt = connection.prepare_cached(
            "
        INSERT INTO tmptf (webPageId, tokenId, tf, count)
        SELECT tf.webPageId, tokenId, (count + ?1 + 1) / (count + ?1 - ?2 + ?2 * (webPage.tokenCount / ?3)), count
        FROM tf
        JOIN webPage ON tf.webPageId = webPage.webPageId
        WHERE (tf.tokenId, tf.webPageId) > (?4, ?5)
        ORDER BY tf.tokenId ASC, tf.webPageId ASC
        LIMIT ?6
        ",
        )?;

        let rows_count = stmt.execute(params![
            k1,
            b,
            avgdl,
            last_token_id,
            last_web_page_id,
            batch_size,
        ])?;

        if rows_count == 0 {
            connection.execute_batch("COMMIT;")?;
            break;
        }

        connection.execute_batch("COMMIT;")?;
        if let Ok((t_id, w_id)) = connection.query_row(
            "SELECT tokenId, webPageId FROM tmptf ORDER BY tokenId DESC, webPageId DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ) {
            last_token_id = t_id;
            last_web_page_id = w_id;
        }

        tracing::debug!(
            "progress {} {} {} {:.4}s",
            i,
            last_token_id,
            last_web_page_id,
            (Instant::now() - start).as_secs_f32()
        );
        start = Instant::now();
        connection.execute_batch("BEGIN TRANSACTION;")?;
    }

    move_tmptf_to_tf(connection)
}

pub fn upsert_tf(
    connection: &Connection,
    web_page_id: i64,
    token_id: i64,
    count: &i64,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO tf
        (tokenId, webPageId, tf, count) VALUES
        (?1, ?2, 0, ?3)
        ON CONFLICT(tokenId, webPageId)
        DO UPDATE SET count = excluded.count
        ",
    )?;

    stmt.execute((token_id, web_page_id, count))
}

pub fn insert_tfs(connection: &Connection, tfs: &Vec<Tf>) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        INSERT INTO tf
        (tokenId, webPageId, tf) VALUES
        (?1, ?2, ?3)
        ON CONFLICT(tokenId, webPageId)
        DO UPDATE SET tf=excluded.tf
        ",
    )?;

    connection.execute("BEGIN", [])?;
    for tf in tfs {
        stmt.execute((tf.token_id, tf.web_page_id, tf.tf))?;
    }
    connection.execute("COMMIT", [])
}

pub fn get_token_ids(
    connection: &Connection,
    tokens: &Vec<String>,
) -> Result<Vec<i64>, rusqlite::Error> {
    let mut stmt = connection.prepare_cached(
        "
        SELECT tokenId
        FROM token
        WHERE value in rarray(?)
    ",
    )?;

    let values = Rc::new(
        tokens
            .clone()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<Value>>(),
    );

    let mut out = Vec::new();
    for row in stmt.query_map((values,), |row| row.get(0))? {
        let token_id = row?;
        out.push(token_id);
    }
    Ok(out)
}

pub fn get_web_pages_req(
    connection: &Connection,
    req_tokens: Vec<i64>,
) -> Result<Vec<i64>, rusqlite::Error> {
    if req_tokens.is_empty() {
        return Ok(Vec::new());
    }

    let mut query = String::from("SELECT webPageId FROM champion_bm25 WHERE tokenId = ?");
    for _ in 1..req_tokens.len() {
        query.push_str(" INTERSECT SELECT webPageId FROM champion_bm25 WHERE tokenId = ?");
    }

    let mut stmt = connection.prepare_cached(&query)?;
    let mut web_page_ids = Vec::new();

    let values = rusqlite::params_from_iter(req_tokens.iter());
    for row in stmt.query_map(values, |row| row.get::<usize, i64>(0))? {
        let web_page_id = row?;
        web_page_ids.push(web_page_id);
    }

    Ok(web_page_ids)
}

pub fn search_query(
    connection: &Connection,
    query: Vec<String>,
    page: i64,
    site: Option<&str>,
    req_tokens: Option<Vec<String>>,
) -> Result<(Vec<WebPage>, i64), rusqlite::Error> {
    let mut out = Vec::with_capacity(10);
    let offset = 10 * (page - 1);

    let mut where_sql = Vec::with_capacity(2);
    let web_page_ids: Option<Vec<i64>> = if let Some(ref req_tokens) = req_tokens {
        let req_tokens = get_token_ids(connection, req_tokens)?;
        Some(get_web_pages_req(connection, req_tokens)?)
    } else {
        None
    };

    if !query.is_empty() {
        where_sql.push("token.value in rarray(?1)");
    }
    if web_page_ids.is_some() {
        where_sql.push("champion_bm25.webPageId in rarray(?2)");
    }

    if where_sql.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let mut sql = String::from(
        "
        WITH top_results AS (
            select champion_bm25.webPageId, SUM(token.idf * champion_bm25.tf) as score
            FROM champion_bm25
            JOIN token ON champion_bm25.tokenId = token.tokenId
            WHERE 
        ",
    );
    sql.push_str(&where_sql.join(" AND "));

    sql.push_str(
        "
            GROUP BY champion_bm25.webPageId
            ORDER BY score DESC
            LIMIT 10
            OFFSET ?3
        )

        SELECT tr.score, w.title, w.summary, w.link, w.tokenCount, w.publishDate, w.lastUpdate
        FROM top_results tr
        JOIN webPage w ON w.webPageId = tr.webPageId
        ORDER BY tr.score DESC
    ",
    );

    let mut stmt = connection.prepare_cached(&sql)?;

    let values = Rc::new(
        query
            .clone()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<Value>>(),
    );
    let web_page_ids = match web_page_ids {
        Some(web_page_ids) => Some(Rc::new(
            web_page_ids
                .into_iter()
                .map(Value::from)
                .collect::<Vec<Value>>(),
        )),
        None => None,
    };

    let web_pages = stmt.query_map((&values, &web_page_ids, offset), |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(1)?,
            summary: row.get(2)?,
            url: row.get(3)?,
            token_count: row.get(4)?,
            publish_date: DateTime::from_timestamp_millis(row.get(5)?).unwrap(),
            last_update: DateTime::from_timestamp_millis(row.get(6)?).unwrap(),
        })
    })?;

    for web_page in web_pages {
        let web_page = web_page?;
        out.push(web_page);
    }

    // Very hacky way to get it working
    let mut count_sql = String::from(
        "
        SELECT COUNT(DISTINCT webPageId)
        FROM champion_bm25
        JOIN token ON champion_bm25.tokenId = token.tokenId
        WHERE ?3 AND 
        ",
    );

    if !where_sql.is_empty() {
        count_sql.push_str(&where_sql.join(" AND "));
    }

    let count =
        connection.query_one(&count_sql, (&values, &web_page_ids, true), |row| row.get(0))?;
    // let count = 100;
    println!("{} {}", out.len(), count);

    Ok((out, count))
}

pub fn random_web_pages(connection: &Connection) -> Result<Vec<WebPage>, rusqlite::Error> {
    let mut out = Vec::with_capacity(10);

    let mut stmt = connection.prepare_cached(
        "
WITH RECURSIVE random_targets(id_target) AS (
    SELECT abs(random()) % (SELECT MAX(webPageId) FROM webPage) + 1

    UNION ALL

    SELECT abs(random()) % (SELECT MAX(webPageId) FROM webPage) + 1
    FROM random_targets
    LIMIT 15  -- to not have duplicates
)

SELECT DISTINCT w.title, w.summary, w.link, w.tokenCount, w.publishDate, w.lastUpdate FROM random_targets r
JOIN webPage w ON w.webPageId = (
    SELECT MIN(webPageId)
    FROM webPage
    WHERE webPageId >= r.id_target
);
        ",
    )?;

    let web_pages = stmt.query_map([], |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(0)?,
            summary: row.get(1)?,
            url: row.get(2)?,
            token_count: row.get(3)?,
            publish_date: DateTime::from_timestamp_millis(row.get(4)?).unwrap(),
            last_update: DateTime::from_timestamp_millis(row.get(5)?).unwrap(),
        })
    })?;

    for web_page in web_pages {
        let web_page = web_page?;
        out.push(web_page);
    }

    Ok(out)
}

pub fn latest_web_pages(
    connection: &Connection,
    page: i64,
) -> Result<Vec<WebPage>, rusqlite::Error> {
    let mut out = Vec::with_capacity(10);

    let mut stmt = connection.prepare_cached(
        "
        SELECT title, summary, link, tokenCount, publishDate, lastUpdate
        FROM webPage
        ORDER BY publishDate DESC
        LIMIT 10
        OFFSET ?1;
        ",
    )?;

    let web_pages = stmt.query_map((10 * page,), |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(0)?,
            summary: row.get(1)?,
            url: row.get(2)?,
            token_count: row.get(3)?,
            publish_date: DateTime::from_timestamp_millis(row.get(4)?).unwrap(),
            last_update: DateTime::from_timestamp_millis(row.get(5)?).unwrap(),
        })
    })?;

    for web_page in web_pages {
        let web_page = web_page?;
        out.push(web_page);
    }

    Ok(out)
}

#[derive(Clone)]
pub struct IndexStats {
    pub web_pages_count: i64,
    pub web_page_token_count: i64,
    pub tokens_count: i64,
    pub bm25_count: i64,
}

pub fn get_stats(connection: &Connection) -> Result<IndexStats, rusqlite::Error> {
    let (web_pages_count, web_page_token_count, tokens_count, bm25_count) = connection.query_one(
        "
        SELECT webPageCount, webPageTokenCount, tokenCount, tfCount
        FROM indexStat
        ",
        (),
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;

    Ok(IndexStats {
        web_pages_count: web_pages_count,
        web_page_token_count: web_page_token_count,
        tokens_count: tokens_count,
        bm25_count: bm25_count,
    })
}
