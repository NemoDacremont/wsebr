use chrono::{DateTime, Utc};
use feed_rs::model::Entry;
use htmlentity::entity::{self, ICodedDataTrait};
use nanohtml2text::html2text;
use rusqlite::types::Value;
use rusqlite::vtab::array::load_module;
use rusqlite::{Connection, params};
use std::rc::Rc;

pub fn sqlite_init(db_path: &str) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(db_path)?;

    // Enable the use of `rarray` in queries
    load_module(&connection)?;

    create_web_page_table(&connection)?;
    create_token_table(&connection)?;
    create_tf_table(&connection)?;

    connection.execute_batch(
        "
        pragma temp_store   = memory;
        pragma mmap_size    = 536870912;
        pragma page_size    = 4096;
        pragma journal_mode = WAL;
        pragma synchronous  = NORMAL;
        pragma journal_size_limit = 134217728;
        pragma wal_autocheckpoint = 20000;
        pragma foreign_keys = ON;
    ",
    )?;

    Ok(connection)
}

pub struct WebPage {
    pub web_page_id: i64,
    pub title: String,
    pub summary: String,
    pub url: String,
    pub last_update: i64,
    pub publish_date: DateTime<Utc>,
}

impl WebPage {
    pub fn tokenize(&self) -> impl Iterator<Item = String> {
        fn _tokenize(string: &String) -> impl Iterator<Item = String> {
            string
                .as_str()
                .split(|c: char| {
                    !c.is_ascii() || c.is_ascii_whitespace() || c.is_ascii_punctuation()
                })
                .map(|word| {
                    word.chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .map(|c| c.to_ascii_lowercase())
                        .collect::<String>()
                })
                .filter(|token| !token.is_empty())
        }

        let domain = _tokenize(&self.url);
        let title = _tokenize(&self.title);
        let summary = _tokenize(&self.summary);

        title.chain(domain).chain(summary)
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
                .unwrap_or_default()
                .split_whitespace()
                .take(150) // Prevent abuse of sites having huge description
                .collect::<Vec<_>>()
                .join(" "),
            None => "".to_string(),
        };

        let url = match entry.links.first() {
            Some(link) => link.clone().href,
            None => "".to_string(),
        };

        WebPage {
            web_page_id: 0,
            title,
            summary,
            url,
            last_update: Utc::now().timestamp_millis(),
            publish_date: entry.published.unwrap_or_default(),
        }
    }
}

pub fn create_web_page_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_web_page = "
        CREATE TABLE IF NOT EXISTS webPage
        (
            webPageId INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            link TEXT NOT NULL,
            lastUpdate INTEGER NOT NULL,
            publish_date INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS webPageLink ON webPage (link);
        CREATE INDEX IF NOT EXISTS webPagePublishDate ON webPage (publish_date DESC);
    ";
    connection.execute_batch(create_web_page)
}

pub fn drop_web_pages(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE webPage")
}

pub fn insert_web_pages(
    connection: &Connection,
    web_pages: &Vec<WebPage>,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare(
        "
        INSERT INTO webPage
        (title, summary, link, lastUpdate, publish_date) VALUES
        (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(link)
        DO UPDATE SET title=excluded.title, summary=excluded.summary, lastUpdate=excluded.lastUpdate, publish_date=excluded.publish_date
        ",
    )?;
    connection.execute("BEGIN", [])?;
    for web_page in web_pages {
        stmt.execute((
            web_page.title.as_str(),
            web_page.summary.as_str(),
            web_page.url.as_str(),
            Utc::now().timestamp_millis(),
            web_page.publish_date.timestamp_millis(),
        ))?;
    }
    connection.execute("COMMIT", [])
}

pub fn retrieve_web_page<F: FnMut(WebPage)>(
    connection: &Connection,
    mut cb: F,
) -> Result<(), rusqlite::Error> {
    let batch_size: i64 = 1000;
    let mut last_id: i64 = 1;

    loop {
        let batch: Vec<WebPage> = {
            let mut stmt = connection.prepare(
                "
            SELECT webPageId, title, summary, link, publish_date
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
                    publish_date: DateTime::from_timestamp_millis(row.get(4)?).unwrap(),
                    last_update: 0,
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
}

impl Clone for Token {
    fn clone(&self) -> Token {
        Token {
            token_id: self.token_id,
            value: self.value.clone(),
            idf: self.idf,
        }
    }
}

pub fn create_token_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_token = "
        CREATE TABLE IF NOT EXISTS token
        (
            tokenId INTEGER PRIMARY KEY AUTOINCREMENT,
            value BLOB,
            idf REAL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS token_value ON token (value);
    ";
    connection.execute_batch(create_token)
}

pub fn drop_tokens(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE token")
}

pub fn retrieve_token(connection: &Connection, value: &String) -> Result<Token, rusqlite::Error> {
    Ok(connection.query_row(
        "
        SELECT tokenId, idf
        FROM token
        WHERE value = ?1
        LIMIT 1
        ",
        (value,),
        |row| {
            Ok(Token {
                token_id: row.get(0)?,
                value: value.to_string(),
                idf: row.get(1)?,
            })
        },
    )?)
}

pub fn retrieve_tokens<F: FnMut(Token)>(
    connection: &Connection,
    mut cb: F,
) -> Result<(), rusqlite::Error> {
    let mut stmt = connection.prepare(
        "
        SELECT tokenId, value, idf
        FROM token
        ",
    )?;

    let tokens = stmt.query_map([], |row| {
        Ok(Token {
            token_id: row.get(0)?,
            value: row.get::<usize, String>(1)?.to_string(),
            idf: row.get::<usize, f64>(2)?,
        })
    })?;

    for token in tokens {
        let token = token?;
        cb(token);
    }

    Ok(())
}

pub fn insert_tokens(
    connection: &Connection,
    tokens: &Vec<Token>,
) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare(
        "
        INSERT INTO token
        (value, idf) VALUES
        (?1, ?2)
        ON CONFLICT(value)
        DO UPDATE SET idf=excluded.idf
        ",
    )?;

    connection.execute("BEGIN", [])?;
    for token in tokens {
        stmt.execute((token.value.as_str(), token.idf))?;
    }
    connection.execute("COMMIT", [])
}

pub struct Tf {
    pub token_id: i64,
    pub web_page_id: i64,
    pub tf: f64,
}

pub fn create_tf_table(connection: &Connection) -> Result<(), rusqlite::Error> {
    let create_tf = "
        CREATE TABLE IF NOT EXISTS tf
        (
            tokenId INTEGER,
            webPageId INTEGER,
            tf INTEGER,
            PRIMARY KEY (tokenId, webPageId),
            FOREIGN KEY(tokenId) REFERENCES token(tokenId),
            FOREIGN KEY(webPageId) REFERENCES webPage(webPageId)
        );
    ";
    connection.execute_batch(create_tf)
}

pub fn drop_tf(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch("DROP TABLE tf")
}

pub fn retrieve_tf(
    connection: &Connection,
    web_page_id: i64,
    token_id: i64,
) -> Result<Tf, rusqlite::Error> {
    Ok(connection.query_row(
        "
        SELECT tf
        FROM token
        WHERE tokenId = ?1, webPageId = ?2
        LIMIT 1
        ",
        (token_id, web_page_id),
        |row| {
            Ok(Tf {
                web_page_id: web_page_id,
                token_id: token_id,
                tf: row.get(0)?,
            })
        },
    )?)
}

pub fn insert_tfs(connection: &Connection, tfs: &Vec<Tf>) -> Result<usize, rusqlite::Error> {
    let mut stmt = connection.prepare(
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

pub fn search_query(
    connection: &Connection,
    query: Vec<String>,
    page: i64,
) -> Result<(Vec<WebPage>, i64), rusqlite::Error> {
    let mut out = Vec::with_capacity(10);
    let offset = 10 * (page - 1);

    let mut stmt = connection.prepare(
        "
        WITH top_results AS (
            select tf.webPageId, SUM(tf.tf) as score
            FROM token t
            JOIN tf on t.tokenId = tf.tokenId
            WHERE t.value IN (SELECT value FROM token WHERE value IN rarray(?1) ORDER BY idf DESC LIMIT 10)
            GROUP BY tf.webPageId
            ORDER BY score DESC
            LIMIT 10
            OFFSET ?2
        )

        SELECT tr.score, w.title, w.summary, w.link, w.publish_date
        FROM top_results tr
        JOIN webPage w ON w.webPageId = tr.webPageId
        ORDER BY tr.score DESC
        ",
    )?;

    let values = Rc::new(query.into_iter().map(Value::from).collect::<Vec<Value>>());

    let web_pages = stmt.query_map((&values, offset), |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(1)?,
            summary: row.get(2)?,
            url: row.get(3)?,
            publish_date: DateTime::from_timestamp_millis(row.get(4)?).unwrap(),
            last_update: 0,
        })
    })?;

    for web_page in web_pages {
        let web_page = web_page?;
        out.push(web_page);
    }

    let count = connection.query_one("
        SELECT count(*)
        FROM token
        NATURAL JOIN tf
        WHERE value in (select value from token where value in rarray(?1) order by idf desc limit 10);
        ", (values,), |row| row.get(0))?;
    // let count = 100;

    Ok((out, count))
}

pub fn random_web_pages(connection: &Connection) -> Result<Vec<WebPage>, rusqlite::Error> {
    let mut out = Vec::with_capacity(10);

    let mut stmt = connection.prepare(
        "
        WITH RECURSIVE
            rand_ids(webPageId) AS (
              SELECT abs(random()) % (SELECT MAX(webPageId) FROM webPage) + 1
              UNION ALL
              SELECT abs(random()) % (SELECT MAX(webPageId) FROM webPage) + 1
              FROM rand_ids
              LIMIT 15 -- We grab 15 to have a buffer in case some IDs were deleted
            )

        SELECT title, summary, link, publish_date
        FROM webPage
        WHERE webPageId IN rand_ids
        LIMIT 10;
        ",
    )?;

    let web_pages = stmt.query_map([], |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(0)?,
            summary: row.get(1)?,
            url: row.get(2)?,
            publish_date: DateTime::from_timestamp_millis(row.get(3)?).unwrap(),
            last_update: 0,
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

    let mut stmt = connection.prepare(
        "
        SELECT title, summary, link, publish_date
        FROM webPage
        ORDER BY publish_date DESC
        LIMIT 10
        OFFSET ?1;
        ",
    )?;

    let web_pages = stmt.query_map([10 * page], |row| {
        Ok(WebPage {
            web_page_id: -1,
            title: row.get(0)?,
            summary: row.get(1)?,
            url: row.get(2)?,
            publish_date: DateTime::from_timestamp_millis(row.get(3)?).unwrap(),
            last_update: 0,
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
    pub tokens_count: i64,
    pub bm25_count: i64,
}

pub fn get_stats(connection: &Connection) -> Result<IndexStats, rusqlite::Error> {
    let web_pages_count = connection.query_one(
        "
        SELECT count(*)
        FROM webPage
        ",
        (),
        |row| row.get(0),
    )?;

    let tokens_count = connection.query_one(
        "
        SELECT count(*)
        FROM token
        ",
        (),
        |row| row.get(0),
    )?;

    let bm25_count = connection.query_one(
        "
        SELECT count(*)
        FROM tf
        ",
        (),
        |row| row.get(0),
    )?;

    Ok(IndexStats {
        web_pages_count: web_pages_count,
        tokens_count: tokens_count,
        bm25_count: bm25_count,
    })
}
