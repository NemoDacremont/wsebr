use rayon::prelude::*;
use reqwest::blocking;
use std::{
    env::args,
    fs::{self, File, create_dir_all},
    io::{BufRead, BufReader},
    path::PathBuf,
};

fn crawl(url: String, client: &blocking::Client) {
    let mut parts = url.split('/').skip(2);
    let domain = parts.next().expect("couldn't retrieve domain from URL");
    let filename = parts.last().unwrap_or("index.html");

    if domain == "simturax.com" {
        println!("{domain} - blacklisted")
    }

    let directory = "./crawled/".to_string() + domain + "/";
    let out_path = directory.to_owned() + filename;

    if PathBuf::from(out_path.as_str()).exists() {
        return;
    }

    let res = client.get(&url).send();
    if !res.is_ok() {
        let err = res.err().unwrap();
        println!("{url} - Couldn't fetch : {err}");
        return;
    }

    let mut response = res.unwrap();

    if response.status().is_success() {
        create_dir_all(directory).expect("Directories to be created");
        let mut file = File::create(&out_path).expect("File to be opened");
        if let Err(e) = response.copy_to(&mut file) {
            println!("{url} - Failed to write to file: {}", e);
        }
        println!("{url} - Fetched");
    } else {
        // println!("{url} - Couldn't fetch, status {}", response.status());
    }
    return;
}

fn main() -> Result<(), reqwest::Error> {
    let url_path = args().skip(1).next().expect("./crawler <urls_file>");
    let url_file = fs::File::open(url_path).expect("./crawler <urls_file>");

    let urls: Vec<String> = BufReader::new(url_file)
        .lines()
        .map(|line| line.unwrap())
        .collect();

    let global_client = blocking::ClientBuilder::new()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        .tls_danger_accept_invalid_hostnames(true)
        .tls_danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::limited(99))
        .build()
        .unwrap();

    urls.into_par_iter()
        .for_each(|url| crawl(url, &global_client));
    Ok(())
}
