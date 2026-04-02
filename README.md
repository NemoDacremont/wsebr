# wsebr - the Worst Search Engine But in Rust

This is project yet another not useful project in rust, the goal is to write a
Search Engine from *scratch* (to some extent) to explore the small web.

## Getting started

It is quite CPU, network and I/O expensive to build the database (~8GB network
download, and the processing will be quite slow if you don't use an NVMe)

> [NOTE]
> The crawler is a little bit aggressive (~100 concurrent requests), which brings
> its own inconveniences, you should take a look at the crawler optimisations
> below to make sure your requests are not failing

Building :

```bash
make database.db
# make docker  # if you want to build the docker image
```

Then, it is possible to start the application :

```bash
./target/release/wsebrd
# docker run --rm -p 127.0.0.1:3000:3000 -v ./database.db:/app/database.db ghcr.io/nemodacremont/wsebrd:latest
```

The webui will then be available on `http://localhost:3000`

## How it works

Database computation :

1. Crawl blog feeds
2. Extract all pages from the feeds and store title, link & summary
3. Iterate through pages, tokenize them and count the tokens (idf computation)
4. NOTE: tokenization is done with [rust-stemmers](https://github.com/CurrySoftware/rust-stemmers), which is a wrapper of [snowballstem](https://snowballstem.org/), which will be better than what I can do
5. Iterate through pages, count the tokens occurence for each page (bm25 computation)

Search :

1. Tokenize the query
2. For each page having these tokens, compute the score (=sum(bm25(token, webpage)))
3. Sort and take the top 10

## Optimisations

### Crawler

You should increase the amount of available fds at once :

```bash
ulimit -n 20000
```

You should also update your `/etc/resolv.conf` file (or local resolver, but I
don't use local resolver) as following :

```text
nameserver 8.8.8.8
nameserver 8.8.4.4
nameserver 1.1.1.1
nameserver 1.0.0.1
options rotate timeout:2 attempts:5
```

It will make the os load balance the requests accross the domain servers. You
should add more servers near your location from <https://public-dns.info> to
make sure you don't get bocked by cloudflare or google at ip level.

## See also

I was motivated to create wsebr by these projects :

* Wander : A tiny, decentralised tool you can host with just two files to
explore the small web <https://codeberg.org/susam/wander>
* kagi small web : <https://github.com/kagisearch/smallweb>

The design is *highly* inspired from <https://github.com/pruger/tiny-brutalism-css>
