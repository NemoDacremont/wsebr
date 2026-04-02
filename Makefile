release_prefix ?= target/release
wsebr = $(release_prefix)/wsebr
wsebr_crawler = $(release_prefix)/wsebr_crawler

langs = arabic bulgarian czech chinese dutch english german greek finnish french hindi hungarian indonesian italian japanese latvian norwegian polish portuguese romanian russian slovak spanish swedish turkish
stopwords = $(addprefix .stopwords/,$(langs))

PHONY += all
all: build

PHONY += docker
docker:
	docker build -t ghcr.io/nemodacremont/wsebrd:latest .

buid: database.db

$(wsebr) $(wsebr_crawler):
	cargo build --release

crawled: smallweb.txt
	ulimit -n 4096  # Enable having a huge amount of fd
	$(wsebr_crawler) $^

database.db: $(wsebr) $(wsebr_crawler) crawled
	$(wsebr) build webpages crawled
	$(wsebr) build idf
	$(wsebr) build okapi

smallweb.txt:
	wget -O $@ https://github.com/kagisearch/smallweb/raw/refs/heads/main/smallweb.txt

stopwords.txt: $(stopwords)
	cat $^ > $@

$(stopwords): .stopwords/%:
	@mkdir -p $(dir $@)
	curl -so $@ https://countwordsfree.com/stopwords/$*/txt

PHONY += clean
clean:
	rm -rf .stopwords stopwords.txt smallweb.txt

.PHONY: $(PHONY)
