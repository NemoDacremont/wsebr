langs = arabic bulgarian czech chinese dutch english german greek finnish french hindi hungarian indonesian italian japanese latvian norwegian polish portuguese romanian russian slovak spanish swedish turkish
stopwords = $(addprefix .stopwords/,$(langs))

all: smallweb.txt stopwords.txt

smallweb.txt:
	wget -O $@ https://github.com/kagisearch/smallweb/raw/refs/heads/main/smallweb.txt

stopwords.txt: $(stopwords)
	cat $^ > $@

$(stopwords): .stopwords/%:
	@mkdir -p $(dir $@)
	curl -so $@ https://countwordsfree.com/stopwords/$*/txt

clean:
	rm -rf .stopwords stopwords.txt smallweb.txt

.PHONY: all clean
