langs = arabic bulgarian czech chinese dutch english german greek finnish french hindi hungarian indonesian italian japanese latvian norwegian polish portuguese romanian russian slovak spanish swedish turkish
stopwords = $(addprefix .stopwords/,$(langs))

stopwords.txt: $(stopwords)
	cat $^ > $@

$(stopwords): .stopwords/%:
	@mkdir -p $(dir $@)
	curl -so $@ https://countwordsfree.com/stopwords/$*/txt

.PHONY: clean
clean:
	rm -rf .stopwords stopwords.txt

