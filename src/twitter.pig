/*
Copyright (c) 2012 Johan Gustavsson <johan@life-hack.org>

Twitter-HDFS is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Twitter-HDFS is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Twitter-HDFS.  If not, see <http://www.gnu.org/licenses/>.
*/
REGISTER /usr/lib/pig/darkfoo.jar
DEFINE DotReducer darkfoo.pig.Cleanup.DotReducer();
DEFINE MarkovLikeLadder darkfoo.pig.Bags.MarkovLikeLadder();
DEFINE BagToString darkfoo.pig.Bags.BagToString();
DEFINE Search darkfoo.pig.WordNet.Search();
DEFINE OnlyAlphaNumSpace darkfoo.pig.Cleanup.OnlyAlphNumSpace();
DEFINE SpaceReducer darkfoo.pig.Cleanup.SpaceReducer();
DEFINE TokenizeDot darkfoo.pig.Bags.TokenizeDot();
DEFINE TokenizeSpace darkfoo.pig.Bags.TokenizeSpace();
DEFINE TabToSpace darkfoo.pig.Cleanup.TabToSpace();
DEFINE URLDelete darkfoo.pig.Cleanup.URLDelete();
DEFINE DelHtml darkfoo.pig.Cleanup.DelHtml();
DEFINE RTDelete darkfoo.pig.Twitter.RTDelete();
DEFINE AtDelete darkfoo.pig.Twitter.AtDelete();
DEFINE HashDelete darkfoo.pig.Twitter.HashDelete();

raw_tweet = LOAD '/user/hive/warehouse/twitter_raw/yymmdd=$date' USING PigStorage('\t') AS (id:chararray, name:chararray, usertz:chararray, followers:int, friends:int, time:int, place:chararray, hash:chararray, rtcnt:int, mess:chararray);


--clean tweet
cltweet = FOREACH raw_tweet GENERATE id AS id, SpaceReducer(OnlyAlphaNumSpace(DotReducer(TabToSpace(HashDelete(AtDelete(URLDelete(DelHtml(RTDelete(mess))))))))) AS mess, hash AS hash, rtcnt AS rtcnt;
cltweet = FILTER cltweet BY mess IS NOT null;
tweet = FOREACH cltweet GENERATE id AS id, FLATTEN(TokenizeDot(LOWER(mess))) AS mess, LOWER(hash) AS hash, rtcnt AS rtcnt;
STORE tweet INTO '/user/hive/warehouse/twitter_tweet/yymmdd=$date' USING PigStorage('\t');


--singel word work
words = FOREACH tweet GENERATE id, FLATTEN(TokenizeSpace(mess)) AS word, rtcnt AS rtcnt;
word_groups = GROUP words BY word;
index = FOREACH word_groups {
	id = DISTINCT $1.$0;
	cnt = COUNT(id);
	rt = $1.$2;
	rtcnt = SUM(rt);
	GENERATE $0 AS word, cnt AS cnt, id AS id, rtcnt AS rtcnt;
};
word_index = FOREACH index GENERATE Search(word) AS word, cnt AS cnt, BagToString(id) AS id, rtcnt AS rtcnt;
word_index_final = FOREACH word_index GENERATE word.$0 AS word, word.$1 AS adj, word.$2 AS adv, word.$3 AS verb, word.$4 AS noun, cnt AS cnt, id AS id, rtcnt AS rtcnt;
STORE word_index_final INTO '/user/hive/warehouse/twitter_word_index/yymmdd=$date' USING PigStorage('\t');


--index pattern in tweet
tweet_words = FOREACH tweet GENERATE id AS id, TokenizeSpace(mess) AS word, rtcnt AS rtcnt;
tweet_words = FILTER tweet_words BY word IS NOT null;
tweet_group = FOREACH tweet_words GENERATE id AS id, MarkovLikeLadder(word) AS pattern, rtcnt AS rtcnt;
tweet_group = FOREACH tweet_group GENERATE id AS id, FLATTEN(pattern) AS (c1, c2, c3), rtcnt AS rtcnt;
tweet_group_grouped = GROUP tweet_group BY (c1, c2, c3);
tweet_group_index = FOREACH tweet_group_grouped {
	id = DISTINCT $1.$0;
	cnt = COUNT(id);
	rt = $1.$4;
	rtcnt = SUM(rt);
	GENERATE $0.$0 AS first, $0.$1 AS second, $0.$2 AS last, cnt AS cnt, BagToString(id) AS id, rtcnt AS rtcnt; 
};
STORE tweet_group_index INTO '/user/hive/warehouse/twitter_pattern_index/yymmdd=$date' USING PigStorage('\t');


--index tweet hashes
tweet_hashes = FOREACH raw_tweet GENERATE id AS id, FLATTEN(TOKENIZE(hash)) AS hash, rtcnt AS rtcnt;
tweet_hashes = FILTER tweet_hashes BY hash IS NOT null;
tweet_hash_grouped = GROUP tweet_hashes BY hash;
tweet_hash_index = FOREACH tweet_hash_grouped {
	id = DISTINCT $1.$0;
	cnt = COUNT(id);
	rt = $1.$2;
	rtcnt = SUM(rt);
	GENERATE $0 AS word, cnt AS cnt, BagToString(id) AS id, rtcnt AS rtcnt;
};

STORE tweet_hash_index INTO '/user/hive/warehouse/twitter_hash_index/yymmdd=$date' USING PigStorage('\t');
