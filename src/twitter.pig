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
DEFINE DotDel darkfoo.pig.misc.DotReducer();
DEFINE Markov darkfoo.pig.misc.MarkovPair();
DEFINE LastClean darkfoo.pig.misc.OnlyAlphNumSpace();
DEFINE SpaDel darkfoo.pig.misc.SpaceReducer();
DEFINE SplitDot darkfoo.pig.misc.SplitAtDot();
DEFINE SplitSpace darkfoo.pig.misc.SplitAtSpace();
DEFINE TabSpace darkfoo.pig.misc.TabToSpace();
DEFINE URLCL darkfoo.pig.misc.URLDelete();
DEFINE AtDel darkfoo.pig.twitter.AtDelete();
DEFINE HashDel darkfoo.pig.twitter.HashDelete();

raw_tweet = LOAD '/user/hive/warehouse/twitter_raw/yymmdd=$date' USING PigStorage('\t') AS (id:chararray, name:chararray, usertz:chararray, followers:int, friends:int, time:int, place:chararray, hash:chararray, rtcnt:int, mess:chararray);

--clean tweet
cltweet = FOREACH raw_tweet GENERATE id AS id, SpaDel(LastClean(DotDel(TabSpace(HashDel(AtDel(URLCL(mess))))))) AS mess, hash AS hash;
cltweet = FILTER cltweet BY mess IS NOT null;
tweet = FOREACH cltweet GENERATE id AS id, FLATTEN(SplitDot(LOWER(mess))) AS mess, LOWER(hash) AS hash;
STORE tweet INTO '/user/hive/warehouse/twitter_tweet/yymmdd=$date' USING PigStorage('\t');


--index words in tweet
tweet_word = FOREACH tweet GENERATE id AS id, FLATTEN(SplitSpace(mess)) AS word;
tweet_word = FILTER tweet_word BY word IS NOT null;
STORE tweet_word INTO '/user/hive/warehouse/twitter_word_index/yymmdd=$date' USING PigStorage('\t');

--tweet word count
tweet_word_grouped = GROUP tweet_word BY word;
tweet_word_count = FOREACH tweet_word_grouped GENERATE group , COUNT(tweet_word);
STORE tweet_word_count INTO '/user/hive/warehouse/twitter_word_count/yymmdd=$date' USING PigStorage('\t');

--index pairs in tweet
tweet_words = FOREACH tweet GENERATE id AS id, TOKENIZE(mess) AS word;
tweet_words = FILTER tweet_words BY word IS NOT null;
tweet_pairs = FOREACH tweet_words GENERATE id AS id, Markov(word) AS pair;
tweet_pairs = FOREACH tweet_pairs GENERATE id AS id, FLATTEN(pair) AS (c1, c2);
STORE tweet_pairs INTO '/user/hive/warehouse/twitter_pair_index/yymmdd=$date' USING PigStorage('\t');

--count pairs in tweets
tweet_pairs_grouped = GROUP tweet_pairs BY (c1, c2);
tweet_pair_count = FOREACH tweet_pairs_grouped
		GENERATE group.c1 AS c1,
			 group.c2 AS c2,
			 COUNT(tweet_pairs) AS cnt;
STORE tweet_pair_count INTO '/user/hive/warehouse/twitter_pair_count/yymmdd=$date' USING PigStorage('\t');

--index tweet hashes
tweet_hashes = FOREACH tweet GENERATE id AS id, FLATTEN(TOKENIZE(hash)) AS hash;
tweet_hashes = FILTER tweet_hashes BY hash IS NOT null;
STORE tweet_hashes INTO '/user/hive/warehouse/twitter_hash_index/yymmdd=$date' USING PigStorage('\t');

--tweet hash count
tweet_hash_grouped = GROUP tweet_hashes BY hash;
tweet_hash_count = FOREACH tweet_hash_grouped GENERATE group, COUNT(tweet_hashes);
STORE tweet_hash_count INTO '/user/hive/warehouse/twitter_hash_count/yymmdd=$date' USING PigStorage('\t');
