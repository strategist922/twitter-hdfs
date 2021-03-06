#!/usr/bin/perl

#Copyright (c) 2012 Johan Gustavsson <johan@life-hack.org>
#
#Twitter-HDFS is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#Twitter-HDFS is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with Twitter-HDFS.  If not, see <http://www.gnu.org/licenses/>.

package tweet;
require stfunc;
use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use lib <./HiveConn>;
use ThriftHive;
sub hive_push {
	my ($date, $filenr, $tempdir, $hive_addr, $hive_port) = @_;
	my $socket = Thrift::Socket->new($hive_addr, $hive_port);
	$socket->setSendTimeout(600 * 1000); # 10min.
	$socket->setRecvTimeout(600 * 1000);
	my $transport = Thrift::BufferedTransport->new($socket);
	my $protocol = Thrift::BinaryProtocol->new($transport);

	my $client = ThriftHiveClient->new($protocol);

	eval {
		$transport->open();
		my $query = "LOAD DATA LOCAL INPATH '${tempdir}${date}/${filenr}' INTO TABLE twitter_raw PARTITION (yymmdd = '${date}')";
		$client->execute($query);
		$transport->close();
	};
}
sub process_tweets {
	my ($date, $hive_addr, $hive_port, $last_date, $backup_host) = @_;
	unless (my $pid = fork) {
        die "Couldn't fork" unless defined $pid;
		if($backup_host ne 'false'){
			stfunc::backup($date, $backup_host);
		}
		system("rm", "-r", "${tempdir}${last_date}");
		my $socket = Thrift::Socket->new($hive_addr, $hive_port);
		$socket->setSendTimeout(600 * 1000);
		$socket->setRecvTimeout(600 * 1000);
		my $transport = Thrift::BufferedTransport->new($socket);
		my $protocol = Thrift::BinaryProtocol->new($transport);
		my $client = ThriftHiveClient->new($protocol);
		system("pig", "-param", "date=${date}", "twitter.pig");
		eval{
			$transport->open();
			$client->execute("ALTER TABLE twitter_hash_index ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_pattern_index ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_word_index ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_tweet ADD PARTITION (yymmdd = ${date})");
			$transport->close();
		};
		exit;
	}
}
sub extract_tweet {
	my($tweet) = @_;
	if(defined($tweet->{text})){
		my $mess = $tweet->{text};
		my $by = $tweet->{user}{screen_name};
		my $user_tz = "";
		if(defined($tweet->{user}{time_zone})){
			$user_tz = $tweet->{user}{time_zone};
			$user_tz =~ s/\t/ /g;
		}
		my $user_follow = "";
		if(defined($tweet->{user}{followers_count})){
			$user_follow = $tweet->{user}{followers_count};
		}
		my $user_friend = "";
		if(defined($tweet->{user}{friends_count})){
			$user_friend = $tweet->{user}{friends_count};
		}
		my @datetime = stfunc::fix_date($tweet->{created_at});
		my $place = "";
		if(defined($tweet->{place})){
			$place = "$tweet->{place}{country_code}";
		}
		my $i = 0;
		my $hash = "";
		while(defined($tweet->{entities}{hashtags}[$i]{text})){ 
			if ($i >= 1){
				$hash .= " ";
			}
			$hash .= $tweet->{entities}{hashtags}[$i]{text};
			$i++;
		}
		my $rt_cnt = $tweet->{retweet_count};
		my $work = "${by}\t${user_tz}\t${user_follow}\t${user_friend}\t${datetime[3]}${datetime[4]}${datetime[5]}\t${place}\t${hash}\t${rt_cnt}\t${mess}";
		$work =~ s/\n/ /g;
		$hash = stfunc::get_id($work); 
		my $payload = "${hash}\t${work}\n";
		my $date = "${datetime[0]}${datetime[1]}${datetime[2]}";
        return ($payload, $date);        
	}
}

1;