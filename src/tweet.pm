#!/usr/bin/perl
package tweet;
require stfunc;
use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use lib <./HiveConn>;
use ThriftHive;
sub hive_push {
	my ($date, $filenr) = @_;
	#change the follwing to according to your cluster. 
	#This hive server instance needs to be running on the same host as the script.
	my serveraddr = "h-apps1.local";
	my serverport = 10000;

	my $socket = Thrift::Socket->new(serveraddr, serverport);
	$socket->setSendTimeout(600 * 1000); # 10min.
	$socket->setRecvTimeout(600 * 1000);
	my $transport = Thrift::BufferedTransport->new($socket);
	my $protocol = Thrift::BinaryProtocol->new($transport);

	my $client = ThriftHiveClient->new($protocol);

	eval {
		$transport->open();
		my $query = "LOAD DATA LOCAL INPATH '/tmp/${date}/${filenr}' INTO TABLE twitter_raw PARTITION (yymmdd = '${date}')";
		$client->execute($query);
		$transport->close();
	};
}
sub process_tweets {
	my ($date) = @_;
	#change the follwing to according to your cluster. 
	my serveraddr = "h-apps1.local";
	my serverport = 10001;
	
	unless (my $pid = fork) {
        die "Couldn't fork" unless defined $pid;
		my $socket = Thrift::Socket->new(serveraddr, serverport);
		$socket->setSendTimeout(600 * 1000);
		$socket->setRecvTimeout(600 * 1000);
		my $transport = Thrift::BufferedTransport->new($socket);
		my $protocol = Thrift::BinaryProtocol->new($transport);
		my $client = ThriftHiveClient->new($protocol);
		system("pig", "-param", "date=${date}", "twitter.pig");
		eval{
			$transport->open();
			$client->execute("ALTER TABLE twitter_hash_count ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_hash_index ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_pair_count ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_pair_index ADD PARTITION (yymmdd = ${date})");
			$client->execute("ALTER TABLE twitter_word_count ADD PARTITION (yymmdd = ${date})");
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