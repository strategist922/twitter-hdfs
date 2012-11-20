#!/usr/bin/perl -CSDL

#Copyright (c) 2012 Johan Gustavsson <johan@life-hack.org>
#
#Hive-api is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#Hive-api is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with Hive-api.  If not, see <http://www.gnu.org/licenses/>.

use encoding "utf-8";
use strict;
use warnings;
use Net::Twitter::Stream;
require tweet;
sub streamer{
	my $username = 'twitter user name';
	my $password = 'twitter password';
	our $filesize = 10000;
	our $lang = "en";
	our $filenr = 0;
	our $linenr = 0;
	our $active_date;
	our $last_date;
	our $date_set = 0;
	
	#daemonize
	close(STDIN);
	close(STDOUT);
	close(STDERR);
	exit if (fork());
	exit if (fork());
	
	open OUTPUT, ">/tmp/twitter" or die "Cant open file: $!";
	Net::Twitter::Stream->new ( user => $username, pass => $password,
								callback => \&got_tweet,
								connection_closed_cb => \&connection_closed,
								track => '.'
								);

	sub connection_closed {
		sleep 10;
		warn "Connection to Twitter closed";
		close(OUTPUT);
		streamer();
	}
	sub got_tweet {
		my ( $tweet, $json ) = @_;   
		my $tweet_lang = "";
		if(defined($tweet->{user}{lang})){
			$tweet_lang = $tweet->{user}{lang};
		}
		if($tweet_lang eq $lang){
			my ($tweet, $tweet_date) = tweet::extract_tweet($tweet);
			if(!$date_set){
				$date_set = 1;
				$active_date = $tweet_date;
			}
			print OUTPUT $tweet;
			$linenr++;
			if($linenr == $filesize){
				close(OUTPUT);
				move_file($tweet_date);
				$linenr = 0;
				open OUTPUT, ">/tmp/twitter" or die "Cant open file $!";
			}

		}
	}
	sub move_file {
		my ($tweet_date) = @_;
		if($active_date == $tweet_date){        
			unless(-e "/tmp/${active_date}/"){
				system("mkdir", "/tmp/${active_date}/");
			}
		}else{
			$last_date = $active_date;
			$active_date = $tweet_date;
			unless(-e "/tmp/${active_date}/"){
				system("mkdir", "/tmp/${active_date}/");
			}
			system("rm", "-r", "/tmp/${last_date}");
			tweet::process_tweets($last_date);
			$filenr = 0;
		}
		my $file = "/tmp/${active_date}/${filenr}";
		while(-e $file){
			$filenr++;
			$file = "/tmp/${active_date}/${filenr}";
		}
		system("mv","/tmp/twitter",$file); 
		tweet::hive_push($active_date, $filenr);
		$linenr = 0;
	}
}
streamer();