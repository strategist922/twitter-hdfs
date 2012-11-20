#!/usr/bin/perl -CSDL

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

use encoding "utf-8";
use strict;
use warnings;
use Net::Twitter::Stream;
use Config::Simple;
require tweet;
sub streamer{
	my $cfg = new Config::Simple('/etc/lazydrone.conf');
	if(!$cfg){
		print 'Error: Conf could not be read.\n';
		return -1;
	}
	my $username = $cfg->param("twitter-hdfs.twitter-user");
	my $password = $cfg->param("twitter-hdfs.twitter-pass");
	our $filesize = $cfg->param("twitter-hdfs.file-size");
	our $lang = $cfg->param("twitter-hdfs.work-lang");
	our $tempdir = $cfg->param("twitter-hdfs.work-dir");
	our $tempfile = $cfg->param("twitter-hdfs.work-file");
	our $hive1_addr = $cfg->param("twitter-hdfs.hive1-addr");
	our $hive1_port = $cfg->param("twitter-hdfs.hive1-port");
	our $hive2_addr = $cfg->param("twitter-hdfs.hive2-addr");
	our $hive2_port = $cfg->param("twitter-hdfs.hive2-port");
	if(!$username || !$password || !$filesize || !$lang || !$tempdir || !$tempfile || !$hive1_addr || !$hive1_port || !$hive2_addr || !$hive2_port){
		print 'Error: one or more values are missing from config file.\n';	
		return -1;
	}
	our $filenr = 0;
	our $linenr = 0;
	our $active_date;
	our $last_date;
	our $date_set = 0;
	
	open OUTPUT, ">$tempfile" or die "Cant open file: $!";
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
				open OUTPUT, ">$tempfile" or die "Cant open file $!";
			}

		}
	}
	sub move_file {
		my ($tweet_date) = @_;
		if($active_date == $tweet_date){        
			unless(-e "${tempdir}${active_date}/"){
				system("mkdir", "${tempdir}${active_date}/");
			}
		}else{
			$last_date = $active_date;
			$active_date = $tweet_date;
			unless(-e "${tempdir}${active_date}/"){
				system("mkdir", "${tempdir}${active_date}/");
			}
			system("rm", "-r", "${tempdir}${last_date}");
			tweet::process_tweets($last_date, $hive2_addr, $hive2_port);
			$filenr = 0;
		}
		my $file = "${tempdir}${active_date}/${filenr}";
		while(-e $file){
			$filenr++;
			$file = "${tempdir}${active_date}/${filenr}";
		}
		system("mv",$tempfile,$file); 
		tweet::hive_push($active_date, $filenr, $tempdir, $hive1_addr, $hive1_port);
		$linenr = 0;
	}
}
streamer();
