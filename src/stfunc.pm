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

package stfunc;
use Digest::MD5 qw(md5_hex);
sub get_id {
	my($input) = @_;
	return md5_hex($input);
}
sub backup {
	my($date, $backup_host) = @_;
	system("cp", "-r", "/tmp/$date", "./yymmdd=$date");
	system("tar", "-zcvf", "$date.tar.gz", "yymmdd=$date");
	system("scp", "$date.tar.gz", $backup_host);
	system("rm", "$date.tar.gz");
	system("rm", "-r", "yymmdd=$date");
}
sub fix_date {
	my($date) = @_;
	my %months = ("Jan" => "01",
					"Feb" => "02",
					"Mar" => "03",
					"Apr" => "04",
					"May" => "05",
					"Jun" => "06",
					"Jul" => "07",
					"Aug" => "08",
					"Sep" => "09",
					"Oct" => "10",
					"Nov" => "11",
					"Dec" => "12");
	my $monthRef = \%months;
	my @work = split(' ',$date);
	my @time = split(':',$work[3]);
	my @callback = [];
	$callback[0] = "$work[5]";
	$callback[1] = "$$monthRef{$work[1]}";
	$callback[2] = "$work[2]";
	$callback[3] = "$time[0]";
	$callback[4] = "$time[1]";
	$callback[5] = "$time[2]";
	return @callback;
}

1;