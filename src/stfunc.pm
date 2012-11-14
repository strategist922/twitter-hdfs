#!/usr/bin/perl
package stfunc;
use Digest::MD5 qw(md5_hex);
sub get_id {
	my($input) = @_;
	return md5_hex($input);
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