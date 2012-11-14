#Twitter-HDFS
===========================
A crawler that uses twitters streaming api to collect tweets into a hive table named twitter_raw on hdfs and executes a pig script ones a day to analyze the data and save it into separate tables.
All the content of HiveConn have been generate using thrifts client generator in combination with hives source code. Since hive and thrift uses the Apache licence it should be compatible with the rest of the code that's GPLv.3 but if there are any claims please contact the mail list.

##Dependencies
-------------------------------------
#Perl modules:
- Net::Twitter::Stream
- Thrift
- Thrift::XS
#Other
- Two instances of a running hive-server (may be compatible with hive-server2 but not tested).
- Pig
- [DarkFoo](https://github.com/LazyDrone/darkfoo)
- A up and running hadoop cluster

##More info
-------------------------------------
For more info visit the [wiki](https://github.com/LazyDrone/twitter-hdfs/wiki).