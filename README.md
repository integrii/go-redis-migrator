# go-redis-migrator
A cluster aware Redis key migrator.  Moves keys from one cluster or host to another cluster or host.

## Details
This program connects to a source host or cluster and migrates keys to a destination host or cluster.  It is possible to fetch a list of keys with this program, create a file with the keys to migrate, and feed that list into this program for selective migration.  This is useful if you have some keys hanging around that you do not want to migrate.

#### Getting a key list from the source
`./go-redis-migrator -getKeys=true -sourceHosts=127.0.0.1:6379`

With a cluster:

`./go-redis-migrator -getKeys=true -sourceHosts=127.0.0.1:6379,127.0.0.1:6380`

#### Migrating keys 
`./go-redis-migrator -copydata=true -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379`

With clusters:

`./go-redis-migrator -copyData=true -sourceHosts=127.0.0.1:6379,127.0.0.1:6380 -destinationHosts=192.168.0.1:6379,192.168.0.1:6380`

or from a cluster and to a single host:

`./go-redis-migrator -copyData=true -sourceHosts=127.0.0.1:6379,127.0.0.1:6380 -destinationHosts=192.168.0.1:6379`

From a single host to a cluster:

`./go-redis-migrator -copyData=true -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379,192.168.0.1:6380`

#### Migrating only keys from a list
`./go-redis-migrator -copydata=true -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379 -keyFile=./onlyMoveTheseKeys.txt`


#### CLI help
Simply run the binary to get the following help:
```
- Redis Key Migrator - 
https://github.com/integrii/go-redis-migrator

Migrates all or some of the keys from a Redis host or cluster to a specified host or cluster.  Supports migrating TTLs.
go-redis-migrator can also be used to list the keys in a cluster.  Run with the -getKey=true and -sourceHosts= flags to do so.

You must run this program with an operation before it will do anything.

Flags:
  -getKeys=false: Fetches and prints keys from the source host.
  -copyData=false: Copies all keys in a list specified by -keyFile= to the destination cluster from the source cluster.
  -keyFile="": The file path which contains the list of keys to migrate.  One per line.
  -destinationHosts="": A list of source cluster host:port servers seperated by spaces. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
  -sourceHosts="": A list of source cluster host:port servers seperated by commas. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
```
