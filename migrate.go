package main

//
// Migrate data from one redis cluster to another.
//

import (
	"bufio"
	"flag"
	"fmt"
	"gopkg.in/redis.v3" // http://godoc.org/gopkg.in/redis.v3
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// redis server connections
var sourceCluster *redis.ClusterClient
var destinationCluster *redis.ClusterClient
var sourceHost *redis.Client
var destinationHost *redis.Client

// redis server arrays for connecting
var sourceHostsArray []string
var destinationHostsArray []string

// cluster indication flags
var sourceIsCluster = false
var destinationIsCluster = false

// connected states
var sourceClusterConnected = false
var destinationClusterConnected = false

// counter for number of keys migrated
var keysMigrated int64 = 0

func main() {

	//
	// Command line argument handling
	//

	// parse command line arguments
	var sourceHosts = flag.String("sourceHosts", "", "A list of source cluster host:port servers seperated by commas. EX) 127.0.0.1:6379,127.0.0.1:6380")
	var destinationHosts = flag.String("destinationHosts", "", "A list of source cluster host:port servers seperated by spaces. EX) 127.0.0.1:6379,127.0.0.1:6380")
	var getKeys = flag.Bool("getKeys", false, "Fetches and prints keys from the source host.")
	var copyData = flag.Bool("copyData", false, "Copies all keys in a specified list to the destination cluster from the source cluster.")
	var keyFilePath = flag.String("keyFile", "", "The file path which contains the list of keys to migrate.")
	var keyFilter = flag.String("keyFilter", "*", "The pattern of keys to migrate if no key file path was specified.")

	// parse the args we are looking for
	flag.Parse()

	// Ensure a valid operation was passed
	if *getKeys == false && *copyData == false {
		showHelp()
	}

	//
	// Connect to redis servers or clusters
	//

	// connect to the host if servers found
	// break source hosts comma list into an array
	var sourceHostsString = *sourceHosts
	if len(sourceHostsString) > 0 {
		//log.Println("Source hosts detected: " + sourceHostsString)
		// break source hosts string at commas into a slice
		sourceHostsArray = strings.Split(sourceHostsString, ",")
		connectSourceCluster()
	}

	// connect to the host if servers found
	// break destination hosts comma list into an array
	var destinationHostsString = *destinationHosts
	if len(destinationHostsString) > 0 {
		//log.Println("Destination hosts detected: " + destinationHostsString)
		// break source hosts string at commas into a slice
		destinationHostsArray = strings.Split(destinationHostsString, ",")
		connectDestinationCluster()
	}

	//
	// Do the right thing depending on the operations passed from cli
	//

	// Get and display a key list
	if *getKeys == true {

		// ensure we are connected
		if sourceClusterConnected != true {
			log.Fatalln("Please specify a source cluster using -sourceCluster=127.0.0.1:6379.")
		}

		//log.Println("Getting full key list...")
		// iterate through each host in the destination cluster, connect, and
		// run KEYS search
		var allKeys = getSourceKeys(*keyFilter)

		// see how many keys we fetched
		if len(allKeys) > 0 {
			// loop through all keys and print them plainly one per line
			for i := 0; i < len(allKeys); i++ {
				fmt.Println(allKeys[i])
			}
		} else {
			fmt.Println("No keys found in source cluster.")
		}
	}

	// Copy all or some keys to the new server/cluster
	if *copyData == true {

		// ensure we are connected
		if sourceClusterConnected != true {
			log.Fatalln("Please specify a source cluster using -sourceCluster=127.0.0.1:6379.")
			showHelp()
		}
		if destinationClusterConnected != true {
			log.Fatalln("Please specify a destination cluster using -destinationCluster=127.0.0.1:6379")
			showHelp()
		}

		// if the key file path was set, open the file
		if len(*keyFilePath) > 0 {

			// ensure that keyFile and keyFilter are not both specified
			if *keyFilter != "*" {
				log.Fatalln("Can not use -keyFilter= option with -keyFile= option.")
			}

			var keyFile, err = os.Open(*keyFilePath)
			if err != nil {
				log.Fatalln("Unable to open key file specified.")
			}
			// create a new scanner for parsing the io reader returned by the
			// os.Open call earlier
			var keyFileScanner = bufio.NewScanner(keyFile)

			// read the entire key file
			for keyFileScanner.Scan() {

				// fetch the text for this line
				var key = keyFileScanner.Text()

				// migrate the key from source to destination
				migrateKey(key)

			}
		} else {
			// This is what we do if no key file was specified

			var allKeys = getSourceKeys(*keyFilter)

			if len(allKeys) > 0 {
				// loop through all keys and print them plainly one per line
				for i := 0; i < len(allKeys); i++ {
					var key = allKeys[i]
					migrateKey(key)
				}
			} else {
				fmt.Println("No keys found in source cluster.")
			}
		}

	}

	// Finish up with some stats
	fmt.Println("Migrated " + strconv.FormatInt(keysMigrated, 10) + " keys.")
}

// ping testing functions
func clusterPingTest(redisClient *redis.ClusterClient) {
	var pingTest = redisClient.Ping()
	var pingMessage, pingError = pingTest.Result()
	if pingError != nil {
		log.Fatalln("Error when pinging a Redis connection:" + pingMessage)
	}
}
func hostPingTest(redisClient *redis.Client) {
	var pingTest = redisClient.Ping()
	var pingMessage, pingError = pingTest.Result()
	if pingError != nil {
		log.Fatalln("Error when pinging a Redis connection:" + pingMessage)
	}
}

// Connects to the source host/cluster
func connectSourceCluster() {

	// connect to source cluster and ping it
	if len(sourceHostsArray) == 1 {
		sourceHost = redis.NewClient(&redis.Options{
			Addr: sourceHostsArray[0],
		})
		sourceIsCluster = false
		//log.Println("Source is a single host.")
		hostPingTest(sourceHost)
	} else {
		sourceCluster = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: sourceHostsArray,
		})
		sourceIsCluster = true
		//log.Println("Source is a cluster.")
		clusterPingTest(sourceCluster)
	}
	sourceClusterConnected = true
	//log.Println("Source connected")
}

// Connects to the destination host/cluster
func connectDestinationCluster() {

	// connect to destination cluster and ping it
	if len(destinationHostsArray) == 1 {
		destinationHost = redis.NewClient(&redis.Options{
			Addr: destinationHostsArray[0],
		})
		destinationIsCluster = false
		//log.Println("Destination is a single host.")
		hostPingTest(destinationHost)
	} else {
		destinationCluster = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: destinationHostsArray,
		})
		destinationIsCluster = true
		//log.Println("Destination is a cluster.")
		clusterPingTest(destinationCluster)
	}
	destinationClusterConnected = true
	//log.Println("Destination connected.")
}

// Gets all the keys from the source server/cluster
func getSourceKeys(keyFilter string) []string {

	var allKeys *redis.StringSliceCmd
	if sourceIsCluster == true {
		allKeys = sourceCluster.Keys(keyFilter)
	} else {
		allKeys = sourceHost.Keys(keyFilter)
	}

	return allKeys.Val()
}

// Migrates a key from the source cluster to the deestination one
func migrateKey(key string) {

	//log.Println("migrating key:" + key)

	keysMigrated = keysMigrated + 1

	// init a value to hold the key data
	var data string

	// init a value to hold the key ttl
	var ttl time.Duration

	// get the key from the source
	if sourceIsCluster == true {
		data = sourceCluster.Dump(key).Val()
		ttl = sourceCluster.PTTL(key).Val()

	} else {
		data = sourceHost.Dump(key).Val()
		ttl = sourceHost.PTTL(key).Val()
	}

	// put the key in the destination cluster and set the ttl
	if destinationIsCluster == true {
		destinationCluster.Restore(key, ttl, data)
	} else {
		destinationHost.Restore(key, ttl, data)
	}

	return
}

// Displays the help content
func showHelp() {
	fmt.Println(`
- Redis Key Migrator - 
https://github.com/integrii/go-redis-migrator

Migrates all or some of the keys from a Redis host or cluster to a specified host or cluster.  Supports migrating TTLs.
go-redis-migrator can also be used to list the keys in a cluster.  Run with the -getKey=true and -sourceHosts= flags to do so.

You must run this program with an operation before it will do anything.

Flags:
  -getKeys=false: Fetches and prints keys from the source host.
  -copyData=false: Copies all keys in a list specified by -keyFile= to the destination cluster from the source cluster.
  -keyFile="": The file path which contains the list of keys to migrate.  One per line.
  -keyFilter="*": The filter for which keys to migrate.  Does not work when -keyFile is also specified.
  -destinationHosts="": A list of source cluster host:port servers seperated by spaces. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
  -sourceHosts="": A list of source cluster host:port servers seperated by commas. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
	`)

	os.Exit(0)
}
