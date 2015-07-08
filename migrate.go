package main

//
// Migrate data from one redis cluster to another.
//

import (
	"flag"
	"fmt"
	"gopkg.in/redis.v3" // http://godoc.org/gopkg.in/redis.v3
	"log"
	"strings"
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

	// parse the args we are looking for
	flag.Parse()

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
		// run KEYS *
		var allKeys = getSourceKeys()

		// loop through all keys and print them plainly one per line
		for i := 0; i < len(allKeys); i++ {
			fmt.Println(allKeys[i])
		}
	}

	// Copy all or some keys to the new server/cluster
	if *copyData == true {

		// ensure we are connected
		if sourceClusterConnected != true {
			log.Fatalln("Please specify a source cluster using -sourceCluster=127.0.0.1:6379.")
		}
		if destinationClusterConnected != true {
			log.Fatalln("Please specify a destination cluster using -destinationCluster=127.0.0.1:6379")
		}

		// check if a keyfile was specified
		var keyFile = *keyFilePath

		// if the key file path was set, open the file and read all the keys
		// into an array

		// loop through the array of key strings
		// read key from source cluster
		// write key to destination cluster

		// load the list of keys from the keyfile
		log.Println("Destination hosts: " + destinationHostsString + keyFile)
	}
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
func getSourceKeys() []string {

	var allKeys *redis.StringSliceCmd
	if destinationIsCluster == true {
		allKeys = sourceCluster.Keys("*")
	} else {
		allKeys = sourceHost.Keys("*")
	}

	return allKeys.Val()
}
