package mapreduce
// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function// (reduceF) for each key, and writes the output to disk.

import (
	"os"
	"encoding/json"
	"sort"
	"strings"
)

type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return strings.Compare(a[i].Key, a[j].Key) == -1 }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from
	// map task number m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	var kvSet []KeyValue

  // loop over all map files for this reduce task and write KeyValues to memory
	for m := 0; m < nMap; m++ {
		mergeFile := reduceName(jobName, m, reduceTaskNumber)
		file, err := os.OpenFile(mergeFile, os.O_RDWR|os.O_CREATE, 0755)
		defer file.Close()
		checkErr(err)

		dec := json.NewDecoder(file)

		for dec.More() {
			var kv KeyValue

			err := dec.Decode(&kv)
			if err != nil {
				break;
			}

			kvSet = append(kvSet, kv)
		}
	}

  // sort the KeyValues by key
	sort.Sort(ByKey(kvSet))

  var lastKey string
  var resultArr []string

  mergeFilePath := mergeName(jobName, reduceTaskNumber)
  mergeFile, err := os.OpenFile(mergeFilePath, os.O_RDWR|os.O_CREATE, 0755)
  checkErr(err)
  defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)

  // loop over all of the KeyValues, combining keys and then running the 
  // reduce task on the keys / value arrays
  for idx, kvPair := range kvSet {
    // if you're starting a new key, write to reduce task file 
    // and clear the results array
    if idx != 0 && strings.Compare(lastKey, kvPair.Key) != 0 {
      enc.Encode(KeyValue{lastKey, reduceF(lastKey, resultArr)})
      resultArr = resultArr[:0]
    }

    lastKey = kvPair.Key

    resultArr = append(resultArr, kvPair.Value)
  }

  enc.Encode(KeyValue{lastKey, reduceF(lastKey, resultArr)})
}
