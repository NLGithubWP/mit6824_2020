package mr

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const (
	MapPhrase = "map"
	ReducePhrase = "reduce"

	StatusFinish  = 2

	WorkerDefaultIndex = -1

	WorkerNormal = 2
	WorkerDelay = 3
)




func readFile(filename string) string{
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[Worker]: cannot open %v, err: %s", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker]: cannot read %v, err: %s", filename, err)
	}
	file.Close()
	return string(content)
}

func toJsonFile(filename string, kvs []KeyValue){
	/*
		The worker's map task code will need a way to store intermediate key/value pairs in files in
		a way that can be correctly read back during reduce tasks. One possibility is to use Go's
		encoding/json package. To write key/value pairs to a JSON file:
	*/
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("[Worker]: Cannot create %v, err: %s", filename, err)
	}

	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err!=nil{
			panic(err)
		}
	}
	//fmt.Println(enc)
	//_, err = fmt.Fprintf(file, "%v\n", enc)
	//if err != nil {
	//	log.Fatalf("cannot write %v", enc)
	//}
	_ = file.Close()
}


func Json2String(fileName string) (kva []KeyValue) {
	jsonData := readFile(fileName)
	reader := strings.NewReader(jsonData)
	// and to read such a file back
	dec := json.NewDecoder(reader)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return
}