package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/tidwall/gjson"
)

type Student struct {
	Name         string  `json:"name"`
	Age          int64   `json:"age"`
	AverageScore float64 `json:"average_score"`
}

func createElasticsearchClient() (*elasticsearch.Client, error) {
	var r map[string]interface{}
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Username: "",
		Password: "",
	}
	client, err := elasticsearch.NewClient(cfg)

	fmt.Println("ES Client created")
	// 1. Get cluster info
	clusterInfo, err := client.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer clusterInfo.Body.Close()
	// Deserialize the response into a map.
	if err := json.NewDecoder(clusterInfo.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("Client: %s", elasticsearch.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("~", 37))
	return client, err
}

func main() {
	log.SetFlags(0)
	// var r map[string]interface{}

	var (
	// wg sync.WaitGroup
	)
	esClient, _ := createElasticsearchClient()
	indexName := "kibana_sample_data_ecommerce"
	fmt.Println("creating index")
	res, err := esClient.Index(
		indexName,
		strings.NewReader(`{"customer_gender":"MALE"}`),
		esClient.Index.WithDocumentID("1"))
	fmt.Println(res, err)

	fmt.Println("searching index")
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"customer_gender": "MALE",
			},
			// "_source": false,
			// "fields": []interface{}{"customer_gender", "email"},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}
	searchRes, err := esClient.Search(
		// es.Search.WithContext(context.Background()),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(&buf),
		esClient.Search.WithTrackTotalHits(true),
		esClient.Search.WithPretty(),
	)
	if err != nil {
		fmt.Printf("Error searching: %s\n", err)
		os.Exit(2)
	}
	defer searchRes.Body.Close()
	if searchRes.IsError() {
		printErrorResponse(searchRes)
	}

	// parse with gjson
	var b bytes.Buffer
	b.ReadFrom(searchRes.Body)

	values := gjson.GetManyBytes(b.Bytes(), "hits.total.value", "took", "hits.hits.#", "hits.hits.0")
	// values := gjson.GetManyBytes(b.Bytes(), "hits.total.value", "_source")
	fmt.Printf(
		"[%s] %d hits; took: %dms\n",
		res.Status(),
		values[0].Int(),
		values[1].Int(),
	)
	// Print the ID and document source for each hit.
	objValues := (values[3].Value()).(map[string]interface{})
	category := objValues["_source"].(map[string]interface{})["category"]
	log.Printf(" * total=%d, values=%d", values[2].Int(), category)
}

func printErrorResponse(res *esapi.Response) {
	fmt.Printf("[%s] ", res.Status())
	var e map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	} else {
		// Print the response status and error information.
		log.Fatalf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],
		)
	}
}
