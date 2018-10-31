package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/disiqueira/gotree"
	"github.com/fatih/color"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"go.elastic.co/apm"
	"go.elastic.co/apm/module/apmhttp"
)

var (
	esURL         string
	kibanaURLFlag string
	kibanaURL     *url.URL
	traceID       string
	pollDuration  time.Duration
)

func init() {
	flag.StringVar(&esURL, "es", "http://localhost:9200", "Whitespace-delimited list of Elasticsearch server URLs")
	flag.StringVar(&kibanaURLFlag, "kibana", "http://localhost:5601", "Base URL for Kibana")
	flag.StringVar(&traceID, "trace", "", "Trace ID to query (must not also specify URL to fetch)")
	flag.DurationVar(&pollDuration, "d", 30*time.Second, "Amount of time to wait for events")

	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [URL]\n\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "OPTIONS:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "")
}

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 && traceID == "" {
		flag.Usage()
		os.Exit(2)
	}

	var err error
	kibanaURL, err = url.Parse(kibanaURLFlag)
	if err != nil {
		log.Fatalf("failed to parse Kibana URL %q: %v", kibanaURLFlag, err)
	}

	var deadline time.Time
	var recentRequest bool
	if len(args) == 1 {
		if traceID != "" {
			flag.Usage()
		}
		url, err := url.Parse(args[0])
		if err != nil {
			log.Fatalf("failed to parse URL %q: %v", url, err)
		}
		doRequest(url)
		recentRequest = true
		fmt.Printf("polling for new events for %s...\n\n", pollDuration)
		deadline = time.Now().Add(pollDuration)
	}

	var lastNodeCount int
queryNodes:
	for {
		var nodes map[nodeId]nodeDetails
		for {
			if recentRequest {
				if !time.Now().Before(deadline) {
					return
				}
				time.Sleep(5 * time.Second)
			}
			var err error
			nodes, err = fetchTrace(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			if len(nodes) != lastNodeCount {
				lastNodeCount = len(nodes)
				break
			}
		}

		var rootTransactions []nodeId
		root := gotree.New("")
		orphaned := gotree.New(color.RedString("<orphaned>"))
		treeNodes := make(map[nodeId]gotree.Tree)
		treeNodes[nodeId{traceID: traceID}] = root
		for id, node := range nodes {
			nodeText := fmt.Sprintf("%s (%s)", node.name, node.service)
			if node.result != "" {
				nodeText += fmt.Sprintf(" => %s", node.result)
			}
			if node.transaction {
				nodeText = color.CyanString(nodeText)
			}
			treeNodes[id] = gotree.New(nodeText)
		}
		for id, node := range nodes {
			parentNode := treeNodes[nodeId{traceID: id.traceID, spanID: node.parentID}]
			if parentNode == nil {
				if recentRequest {
					// Missing a node, so go back and query again.
					continue queryNodes
				}
				parentNode = orphaned
			}
			parentNode.AddTree(treeNodes[id])
			if node.transaction && parentNode == root {
				rootTransactions = append(rootTransactions, id)
			}
		}
		for _, id := range rootTransactions {
			fmt.Println(treeNodes[id].Print())
			if transactionURL, err := transactionURL(id, nodes[id]); err == nil {
				fmt.Printf("✨ Open in Kibana: %s ✨\n\n", color.YellowString(transactionURL))
			}
		}
		if len(orphaned.Items()) > 0 {
			fmt.Println(orphaned.Print())
		}
		if !recentRequest {
			break
		}
	}
}

func doRequest(url *url.URL) {
	client := apmhttp.WrapClient(http.DefaultClient)
	tx := apm.DefaultTracer.StartTransaction("GET "+url.String(), "request")
	ctx := apm.ContextWithTransaction(context.Background(), tx)
	req, _ := http.NewRequest("GET", url.String(), nil)
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	traceContext := tx.TraceContext()
	tx.End()

	ctxFlush, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		apm.DefaultTracer.Flush(ctxFlush.Done())
	}()
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			fmt.Println("Waiting for transaction to be flushed...")
		case <-flushed:
			traceID = traceContext.Trace.String()
			fmt.Println("sent request with trace_id:", traceID)
			return
		}
	}
}

func transactionURL(id nodeId, details nodeDetails) (string, error) {
	kuery := fmt.Sprintf("trace.id:%s and transaction.id:%s", id.traceID, id.spanID)
	u := fmt.Sprintf("/app/apm#/%s/transactions/%s/%s?_g=()&kuery=%s",
		details.service,
		details.type_,
		url.PathEscape(details.name),
		url.PathEscape(kuery),
	)
	// TODO(axw) check how we should escape ~ in the name.
	// See: https://github.com/elastic/kibana/issues/24892
	u = strings.Replace(u, "%", "~", -1)

	content := strings.NewReader(fmt.Sprintf(`{"url":%q}`, u))
	kibanaURL := kibanaURL.String()
	req, err := http.NewRequest("POST", kibanaURL+"/api/shorten_url", content)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(os.Stderr, resp.Body)
		return "", errors.New("error shortening URL")
	}

	var result struct {
		URLID string `json:"urlId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return kibanaURL + "/goto/" + result.URLID, nil
}

type nodeDetails struct {
	parentID    string
	name        string
	type_       string
	service     string
	result      string
	transaction bool
}

type nodeId struct {
	traceID string
	spanID  string
}

func fetchTrace(ctx context.Context) (map[nodeId]nodeDetails, error) {
	esClient := newElasticsearchClient()
	msearchResult, err := esClient.MultiSearch().Add(
		elastic.NewSearchRequest().
			Index("apm-*-transaction-*").
			SearchSource(elastic.NewSearchSource().
				Size(10000).
				Query(elastic.NewTermQuery("trace.id", traceID))),

		elastic.NewSearchRequest().
			Index("apm-*-span-*").
			SearchSource(elastic.NewSearchSource().
				Size(10000).
				Query(elastic.NewTermQuery("trace.id", traceID))),
	).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "querying elasticsearch")
	}

	type source struct {
		Context struct {
			Service struct {
				Name string
			}
		}
		Trace  struct{ ID string } `json:"trace"`
		Parent struct{ ID string } `json:"parent"`
		Span   *struct {
			HexID string `json:"hex_id"`
			Name  string `json:"name"`
			Type  string `json:"type"`
		}
		Transaction *struct {
			ID     string
			Name   string `json:"name"`
			Type   string `json:"type"`
			Result string `json:"result"`
		}
	}

	nodes := make(map[nodeId]nodeDetails)
	for _, response := range msearchResult.Responses {
		if response.Error != nil {
			return nil, errors.Errorf("%s", response.Error.Reason)
		}
		hits := response.Hits
		if hits.TotalHits > int64(len(hits.Hits)) {
			// TODO(axw) could use scroll instead
			return nil, errors.Errorf("too many hits")
		}
		for _, hit := range hits.Hits {
			var source source
			if err := json.Unmarshal(*hit.Source, &source); err != nil {
				return nil, errors.Wrap(err, "failed to unmarshal _source")
			}

			var nodeId nodeId
			var nodeDetails nodeDetails
			switch {
			case source.Span != nil:
				nodeId.spanID = source.Span.HexID
				nodeDetails.name = source.Span.Name
				nodeDetails.type_ = source.Span.Type
			case source.Transaction != nil:
				nodeId.spanID = source.Transaction.ID
				nodeDetails.name = source.Transaction.Name
				nodeDetails.type_ = source.Transaction.Type
				nodeDetails.result = source.Transaction.Result
				nodeDetails.transaction = true
			default:
				panic("no transaction or span in doc")
			}
			nodeId.traceID = source.Trace.ID
			nodeDetails.parentID = source.Parent.ID
			nodeDetails.service = source.Context.Service.Name
			nodes[nodeId] = nodeDetails
		}
	}
	return nodes, nil
}

func newElasticsearchClient() *elastic.Client {
	urls := strings.Fields(esURL)
	client, err := elastic.NewClient(elastic.SetURL(urls...))
	if err != nil {
		log.Fatal("failed to create client", err)
	}
	return client
}
