package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

	"github.com/elastic/apm-agent-go"
	"github.com/elastic/apm-agent-go/module/apmhttp"
)

var (
	esURL        string
	traceID      string
	pollDuration time.Duration
)

func init() {
	flag.StringVar(&esURL, "es", "http://localhost:9200", "Whitespace-delimited list of Elasticsearch server URLs")
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
		fmt.Println("polling for new events for", pollDuration)
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

		root := gotree.New("")
		orphaned := gotree.New(color.RedString("<orphaned>"))
		treeNodes := make(map[nodeId]gotree.Tree)
		treeNodes[nodeId{traceID: traceID}] = root
		for id, node := range nodes {
			nodeText := fmt.Sprintf("%s (%s)", node.name, node.service)
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
		}
		if len(orphaned.Items()) > 0 {
			root.AddTree(orphaned)
		}
		for _, tree := range root.Items() {
			fmt.Println(tree.Print())
		}
		if !recentRequest {
			break
		}
	}
}

func doRequest(url *url.URL) {
	client := apmhttp.WrapClient(http.DefaultClient)
	tx := elasticapm.DefaultTracer.StartTransaction("GET "+url.String(), "request")
	ctx := elasticapm.ContextWithTransaction(context.Background(), tx)
	req, _ := http.NewRequest("GET", url.String(), nil)
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()
	traceContext := tx.TraceContext()
	tx.End()

	ctxFlush, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		elasticapm.DefaultTracer.Flush(ctxFlush.Done())
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

type nodeDetails struct {
	parentID    string
	name        string
	service     string
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
				Query(elastic.NewTermQuery("transaction.trace_id", traceID))),

		elastic.NewSearchRequest().
			Index("apm-*-span-*").
			SearchSource(elastic.NewSearchSource().
				Size(10000).
				Query(elastic.NewTermQuery("span.trace_id", traceID))),
	).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "querying elasticsearch")
	}

	type CommonFields struct {
		TraceID  string `json:"trace_id"`
		ParentID string `json:"parent_id"`
		Name     string `json:"name"`
	}
	type source struct {
		Context struct {
			Service struct {
				Name string
			}
		}
		Span *struct {
			CommonFields
			HexID string `json:"hex_id"`
		}
		Transaction *struct {
			CommonFields
			ID string
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
				nodeId.traceID = source.Span.TraceID
				nodeId.spanID = source.Span.HexID
				nodeDetails.name = source.Span.Name
				nodeDetails.parentID = source.Span.ParentID
			case source.Transaction != nil:
				nodeId.traceID = source.Transaction.TraceID
				nodeId.spanID = source.Transaction.ID
				nodeDetails.name = source.Transaction.Name
				nodeDetails.parentID = source.Transaction.ParentID
				nodeDetails.transaction = true
			default:
				panic("no transaction or span in doc")
			}
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
