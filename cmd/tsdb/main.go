// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	rprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		cli                  = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		benchCmd             = cli.Command("bench", "run benchmarks")
		benchWriteCmd        = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath    = benchWriteCmd.Flag("out", "set the output path").Default("benchout/").String()
		benchWriteNumMetrics = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("560").Int()
		benchSamplesFile     = benchWriteCmd.Arg("file", "input file with samples data, default is (../../testdata/20k.series.json)").Default("../../testdata/560series.json").String()
		listCmd              = cli.Command("ls", "list db blocks")
		listCmdHumanReadable = listCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		listPath             = listCmd.Arg("db path", "database path (default is benchout/storage)").Default("benchout/storage").String()
	)

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case benchWriteCmd.FullCommand():
		wb := &writeBenchmark{
			outPath:     *benchWriteOutPath,
			numMetrics:  *benchWriteNumMetrics,
			samplesFile: *benchSamplesFile,
		}
		wb.run()
	case listCmd.FullCommand():
		db, err := tsdb.Open(*listPath, nil, nil, nil)
		if err != nil {
			exitWithError(err)
		}
		printBlocks(db.Blocks(), listCmdHumanReadable)
		//printBlocks(db.Blocks())
		//tsdb.PrintBlocks4(db)
		
		tsdb.PrintBlocks3(db)
		
	}
	flag.CommandLine.Set("log.level", "debug")
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool
	numMetrics  int

	storage *tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func (b *writeBenchmark) run() {
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	collectorMetricsRegistry := prometheus.NewRegistry()
	//Register metrics related to scrape, and the current go process.
	collectorMetricsRegistry.Register(prometheus.NewProcessCollector(os.Getpid(), ""))
	collectorMetricsRegistry.Register(prometheus.NewGoCollector())
	// Register the metrics from the various collectors.
	go metricsServer(collectorMetricsRegistry, 9080)

	timeInMins := 120 * time.Minute
	fmt.Println(tsdb.ExponentialBlockRanges(int64(timeInMins)/1e6, 10, 5))
	fmt.Println(int64(timeDelta*shardScrapeCount))
	fmt.Println(int64(timeInMins) / 1e6)
	fmt.Println(int64(timeInMins / 1e6) / 2 * 3)
	//exitWithError(errors.New("test"))

	st, err := tsdb.Open(dir, l, collectorMetricsRegistry, &tsdb.Options{
		WALFlushInterval:  200 * time.Millisecond,
		RetentionDuration: 15 * 24 * 60 * 60 * 1000,                                      // 15 days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(timeInMins/1e6), 10, 5), //tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if err != nil {
		exitWithError(err)
	}
	b.storage = st

	var metrics []labels.Labels

	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		metrics, err = readPrometheusLabels(f, b.numMetrics)
		if err != nil {
			exitWithError(err)
		}
	})
	fmt.Println(" > total metrics:", len(metrics))

	var total uint64

	dur := measureTime("ingestScrapes", func() {
		b.startProfiling()
		total, err = b.ingestScrapes(metrics)
		if err != nil {
			exitWithError(err)
		}
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	measureTime("stopStorage", func() {
		if err := b.storage.Close(); err != nil {
			exitWithError(err)
		}
		b.stopProfiling()
	})
}

const (
	// scrapeCount = 10000
	// shards      = scrapeCount / batchSize

	// If we change batch size, then need to adjust BlockRanges
	// size is inverse to block range
	// blockRange[0] need to be atleast 2 * timeDelta*shardScrapeCount
	batchSize = 560

	shards        = 100
	//basets        = 3000
	//shardInterval = 2000

	timeDelta        = 300
	shardScrapeCount = 500

	metricsPath = "/metrics"
	healthzPath = "/healthz"
)

func (b *writeBenchmark) ingestScrapes(lbls []labels.Labels) (uint64, error) {
	var mu sync.Mutex
	var total uint64

	numBatches := b.numMetrics/batchSize
	fmt.Println(" > batch size:", batchSize, "numBatches", numBatches,  ", shards:", shards)

	for shard := 0; shard < shards; shard++ {

		var batchGroup sync.WaitGroup
		lbls := lbls
		batchIndex := 0
		for len(lbls) > 0 {
			l := batchSize
			if len(lbls) < batchSize {
				l = len(lbls)
			}
			batchData := lbls[:l]
			lbls = lbls[l:]

			batchGroup.Add(1)
			go func(batch int) {
				n, err := b.ingestScrapesShard(shard, batch, numBatches, batchData, shardScrapeCount)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				mu.Lock()
				total += n
				mu.Unlock()
				batchGroup.Done()
			}(batchIndex)
			batchIndex++
			batchGroup.Wait()
		}
	}
	fmt.Println("ingestion completed")
	time.Sleep(10*time.Minute)

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(shard int, batch int, numBatches int, metrics []labels.Labels, scrapeCount int) (uint64, error) {
	ts := int64((numBatches*shard + batch) * timeDelta*scrapeCount)
	baset := ts
	fmt.Println(" > scrape range", "shard:", shard, "batch:", batch, "base:", baset, ", max:", baset+int64(timeDelta*scrapeCount))

	type sample struct {
		labels labels.Labels
		value  int64
		ref    *uint64
	}

	scrape := make([]*sample, 0, len(metrics))
	lset := labels.FromStrings("__name__","sfn_statefulset_status_replicas","dc","PHX","namespace","legostore","statefulset","ceph-osd-0-7")
	sort.Sort(lset)

	for _, m := range metrics {
		scrape = append(scrape, &sample{
			labels: m,
			value:  1,
		})
	}
	total := uint64(0)

	refCount := 0
	fastRefCount := 0
	count := 0


	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta

		for _, s := range scrape {
			//s.value += 1000
			if labels.Compare(s.labels, lset) == 0 {
				count++
			}

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, float64(s.value))
				if err != nil {
					panic(err)
				}
				s.ref = &ref
				refCount++
			} else if err := app.AddFast(*s.ref, ts, float64(s.value)); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					fmt.Println(" > scrape range error ", "shard:", shard, "batch:", batch, "base:", ts)
					fmt.Println(b.storage.Head().MinTime())
					fmt.Println(b.storage.Head().MaxTime())
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, float64(s.value))
				if err != nil {
					panic(err)
				}
				s.ref = &ref
				if ref != 0 {
					fastRefCount++
				}
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	fmt.Println(" > total refs :", refCount, ", fast:", fastRefCount, "count:", count)

	return total, nil
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	rprof.StartCPUProfile(b.cpuprof)

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *writeBenchmark) stopProfiling() {
	if b.cpuprof != nil {
		rprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		rprof.Lookup("heap").WriteTo(b.memprof, 0)
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		rprof.Lookup("block").WriteTo(b.blockprof, 0)
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		rprof.Lookup("mutex").WriteTo(b.mtxprof, 0)
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func mapToLabels(m map[string]interface{}, l *labels.Labels) {
	for k, v := range m {
		*l = append(*l, labels.Label{Name: k, Value: v.(string)})
	}
}

func readPrometheusLabels(r io.Reader, n int) ([]labels.Labels, error) {
	scanner := bufio.NewScanner(r)

	var mets []labels.Labels
	hashes := map[uint64]struct{}{}
	i := 0

	for scanner.Scan() && i < n {
		m := make(labels.Labels, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			m = append(m, labels.Label{Name: split[0], Value: split[1]})
		}
		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(m)
		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}
		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	return mets, nil
}


func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printBlocks(blocks []*tsdb.Block, humanReadable *bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable *bool) string {
	if *humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}

func metricsServer(registry prometheus.Gatherer, port int) {
	// Address to listen on for web interface and telemetry
	listenAddress := net.JoinHostPort("", strconv.Itoa(port))

	mux := http.NewServeMux()

	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	// Add metricsPath
	mux.Handle(metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	// Add healthzPath
	mux.HandleFunc(healthzPath, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	// Add index
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>StorageFoundation State Metrics Server</title></head>
             <body>
             <h1>StorageFoundation State Metrics</h1>
			 <ul>
             <li><a href='` + metricsPath + `'>metrics</a></li>
             <li><a href='` + healthzPath + `'>healthz</a></li>
			 </ul>
             </body>
             </html>`))
	})

	fmt.Println(
		"SFnStateMetricsMain",
		"msg:", "Starting metrics server",
		"address:", listenAddress,
	)

	fmt.Println(
		"SFnStateMetricsMain",
		"msg:", "Failed to start metrics server",
		"err:", http.ListenAndServe(listenAddress, mux),
	)

}
