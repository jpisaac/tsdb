package tsdb

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"	
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

type symbolSet map[string]struct{}

func (ss *symbolSet) String() string {
	s := *ss
	sss := s.asSlice()
	sort.Strings(sss)
	return strings.Join(sss, ",")
}

func (ss *symbolSet) asSlice() []string {
	cols := []string{}
	for col := range *ss {
		cols = append(cols, col)
	}
	return cols
}

type pair struct {
	t int64
	v float64
}

func PrintBlocks4(db *DB) (error) {
	//vals, _ := q.LabelValues("blppopdupk")
	//fmt.Println(strings.Join(vals, "\n"))
	osds, _ := labels.NewRegexpMatcher("statefulset","ceph-osd-0-*")
	sel := []labels.Matcher {
		labels.NewEqualMatcher("dc","PHX"),
		labels.NewEqualMatcher("namespace","legostore"),
		osds,
	}


	var (
		//set       	ChunkSeriesSet
		//allSymbols = make(map[string]struct{}, 1<<16)
		//chunkMap   = map[uint64]uint64{}
		closers    = []io.Closer{}
	)
	defer func() { closeAll(closers...) }()
	
	for _, b := range db.blocks {
		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		tombr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tomb reader for block %s", b)
		}
		closers = append(closers, tombr)
		cs, err := LookupChunkSeries(indexr, tombr, sel...)
		if err != nil {
			return errors.Wrapf(err, "open lookup reader for block %s", b)
		}

		for cs.Next() {
			lbls, chks, _ := cs.At()
			fmt.Println("labels: ", lbls.String(), "chunks: ", len(chks))
			/*
			for _, chk := range chks {
				fmt.Println("chunks: ", chk.Ref)
			}

			for _, intv := range intvs {
				fmt.Println("min: ", intv.Mint, "max: ", intv.Maxt)
			}
			*/

		}
		
	}
	
	return nil
}

func PrintBlocks3(db *DB) (error) {
	//vals, _ := q.LabelValues("blppopdupk")
	//fmt.Println(strings.Join(vals, "\n"))
	q, _ := db.Querier(7500001, 150000000)
	
	sel := []labels.Matcher {
		labels.NewEqualMatcher("dc","PHX"),
		labels.NewEqualMatcher("namespace","legostore"),
		labels.NewEqualMatcher("statefulset","ceph-osd-0-7"),
	}
	//m, _ := labels.NewRegexpMatcher("__name__", "sfn_statefulset_status_replicas")
	ss, _ := q.Select(sel...)
	for ss.Next() {
		sl := ss.At().Labels()
		for _, l := range sl {
			fmt.Println(fmt.Sprintf("%v, %v", l.Name, l.Value))	
		}

		sit := ss.At().Iterator()
		tmin := math.MaxFloat64
		tmax := float64(0)
		count := int64(0)
		index := int64(0)
		//last := int64(0)
		for sit.Next() {
			
			t, v := sit.At()
			/*
			if (t-last) != int64(300) && last != 0 {
				fmt.Println(fmt.Sprintf("***%v, %v, %v", last, t, v))	
			}
			if t >= int64(7200000) && t <= int64(7234800) {
				fmt.Println(fmt.Sprintf("*%v, %v, %v", last, t, v))
			}
			*/
			index++
			//last = t
			tmin = math.Min(float64(tmin), float64(t))
			tmax = math.Max(float64(tmax), float64(t))
			count += int64(v)
		}
		fmt.Println(fmt.Sprintf("%f, %f, %v, %d", tmin, tmax, count, index))	
		fmt.Println("*****************")	
	}
	return nil
}


func PrintBlocks2(blocks []*Block) (error) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	var (
		//set       	ChunkSeriesSet
		//allSymbols = make(map[string]struct{}, 1<<16)
		chunkMap   = map[uint64]uint64{}
		closers    = []io.Closer{}
	)
	defer func() { closeAll(closers...) }()
	
	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()
		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %s", b)
		}
		closers = append(closers, chunkr)
		

		allLabels, err := indexr.LabelIndices()
		if err != nil {
			return errors.Wrapf(err, "read all postings")
		}
	
		for i, ls := range allLabels {
			for j, name := range ls {
				// if (name != "syweycqmwiuhe")  {
				// 	continue
				// }

				values, _ := indexr.LabelValues(name)

				fmt.Printf("labels i = %d, j = %d, name = %s, num-values = %d\n", i, j, name, values.Len())


				for v := 0; v < values.Len(); v++ {
					allValues, _ := values.At(v)
					fmt.Printf("\tlabel values = %s \n", strings.Join(allValues, ","))
					gotp, _ := indexr.Postings(name, allValues[0])
					var lset labels.Labels
					var chks []chunks.Meta
								
					for gotp.Next() {
						ref := gotp.At()
						indexr.Series(ref, &lset, &chks)
						fmt.Printf("\t\tpostings num-labels = %d, num-chunks = %d \n", lset.Len(), len(chks))

						for i := range chks {
							chk := chks[i]
						
							var doCount bool
							if (chunkMap[chk.Ref] == 0) {
								doCount = true
							}
							chk.Chunk, err = chunkr.Chunk(chk.Ref)
							if err == nil {
								it := chk.Chunk.Iterator()
								var res []pair
								for it.Next() {
									if (doCount) {
										chunkMap[chk.Ref]++
									}
									ts, v := it.At()
									res = append(res, pair{t: ts, v: v})
								}
								fmt.Printf("\t\t\tchunks ref = %d, min-time = %d, max-time%d, num-samples = %d\n", chk.Ref, chk.MinTime, chk.MaxTime ,len(res))
															
							}
						
													
						}
									
					}

				}

			}
		}
		var numSamples uint64
		fmt.Printf("numChunks %d :", len(chunkMap))
		for _, v := range chunkMap {
			numSamples = numSamples + v
		}
		fmt.Printf("numSamples %d :", numSamples)


		/*
		ss, err := indexr.Symbols()
		if err != nil {
			return errors.Wrapf(err, "read symbols")
		}


		// if (i == 0) {
		// 	fmt.Printf("symbols : %s", (*symbolSet)(&ss).String())
		// }
		fmt.Println("index-version :", i)
		fmt.Println("num-symbols : ", len((*symbolSet)(&ss).asSlice()))
		*/

		fmt.Fprintf(tw,
			"\n%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			meta.MinTime,
			meta.MaxTime,
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}

	return nil
}
