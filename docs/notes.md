 main.go : ingestScrapesShard:
    calls db.head.appender.Add

head.go : Add(...) and AddFast(...)
    stores samples in memory (memSeries)
    onCommit -> wal.LogSeries and wal.LogSamples
    series (labels and values) and samples (ts, value, *series) encoded and written to wal

wal.go : LogSeries and LogSamples
    LogSeries and 
    LogSamples encodes
    	// Store base timestamp and base reference number of first sample.
	    // All samples encode their timestamp and ref as delta to those.
wal.go : cut
    When the segment file reaches the segmentSize the file cut and new segment file is open
    asynchronously (enqueued) flushes the last segment file.
    sync dir (enqueued)

wal.go : run
    flushes the enqueued operations

db.go compact(....)
        calls db.compactor.Write
        which creates new blocks and writes chunks to it.
        calls db.reload(deleteable ...string)
            calls db.head.Truncate(maxt)

db.go
    query: read all the blocks and head