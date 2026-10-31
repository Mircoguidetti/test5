// Load generator for the FENVIX engine vs mongod CPU-efficiency benchmark.
//
// Same binary, same driver (official Go driver v2), same documents and same write concern
// for both servers. Two modes:
//
//	load: fills N databases (bench0..benchN-1, collection "usertable") with D documents each,
//	      _id = 0..D-1, one ~1 KB string field.
//	run:  runs one workload (read | insert | rw) with C concurrent clients, a warm-up phase
//	      and a measured phase; prints one JSON line with throughput, latency and the CPU
//	      used by the server process (--pid) and by the generator itself during the
//	      measured phase only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var (
	uri       = flag.String("uri", "mongodb://127.0.0.1:27017/?directConnection=true", "connection string")
	mode      = flag.String("mode", "run", "load | run")
	workload  = flag.String("workload", "read", "read | insert | rw (50% read, 50% update)")
	dbs       = flag.Int("dbs", 1, "number of databases")
	docs      = flag.Int("docs", 200000, "documents per database (loaded; key range of reads/updates)")
	clients   = flag.Int("clients", 16, "concurrent clients (goroutines, one pooled connection each)")
	warmup    = flag.Int("warmup", 60, "warm-up seconds (not measured)")
	duration  = flag.Int("duration", 180, "measured seconds")
	pid       = flag.Int("pid", 0, "server process id whose CPU is measured (0: none)")
	insBase   = flag.Int64("insert-base", 1_000_000_000, "first _id used by the insert workload (unique per run)")
	journal   = flag.Bool("j", true, "write concern j:true (w:1 always)")
	docSize   = flag.Int("doc-bytes", 1000, "size of the string field")
	loadBatch = flag.Int("load-batch", 500, "documents per insertMany while loading")
	label     = flag.String("label", "", "free text copied to the output")
)

const clkTck = 100.0 // sysconf(_SC_CLK_TCK) on Linux x86_64

// cpuSeconds returns utime+stime of a process (all threads) in seconds.
func cpuSeconds(p int) float64 {
	path := "/proc/self/stat"
	if p > 0 {
		path = "/proc/" + strconv.Itoa(p) + "/stat"
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	s := string(b)
	s = s[strings.LastIndexByte(s, ')')+2:]
	f := strings.Fields(s)
	// after ")": state(0) ... utime is field 14 of the full line => index 11 here, stime 12
	u, _ := strconv.ParseFloat(f[11], 64)
	st, _ := strconv.ParseFloat(f[12], 64)
	return (u + st) / clkTck
}

func payload(r *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(b)
}

func main() {
	flag.Parse()
	wc := writeconcern.W1()
	if *journal {
		wc = &writeconcern.WriteConcern{W: 1, Journal: boolPtr(true)}
	}
	opts := options.Client().ApplyURI(*uri).SetWriteConcern(wc).
		SetMaxPoolSize(uint64(*clients + 4)).SetMinPoolSize(uint64(min(*clients, 100))).
		SetServerSelectionTimeout(30 * time.Second).SetTimeout(60 * time.Second)
	cl, err := mongo.Connect(opts)
	if err != nil {
		fail(err)
	}
	defer cl.Disconnect(context.Background())
	colls := make([]*mongo.Collection, *dbs)
	for i := range colls {
		colls[i] = cl.Database(fmt.Sprintf("bench%d", i)).Collection("usertable")
	}
	switch *mode {
	case "load":
		load(colls)
	case "run":
		run(colls)
	default:
		fail(fmt.Errorf("unknown mode %q", *mode))
	}
}

func boolPtr(b bool) *bool { return &b }

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func load(colls []*mongo.Collection) {
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(1, min(*clients, 16)))
	var errs atomic.Int64
	for di, c := range colls {
		_ = c.Drop(context.Background())
		for lo := 0; lo < *docs; lo += *loadBatch {
			hi := min(lo+*loadBatch, *docs)
			wg.Add(1)
			sem <- struct{}{}
			go func(c *mongo.Collection, lo, hi, di int) {
				defer wg.Done()
				defer func() { <-sem }()
				r := rand.New(rand.NewPCG(uint64(di), uint64(lo)))
				batch := make([]any, 0, hi-lo)
				for id := lo; id < hi; id++ {
					batch = append(batch, bson.D{{Key: "_id", Value: int64(id)}, {Key: "f", Value: payload(r, *docSize)}})
				}
				if _, err := c.InsertMany(context.Background(), batch, options.InsertMany().SetOrdered(false)); err != nil {
					errs.Add(1)
					fmt.Fprintln(os.Stderr, "load error:", err)
				}
			}(c, lo, hi, di)
		}
	}
	wg.Wait()
	out := map[string]any{"mode": "load", "dbs": len(colls), "docs_per_db": *docs, "seconds": time.Since(start).Seconds(), "errors": errs.Load()}
	json.NewEncoder(os.Stdout).Encode(out)
	if errs.Load() > 0 {
		os.Exit(1)
	}
}

func run(colls []*mongo.Collection) {
	var measuring atomic.Bool
	var stop atomic.Bool
	var errs atomic.Int64
	lat := make([][]uint32, *clients) // microseconds, measured phase only
	var wg sync.WaitGroup
	for ci := 0; ci < *clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(ci)+1, uint64(time.Now().UnixNano())))
			newVal := payload(r, *docSize)
			nextIns := *insBase + int64(ci)*100_000_000
			ctx := context.Background()
			local := make([]uint32, 0, 1<<16)
			for !stop.Load() {
				c := colls[r.IntN(len(colls))]
				id := int64(r.IntN(*docs))
				t0 := time.Now()
				var err error
				switch *workload {
				case "read":
					err = c.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err()
				case "insert":
					_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: nextIns}, {Key: "f", Value: newVal}})
					nextIns++
				case "rw":
					if r.IntN(2) == 0 {
						err = c.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err()
					} else {
						var res *mongo.UpdateResult
						res, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "f", Value: newVal}}}})
						if err == nil && res.MatchedCount != 1 {
							err = fmt.Errorf("update matched %d", res.MatchedCount)
						}
					}
				}
				d := time.Since(t0)
				if err != nil {
					errs.Add(1)
					if errs.Load() <= 5 {
						fmt.Fprintln(os.Stderr, "op error:", err)
					}
					continue
				}
				if measuring.Load() {
					local = append(local, uint32(d.Microseconds()))
				}
			}
			lat[ci] = local
		}(ci)
	}
	time.Sleep(time.Duration(*warmup) * time.Second)
	errsAtWarm := errs.Load()
	srv0, gen0 := cpuSeconds(*pid), cpuSeconds(0)
	t0 := time.Now()
	measuring.Store(true)
	time.Sleep(time.Duration(*duration) * time.Second)
	measuring.Store(false)
	elapsed := time.Since(t0).Seconds()
	srv1, gen1 := cpuSeconds(*pid), cpuSeconds(0)
	stop.Store(true)
	wg.Wait()

	var all []uint32
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[min(len(all)-1, int(p*float64(len(all))))]) / 1000.0
	}
	ops := float64(len(all))
	out := map[string]any{
		"label": *label, "workload": *workload, "dbs": len(colls), "clients": *clients,
		"measured_s": elapsed, "ops": len(all), "ops_per_s": ops / elapsed,
		"p50_ms": pct(0.50), "p99_ms": pct(0.99),
		"errors_measured": errs.Load() - errsAtWarm, "errors_total": errs.Load(),
		"server_cpu_s": srv1 - srv0, "server_cores_used": (srv1 - srv0) / elapsed,
		"gen_cores_used": (gen1 - gen0) / elapsed,
	}
	if srv1-srv0 > 0 {
		out["ops_per_cpu_s"] = ops / (srv1 - srv0)
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
