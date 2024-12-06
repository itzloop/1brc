package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/itzloop/1brc/utils"
)

const (
	B   = 1
	KiB = B * 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

type AgMeasures struct {
	Min   float32
	Max   float32
	Total float64
	Count int
	mu    *sync.Mutex
}

var (
	overallDuration atomic.Int64
	ag              = map[string]*AgMeasures{}
	mu              = sync.RWMutex{}
	wg              = sync.WaitGroup{}
	processors      = 4
)

func main() {
	log.SetOutput(os.Stdout)

	inputPath := flag.String("i", "", "path to input file")
	cpuProf := flag.Bool("cpu", false, "run pprof cpu profiling")
	heapProf := flag.Bool("heap", false, "run pprof heap profiling")
	traceProf := flag.Bool("trace", false, "run trace profiling")
	flag.Parse()

	now := time.Now()

	if *cpuProf {
		n := fmt.Sprintf("cpu_prof-%s.pb.gz", now.Format("2006-01-02T15-04"))
		log.Println("running cpu profiler...")
		log.Printf("cpu profiling data will be saved in %s\n", n)

		cpuProf, err := os.Create(n)
		if err != nil {
			log.Panicf("failed to create file [%s]: %v\n", n, err)
		}

		defer cpuProf.Close()

		if err := pprof.StartCPUProfile(cpuProf); err != nil {
			log.Panicf("failed to start cpu profiler: %v\n", err)
		}

		defer pprof.StopCPUProfile()
	}

	if *traceProf {
		n := fmt.Sprintf("trace-%s.trace", now.Format("2006-01-02T15-04"))
		log.Println("running trace profiler...")
		log.Printf("trace data will be saved in %s\n", n)

		traceProf, err := os.Create(n)
		if err != nil {
			log.Panicf("failed to create file [%s]: %v\n", n, err)
		}

		defer traceProf.Close()

		if err := trace.Start(traceProf); err != nil {
			log.Panicf("failed to start cpu profiler: %v\n", err)
		}

		defer trace.Stop()
	}

	// create processrors
	wg.Add(processors)
	ch := make(chan []byte, processors)
	for i := 0; i < processors; i++ {
		i := i
		go process(i, ch)
	}

	input, err := os.Open(*inputPath)
	if err != nil {
		log.Panicf("failed to open file: %v\n", err)
	}

	defer func() {
		if err := input.Close(); err != nil {
			log.Printf("error when trying to close the file: %v\n", err)
		}
	}()

	// read the file in chunks
	start := time.Now()
	chunckSize := 1 * GiB
	var remainder []byte
	overallBytes := 0
	// for count := 0; count < 2; count++ {
	for {
		start := time.Now()
		//n, err := input.ReadAt(buf, int64((7+count)*1073741824))
		buf := make([]byte, chunckSize+len(remainder))
		n, err := input.Read(buf[len(remainder):])
		end := time.Since(start)
		overallDuration.Add(end.Nanoseconds())
		overallBytes += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("EOF")
				break
			}

			log.Panicf("failed to read the input: %v\n", err)
		}
		log.Printf("it took %s to read %d bytes\n", end.String(), n)

		// prepend reminder
		copy(buf, remainder)

		// find new remainder on the new buffer
	findRemainder:
		for i := len(buf) - 1; i >= 0; i-- {
			switch buf[i] {
			case '\n':
				remainder = buf[i+1:]
				buf = buf[:i+1]
				log.Printf("found %d bytes remainder buf[%d:%d]=%s\n", len(remainder), i+1, len(buf), string(remainder))
				break findRemainder
			}
		}

		ch <- buf
	}

	log.Printf("it took %s to fully read %d bytes\n", time.Since(start), overallBytes)
	close(ch)
	wg.Wait()

	log.Printf("it took %s to fully process %d bytes\n", time.Duration(overallDuration.Load()), overallBytes)

	str := strings.Builder{}
	str.WriteString("{")
	for k, v := range ag {
		fmt.Fprintf(&str, "%s=%.1f/%.1f/%.1f, ", k, v.Min, v.Total/float64(v.Count), v.Max)
	}

	str.WriteString("}")

	fmt.Println(str.String())

	if *heapProf {
		n := fmt.Sprintf("heap_prof-%s.pb.gz", now.Format("2006-01-02T15-04"))
		log.Printf("heap profiling data will be saved in %s\n", n)

		heapProf, err := os.Create(n)
		if err != nil {
			log.Panicf("failed to create file [%s]: %v\n", n, err)
		}
		defer heapProf.Close()

		if err := pprof.WriteHeapProfile(heapProf); err != nil {
			log.Panicf("failed to start cpu profiler: %v\n", err)
		}
	}

}

func process(id int, ch <-chan []byte) {
	defer wg.Done()

	for buf := range ch {
		i := 0
		bol := 0  // begining of line
		eost := 0 // end of station name
		start := time.Now()
		totalMeasurements := 0
		for i = 0; i < len(buf); i++ {
			switch buf[i] {
			case ';': // means we reached the end of station name
				eost = i
			case '\n':
				if eost < bol {
					continue
				}
				// buf[bol:eost]: station name
				// buf[eost + 1:i]: measurement
				m, err := utils.BtofV2(buf[eost+1 : i])
				if err != nil {
					log.Panicf("worker %d: failed to parse %v=buf[%d:%d]=%s, eost=%d to float: %v\n", id, buf[bol:i+1], bol, i+1, string(buf[bol:i]), eost, err)
				}
				stNameSubSlice := buf[bol:eost]
				stName := unsafe.String(&stNameSubSlice[0], len(stNameSubSlice))
				mu.Lock()
				agM, ok := ag[stName]
				if !ok {
					agM = &AgMeasures{
						Min: 100,
						Max: -100,
                        mu: &sync.Mutex{},
					}
					ag[stName] = agM
                    mu.Unlock()
				} else {
                    mu.Unlock()

                    agM.mu.Lock()
					agM.Max = max(agM.Max, m)
					agM.Min = min(agM.Min, m)
					agM.Total += float64(m)
					agM.Count++
                    agM.mu.Unlock()
				}

				totalMeasurements++
				bol = i + 1 // set bol to be start of next line
			}
		}

		end := time.Since(start)
		overallDuration.Add(end.Nanoseconds())
		log.Printf("worker %d: it took %s to proccess %d measurements\n", id, end, totalMeasurements)
	}
}
