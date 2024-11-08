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
	"strconv"
	"strings"
	"time"
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
}

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

	input, err := os.Open(*inputPath)
	if err != nil {
		log.Panicf("failed to open file: %v\n", err)
	}

	defer func() {
		if err := input.Close(); err != nil {
			log.Printf("error when trying to close the file: %v\n", err)
		}
	}()

	ag := map[string]AgMeasures{}

	// read the file in chunks
	chunckSize := 1 * GiB
	buf := make([]byte, chunckSize)
	var remainder []byte
	overallDuration := int64(0)
	overallBytes := 0
	// for count := 0; count < 2; count++ {
	for {
		start := time.Now()
		//n, err := input.ReadAt(buf, int64((7+count)*1073741824))
		n, err := input.Read(buf)
		end := time.Since(start)
		overallDuration += end.Nanoseconds()
		overallBytes += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("EOF")
				break
			}

			log.Panicf("failed to read the input: %v\n", err)
		}
		log.Printf("it took %s to read %d bytes\n", end.String(), n)
		buf = buf[:n]
		buf = append(remainder, buf...)
		remainder = nil
		bol := 0  // begining of line
		eost := 0 // end of station name
		start = time.Now()
		totalMeasurements := 0
		i := 0
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
				m, err := strconv.ParseFloat(string(buf[eost+1:i]), 32)
				if err != nil {
					log.Panicf("failed to parse %v=buf[%d:%d]=%s, eost=%d to float: %v\n", buf[bol:i+1], bol, i+1, string(buf[bol:i]), eost, err)
				}

				stName := string(buf[bol:eost])
				agM, ok := ag[stName]
				if !ok {
					agM = AgMeasures{
						Min: 100,
						Max: -100,
					}
				} else {
					agM.Max = max(agM.Max, float32(m))
					agM.Min = min(agM.Min, float32(m))
					agM.Total += m
					agM.Count++
				}
				ag[stName] = agM
				totalMeasurements++
				bol = i + 1 // set bol to be start of next line
			}
		}

		end = time.Since(start)
		overallDuration += end.Nanoseconds()
		log.Printf("it took %s to proccess %d measurements\n", end, totalMeasurements)

		if bol < i {
			log.Printf("found %d bytes remainder buf[%d:%d]=%s\n", len(buf[bol:i]), bol, i, string(buf[bol:i]))
			remainder = append(remainder, buf[bol:i]...)
		}

	}

	log.Printf("it took %s to fully read %d bytes\n", time.Duration(overallDuration), overallBytes)

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
