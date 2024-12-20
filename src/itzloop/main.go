package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/itzloop/1brc/internal/processors"
)

type AgMeasures struct {
	Min   float32
	Max   float32
	Total float64
	Count int
}

var (
	processorCount     = 8
	processorChanSize  = 16
	aggregatorChanSize = 8
)

func main() {
	inputPath := flag.String("i", "", "path to input file")
	cpuProf := flag.Bool("cpu", false, "run pprof cpu profiling")
	heapProf := flag.Bool("heap", false, "run pprof heap profiling")
	traceProf := flag.Bool("trace", false, "run trace profiling")
	disableLog := flag.Bool("disable-log", false, "disable logging")
	flag.Parse()

	if *disableLog {
		log.SetOutput(io.Discard)
	}

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

	result, err := processors.NewSplitBufProcessor(processors.SplitBufOpts{
		Processors:         processorCount,
		ProcessorChanSize:  processorChanSize,
		AggregatorChanSize: aggregatorChanSize,
	}).Process(*inputPath)

	if err != nil {
		log.Panicln(err)
	}

	fmt.Println(result)

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
