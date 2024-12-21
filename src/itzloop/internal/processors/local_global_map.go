package processors

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/itzloop/1brc/constants"
	"github.com/itzloop/1brc/types"
	"github.com/itzloop/1brc/utils"
)

type LocalGlobalMapOpts struct {
	Processors         int
	ProcessorChanSize  int
	AggregatorChanSize int
	Log                *log.Logger
}
type LocalGlobalMapProcessor struct {
	globalAg     map[string]*types.AgMeasures
	aggregatorWG sync.WaitGroup
	processorWG  sync.WaitGroup
	opts         LocalGlobalMapOpts
}

func NewLocalGlobalMapProcessor(opts LocalGlobalMapOpts) *LocalGlobalMapProcessor {
	if opts.Log == nil {
		opts.Log = log.New(io.Discard, "", 0)
	}

	return &LocalGlobalMapProcessor{
		globalAg:     map[string]*types.AgMeasures{},
		aggregatorWG: sync.WaitGroup{},
		processorWG:  sync.WaitGroup{},
		opts:         opts,
	}
}

func (sbp *LocalGlobalMapProcessor) Process(p string) (result string, err error) {
	// create processrors
	agCh := make(chan map[string]*types.AgMeasures, sbp.opts.AggregatorChanSize)
	sbp.aggregatorWG.Add(1)
	go sbp.aggregator(agCh)

	sbp.processorWG.Add(sbp.opts.Processors)
	ch := make(chan []byte, sbp.opts.ProcessorChanSize)
	for i := 0; i < sbp.opts.Processors; i++ {
		i := i
		go sbp.process(i, ch, agCh)
	}

	input, err := os.Open(p)
	if err != nil {
		return "", fmt.Errorf("failed to pen file: %w\n", err)
	}

	defer func() {
		if err = input.Close(); err != nil {
			sbp.opts.Log.Printf("error when trying to close the file: %v\n", err)
		}
	}()

	// read the file in chunks
	start := time.Now()
	chunckSize := 1 * constants.GiB
	var remainder []byte
	overallBytes := 0
	// for count := 0; count < 2; count++ {
	for {
		start := time.Now()
		//n, err := input.ReadAt(buf, int64((7+count)*1073741824))
		buf := make([]byte, chunckSize+len(remainder))
		n, err := input.Read(buf[len(remainder):])
		end := time.Since(start)
		overallBytes += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				sbp.opts.Log.Println("EOF")
				break
			}

			return "", fmt.Errorf("failed to read the input:  %w\n", err)
		}
		sbp.opts.Log.Printf("it took %s to read %d bytes\n", end.String(), n)

		// prepend reminder
		copy(buf, remainder)

		// find new remainder on the new buffer
	findRemainder:
		for i := len(buf) - 1; i >= 0; i-- {
			switch buf[i] {
			case '\n':
				remainder = buf[i+1:]
				buf = buf[:i+1]
				sbp.opts.Log.Printf("found %d bytes remainder buf[%d:%d]=%s\n", len(remainder), i+1, len(buf), string(remainder))
				break findRemainder
			}
		}

		ch <- buf
	}

	sbp.opts.Log.Printf("it took %s to fully read %d bytes\n", time.Since(start), overallBytes)
	close(ch)

	sbp.processorWG.Wait()
	close(agCh)
	sbp.aggregatorWG.Wait()

	sbp.opts.Log.Printf("it took %s to fully process %d bytes\n", time.Since(start), overallBytes)
	return types.AgMeasureMap(sbp.globalAg).SortedString(), nil
}

func (sbp *LocalGlobalMapProcessor) aggregator(agCh <-chan map[string]*types.AgMeasures) {
	defer sbp.aggregatorWG.Done()
	sbp.opts.Log.Println("aggregator start")
	start := time.Now()
	for localAg := range agCh {
		start := time.Now()
		for k, v := range localAg {
			agM, ok := sbp.globalAg[k]
			if !ok {
				agM = &types.AgMeasures{
					Min: 100,
					Max: -100,
				}
				sbp.globalAg[k] = agM
			}

			agM.Max = max(agM.Max, v.Max)
			agM.Min = min(agM.Min, v.Min)
			agM.Total += float64(v.Total)
			agM.Count += v.Count
		}
		dd := time.Since(start)
		sbp.opts.Log.Printf("aggregator: it took %s to aggregate %d results\n", dd.String(), len(localAg))
	}

	sbp.opts.Log.Printf("aggregator: it took %s to fully aggregate all results\n", time.Since(start))
}

func (sbp *LocalGlobalMapProcessor) process(id int, ch <-chan []byte, resultsCh chan<- map[string]*types.AgMeasures) {
	defer sbp.processorWG.Done()
	for buf := range ch {
		ag := map[string]*types.AgMeasures{}
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
					sbp.opts.Log.Panicf("worker %d: failed to parse %v=buf[%d:%d]=%s, eost=%d to float: %v\n", id, buf[bol:i+1], bol, i+1, string(buf[bol:i]), eost, err)
				}
				stNameSubSlice := buf[bol:eost]
				stName := unsafe.String(&stNameSubSlice[0], len(stNameSubSlice))
				agM, ok := ag[stName]
				if !ok {
					agM = &types.AgMeasures{
						Min: 100,
						Max: -100,
					}
					ag[stName] = agM
				}

				agM.Max = max(agM.Max, m)
				agM.Min = min(agM.Min, m)
				agM.Total += float64(m)
				agM.Count++

				totalMeasurements++
				bol = i + 1 // set bol to be start of next line
			}
		}

		end := time.Since(start)
		sbp.opts.Log.Printf("worker %d: it took %s to proccess %d measurements\n", id, end, totalMeasurements)

		resultsCh <- ag
	}
}
