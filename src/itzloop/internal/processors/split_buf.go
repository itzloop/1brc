package processors

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/itzloop/1brc/constants"
	"github.com/itzloop/1brc/types"
	"github.com/itzloop/1brc/utils"
)

type SplitBufOpts struct {
	Processors         int
	ProcessorChanSize  int
	AggregatorChanSize int
}
type SplitBufProcessor struct {
	globalAg     map[string]*types.AgMeasures
	aggregatorWG sync.WaitGroup
	processorWG  sync.WaitGroup
	opts         SplitBufOpts
}

func NewSplitBufProcessor(opts SplitBufOpts) *SplitBufProcessor {
	return &SplitBufProcessor{
		globalAg:     map[string]*types.AgMeasures{},
		aggregatorWG: sync.WaitGroup{},
		processorWG:  sync.WaitGroup{},
		opts:         opts,
	}
}

func (sbp *SplitBufProcessor) Process(p string) (result string, err error) {
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
			log.Printf("error when trying to close the file: %v\n", err)
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
		// overallDuration.Add(end.Nanoseconds())
		overallBytes += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("EOF")
				break
			}

			return "", fmt.Errorf("failed to read the input:  %w\n", err)
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

		// split buf
		bufs := utils.Splitbuf(buf, sbp.opts.Processors)
		for _, buf := range bufs {
			ch <- buf
		}
	}

	log.Printf("it took %s to fully read %d bytes\n", time.Since(start), overallBytes)
	close(ch)

	sbp.processorWG.Wait()
	close(agCh)
	sbp.aggregatorWG.Wait()

	log.Printf("it took %s to fully process %d bytes\n", time.Since(start), overallBytes)

	str := strings.Builder{}
	str.WriteString("{")
	for k, v := range sbp.globalAg {
		fmt.Fprintf(&str, "%s=%.1f/%.1f/%.1f, ", k, v.Min, v.Total/float64(v.Count), v.Max)
	}

	str.WriteString("}")

	return str.String(), nil
}

func (sbp *SplitBufProcessor) aggregator(agCh <-chan map[string]*types.AgMeasures) {
	defer sbp.aggregatorWG.Done()
	log.Println("aggregator start")
	d := atomic.Int64{}
	for localAg := range agCh {
		start := time.Now()
		for k, v := range localAg {
			agM, ok := sbp.globalAg[k]
			if !ok {
				sbp.globalAg[k] = &types.AgMeasures{
					Min: 100,
					Max: -100,
				}
			} else {
				agM.Max = max(agM.Max, v.Max)
				agM.Min = min(agM.Min, v.Min)
				agM.Total += float64(v.Total)
				agM.Count += v.Count
			}
		}
		dd := time.Since(start)
		d.Add(int64(dd.Nanoseconds()))
		log.Printf("aggregator: it took %s to aggregate %d results\n", dd.String(), len(localAg))
	}

	log.Printf("aggregator: it took %s to fully aggregate all results\n", time.Duration(d.Load()))
}

func (sbp *SplitBufProcessor) process(id int, ch <-chan []byte, resultsCh chan<- map[string]*types.AgMeasures) {
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
					log.Panicf("worker %d: failed to parse %v=buf[%d:%d]=%s, eost=%d to float: %v\n", id, buf[bol:i+1], bol, i+1, string(buf[bol:i]), eost, err)
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
				} else {
					agM.Max = max(agM.Max, m)
					agM.Min = min(agM.Min, m)
					agM.Total += float64(m)
					agM.Count++
				}

				totalMeasurements++
				bol = i + 1 // set bol to be start of next line
			}
		}

		resultsCh <- ag

		end := time.Since(start)
		log.Printf("worker %d: it took %s to proccess %d measurements\n", id, end, totalMeasurements)
	}
}
