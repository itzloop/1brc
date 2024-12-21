package processors

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/itzloop/1brc/constants"
	"github.com/itzloop/1brc/types"
	"github.com/itzloop/1brc/utils"
)

type ParallelReadOpts struct {
	Processors         int
	ProcessorChanSize  int
	AggregatorChanSize int
	Log                *log.Logger
}
type ParallelReadProcessor struct {
	globalAg     map[string]*types.AgMeasures
	aggregatorWG sync.WaitGroup
	processorWG  sync.WaitGroup
	opts         ParallelReadOpts
}

func NewParallelReadProcessor(opts ParallelReadOpts) *ParallelReadProcessor {
	if opts.Log == nil {
		opts.Log = log.New(io.Discard, "", 0)
	}

	return &ParallelReadProcessor{
		globalAg:     map[string]*types.AgMeasures{},
		aggregatorWG: sync.WaitGroup{},
		processorWG:  sync.WaitGroup{},
		opts:         opts,
	}
}

func (prp *ParallelReadProcessor) Process(p string) (result string, err error) {
	// create processrors
	agCh := make(chan map[string]*types.AgMeasures, prp.opts.AggregatorChanSize)
	prp.aggregatorWG.Add(1)
	go prp.aggregator(agCh)

	prp.processorWG.Add(prp.opts.Processors)
	processorChan := make(chan []byte, prp.opts.ProcessorChanSize)
	for i := 0; i < prp.opts.Processors; i++ {
		i := i
		go prp.process(i, processorChan, agCh)
	}

	var (
		chunkCount     = 8
		lookAheadBytes = 106
		chunksChanSize = 8
		readerCount    = 8
		readerWG       = sync.WaitGroup{}
		overallBytes   atomic.Int64
	)

	start := time.Now()
	chunks, err := splitFile(p, chunkCount, lookAheadBytes)
	if err != nil {
		return "", err
	}
	prp.opts.Log.Printf("it took %s to split file into %d chunks\n", time.Since(start), len(chunks))
	fmt.Println(chunks)

	start = time.Now()
	chunksChan := make(chan chunk, chunksChanSize)
	readerWG.Add(readerCount)
	for i := 0; i < readerCount; i++ {
		i := i
		go func(id int, ch <-chan chunk) {
			defer readerWG.Done()

			input, err := os.Open(p)
			if err != nil {
				prp.opts.Log.Panicf("reader %d: failed to open file: %v\n", id, err)
			}

			defer func() {
				if err = input.Close(); err != nil {
					prp.opts.Log.Printf("reader %d: error when trying to close the file: %v\n", id, err)
				}
			}()

			for chunk := range ch {
				_, err := input.Seek(chunk.offset, io.SeekStart)
				if err != nil {
					prp.opts.Log.Panicf("reader %d: failed to seek to %d: %v", id, chunk.offset, err)
				}

				remainingBytes := chunk.len
				var remainder []byte
				for remainingBytes > 0 {
					bufSize := chunk.len
					if chunk.len > constants.GiB { // limitation of go read call
						bufSize = constants.GiB
						chunk.len -= constants.GiB
					}
					buf := make([]byte, bufSize+int64(len(remainder)))
					start := time.Now()
					n, err := input.Read(buf[len(remainder):])
					prp.opts.Log.Printf("reader %d: it took %s to read %d bytes at offset %d\n", id, time.Since(start), bufSize, chunk.offset)
					remainingBytes -= int64(n)
					overallBytes.Add(int64(n))
					if err != nil {
						if err == io.EOF {
							prp.opts.Log.Printf("reader %d: EOF\n", id)
							break
						}
						prp.opts.Log.Panicf("failed to seek to %d: %v", chunk.offset, err)
					}

					// prepend remainder
					copy(buf, remainder)

				findRemainder:
					for i := len(buf) - 1; i >= 0; i-- {
						switch buf[i] {
						case '\n':
							remainder = buf[i+1:]
							buf = buf[:i+1]
							prp.opts.Log.Printf("reader %d: found %d bytes remainder buf[%d:%d]=%s\n", id, len(remainder), i+1, len(buf), string(remainder))
							break findRemainder
						}
					}

					// processorChan <- buf
					// TODO what about not split buffering??
					start = time.Now()
					bufs := splitbuf(buf, 4)
					for _, buf := range bufs {
						processorChan <- buf
					}
					prp.opts.Log.Printf("reader %d: it took %s to split buf and send to processors\n", id, time.Since(start))
				}
			}
		}(i, chunksChan)
	}

	for _, chunk := range chunks {
		chunksChan <- chunk
	}
	close(chunksChan)
	readerWG.Wait()
	prp.opts.Log.Printf("it took %s to fully read %d bytes\n", time.Since(start), overallBytes.Load())

	close(processorChan)
	prp.processorWG.Wait()

	close(agCh)
	prp.aggregatorWG.Wait()

	prp.opts.Log.Printf("it took %s to fully process and aggregate %d bytes\n", time.Since(start), overallBytes.Load())
	return types.AgMeasureMap(prp.globalAg).SortedString(), nil
}

func (prp *ParallelReadProcessor) aggregator(agCh <-chan map[string]*types.AgMeasures) {
	defer prp.aggregatorWG.Done()
	prp.opts.Log.Println("aggregator start")
	d := atomic.Int64{}
	for localAg := range agCh {
		start := time.Now()
		for k, v := range localAg {
			agM, ok := prp.globalAg[k]
			if !ok {
				agM = &types.AgMeasures{
					Min: 100,
					Max: -100,
				}
				prp.globalAg[k] = agM
			}
			agM.Max = max(agM.Max, v.Max)
			agM.Min = min(agM.Min, v.Min)
			agM.Total += float64(v.Total)
			agM.Count += v.Count
		}
		dd := time.Since(start)
		d.Add(int64(dd.Nanoseconds()))
		// prp.opts.Log.Printf("aggregator: it took %s to aggregate %d results\n", dd.String(), len(localAg))
	}

	prp.opts.Log.Printf("aggregator: it took %s to fully aggregate all results\n", time.Duration(d.Load()))
}

func (prp *ParallelReadProcessor) process(id int, ch <-chan []byte, resultsCh chan<- map[string]*types.AgMeasures) {
	defer prp.processorWG.Done()

	for buf := range ch {
		ag := map[string]*types.AgMeasures{}
		i := 0
		bol := 0  // begining of line
		eost := 0 // end of station name
		// start := time.Now()
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
					prp.opts.Log.Panicf("worker %d: failed to parse %v=buf[%d:%d]=%s, eost=%d to float: %v\n", id, buf[bol:i+1], bol, i+1, string(buf[bol:i]), eost, err)
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

		resultsCh <- ag

		// end := time.Since(start)
		// prp.opts.Log.Printf("worker %d: it took %s to proccess %d measurements\n", id, end, totalMeasurements)
	}
}

type chunk struct {
	id     int
	offset int64
	len    int64
}

func splitFile(p string, count, lookAheadBytes int) ([]chunk, error) {

	// TODO parallel
	input, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w\n", err)
	}
	defer input.Close()

	fInfo, err := input.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get stat of file: %w\n", err)
	}

	var (
		n          = fInfo.Size()
		chunkBytes = fInfo.Size() / int64(count)
		chunks     []chunk
		remainder  int64
		id         = 0
	)

	for i := int64(0); i < fInfo.Size(); i += chunkBytes + remainder {
		if i+chunkBytes >= n { // last chunk so just go to the end
			chunks = append(chunks, chunk{
				offset: i,
				len:    n - i,
				id:     id,
			})
			break
		}

		buf := make([]byte, lookAheadBytes)
		_, err := input.ReadAt(buf, i+chunkBytes)
		if err != nil && err != io.EOF {
			return nil, err
		}

		remainder = int64(bytes.IndexByte(buf, '\n'))
		if remainder == -1 {
			return nil, errors.New("wtf")
		}
		remainder += 1 // use as len not index

		chunks = append(chunks, chunk{
			offset: i,
			len:    chunkBytes + remainder,
			id:     id,
		})
		id++
	}

	return chunks, nil
}
