package types

import (
	"fmt"
	"sort"
	"strings"
)

type AgMeasures struct {
	Min   float32
	Max   float32
	Total float64
	Count int
}

type AgMeasureMap map[string]*AgMeasures

func (ag AgMeasureMap) SortedString() string {
	keys := make([]string, 0, len(ag))
	for k := range ag {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	str := strings.Builder{}
	str.WriteString("{")
	for i, k := range keys {
		v := ag[k]
		if i == len(keys)-1 {
			fmt.Fprintf(&str, "%s=%.1f/%.1f/%.1f", k, v.Min, v.Total/float64(v.Count), v.Max)
			continue
		}

		fmt.Fprintf(&str, "%s=%.1f/%.1f/%.1f, ", k, v.Min, v.Total/float64(v.Count), v.Max)
	}

	str.WriteString("}")
    
    return str.String()
}
