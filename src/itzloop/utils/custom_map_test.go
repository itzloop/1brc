package utils

import (
	"testing"

)

func BenchmarkSlice(b *testing.B) {
	a := make([]int, 10000)
	a[len(a)-1] = 10000
	b.Run("10000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i := 0; i < len(a); i++ {
				if a[i] == 10000 {
					break
				}
			}
		}
	})
}

var (
	v  int
	ok bool
)

func BenchmarkMap(b *testing.B) {
	a := map[int]int{}
	for i := 0; i < 10000; i++ {
		a[i] = i + 1
	}
	b.Run("10000", func(b *testing.B) {
		var (
			vv   int
			okok bool
		)
		for i := 0; i < b.N; i++ {
			vv, okok = a[10000]
		}

        v, ok = vv, okok
	})
}

func TestUniqeness(t *testing.T) {
}

