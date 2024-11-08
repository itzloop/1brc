package utils

import (
	"strconv"
	"testing"
)

var table = []struct {
	n      []byte
	result float32
}{
	{n: []byte("1.0"), result: 1.0},
	{n: []byte("-1.0"), result: -1.0},
	{n: []byte("-0.2"), result: -0.2},
	{n: []byte("0.2"), result: 0.2},
	{n: []byte("9.9"), result: 9.9},
	{n: []byte("-9.9"), result: -9.9},
	{n: []byte("99.9"), result: 99.9},
	{n: []byte("-99.9"), result: -99.9},
	{n: []byte("21.0"), result: 21.0},
	{n: []byte("-21.0"), result: -21.0},
}

func TestBtof(t *testing.T) {
	for _, test := range table {

		t.Run(string(test.n), func(t *testing.T) {
			result, err := Btof(test.n)
			if err != nil {
				t.Errorf("expected to have no error but got: %v", err)
				return
			}

			if result != test.result {
				t.Errorf("expected to have %.1f but got %.1f", test.result, result)
			}
		})
	}
}

func TestBtofV2(t *testing.T) {
	for _, test := range table {

		t.Run(string(test.n), func(t *testing.T) {
			result, err := BtofV2(test.n)
			if err != nil {
				t.Errorf("expected to have no error but got: %v", err)
				return
			}

			if result != test.result {
				t.Errorf("expected to have %.1f but got %.1f", test.result, result)
			}
		})
	}
}

var (
	result float32
	err    error
)

func BenchmarkBtof(b *testing.B) {
	var (
		r float32
		e error
	)
	for _, bench := range table {
		b.Run(string(bench.n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r, e = Btof(bench.n)
			}
		})
	}

	result, err = r, e
}

func BenchmarkBtofV2(b *testing.B) {
	var (
		r float32
		e error
	)

	for _, bench := range table {
		b.Run(string(bench.n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r, e = BtofV2(bench.n)
			}
		})
	}

	result, err = r, e
}

func BenchmarkParseFloat(b *testing.B) {
	var (
		r float64
		e error
	)

	for _, bench := range table {
		b.Run(string(bench.n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r, e = strconv.ParseFloat(string(bench.n), 32)
			}
		})
	}

	result, err = float32(r), e
}
