package utils

import (
	"testing"
)

func TestSplitbuf(t *testing.T) {
	table := []struct {
		name     string
		buf      []byte
		count    int
		expected [][]byte
	}{
		{
			name:  "split in 3 parts",
			buf:   []byte{'a', 'a', 'a', '\n', 'b', 'b', 'b', 'b', 'b', '\n', 'c', '\n', 'd', '\n', 'e', '\n', 'f', 'f', 'f', 'f', '\n'}, // 21
			count: 3,
			expected: [][]byte{
				{'a', 'a', 'a', '\n', 'b', 'b', 'b', 'b', 'b', '\n'},
				{'c', '\n', 'd', '\n', 'e', '\n', 'f', 'f', 'f', 'f', '\n'},
			},
		},
		{
			name:  "split in 5 parts",
			buf:   []byte{'a', 'a', 'a', '\n', 'b', 'b', 'b', 'b', 'b', '\n', 'c', '\n', 'd', '\n', 'e', '\n', 'f', 'f', 'f', 'f', '\n'}, // 21
			count: 5,
			expected: [][]byte{
				{'a', 'a', 'a', '\n'},
				{'b', 'b', 'b', 'b', 'b', '\n'},
				{'c', '\n', 'd', '\n'},
				{'e', '\n', 'f', 'f', 'f', 'f', '\n'},
			},
		},

		{
			name:  "split in 21 parts",
			buf:   []byte{'a', 'a', 'a', '\n', 'b', 'b', 'b', 'b', 'b', '\n', 'c', '\n', 'd', '\n', 'e', '\n', 'f', 'f', 'f', 'f', '\n'}, // 21
			count: 21,
			expected: [][]byte{
				{'a', 'a', 'a', '\n'},
				{'b', 'b', 'b', 'b', 'b', '\n'},
				{'c', '\n'},
				{'d', '\n'},
				{'e', '\n'},
				{'f', 'f', 'f', 'f', '\n'},
			},
		},
		{
			name:  "split in 8 parts",
			buf:   []byte{'a', 'a', 'a', '\n', 'b', 'b', 'b', 'b', 'b', '\n', 'c', '\n', 'd', '\n', 'e', '\n', 'f', 'f', 'f', 'f', '\n', 'g', 'g', '\n'}, // 24
			count: 8,
			expected: [][]byte{
				{'a', 'a', 'a', '\n'},
				{'b', 'b', 'b', 'b', 'b', '\n'},
				{'c', '\n', 'd', '\n'},
				{'e', '\n', 'f', 'f', 'f', 'f', '\n'},
				{'g', 'g', '\n'},
			},
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			actual := Splitbuf(tc.buf, tc.count)
			if len(actual) != len(tc.expected) {
				t.Errorf("expected chunks to be %d but got %d", len(tc.expected), len(actual))
				t.FailNow()
			}

			for i, b := range actual {
				if len(b) != len(tc.expected[i]) {
					t.Errorf("expected chunk[%d] to be of size %d but got %d", i, len(tc.expected[i]), len(b))
					t.FailNow()
				}
			}
		})
	}
}
