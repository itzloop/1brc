package processors

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelRead(t *testing.T) {

	table := []struct {
		name   string
		data   []byte
		chunk  int
		result []chunk
	}{
		{
			name:  "case 1",
			data:  []byte("aaa\nbbb\nc\nd\neeeeeeee\nff\ngggggg\nhhhhh\ni\nj\nk\nl\n"),
			chunk: 2,
			result: []chunk{
				{
					offset: 0,
					len:    24,
				},
				{
					offset: 25,
					len:    21,
				},
			},
		},
		{
			name:  "case 2",
			data:  []byte("aaa\nbbb\nc\nd\neeeeeeee\nff\ngggggg\nhhhhh\ni\nj\nk\nl\n"),
			chunk: 3,
			result: []chunk{
				{
					offset: 0,
					len:    21,
				},
				{
					offset: 22,
					len:    15,
				},
				{
					offset: 38,
					len:    8,
				},
			},
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := path.Join(dir, "sample.txt")
			f, err := os.Create(p)
			require.NoError(t, err)

			_, err = f.Write(tc.data)
			require.NoError(t, err)

			f.Close()

			chunks, err := splitFile(p, tc.chunk, 10)
			require.NoError(t, err)
			assert.Len(t, chunks, len(tc.result))

			for i, chunk := range chunks {
				assert.EqualValues(t, tc.result[i].offset, chunk.offset)
				assert.EqualValues(t, tc.result[i].len, chunk.len)
			}

		})
	}

}
