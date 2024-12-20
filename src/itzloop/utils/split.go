package utils

func Splitbuf(buf []byte, count int) [][]byte {
	n := len(buf)
	remainder := 0
	chunkBytes := n / count
	var chunks [][]byte

	for i := 0; i < n; i += chunkBytes + remainder {
		remainder = 0
        if i + chunkBytes < n {
            for j, c := range buf[i+chunkBytes - 1:] {
                if c == '\n' {
                    remainder = j
                    break
                }
            }
        } else {
            chunkBytes = n - i
        if i + chunkBytes > n {
            if chunkBytes > 100 {
                chunkBytes = n - 100
            } else {
                chunkBytes = 0
            }
        }
        }
		chunks = append(chunks, buf[i:i+chunkBytes+remainder])
	}

	return chunks
}
