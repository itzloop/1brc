package utils

import (
	"errors"
	"strconv"
)

var (
	ErrFloatInvalidLenght = errors.New("n lenght is less than 3")
    ErrWTF = errors.New("wtf")
)

func Btof(n []byte) (float32, error) {
	if len(n) < 3 {
		return 0, ErrFloatInvalidLenght
	}

	// decimal digit: n[len(n) - 1]
	// decimal point: n[len(n) - 2]
	// integer part: n[:len(n) - 2]

	l := len(n)

	i, err := strconv.ParseInt(string(n[:l-2]), 10, 32)
	if err != nil {
		return 0, err
	}

	if i < 0 || n[0] == '-' {
		return float32(i) - 0.1*float32(n[l-1]-'0'), nil
	}
	return float32(i) + 0.1*float32(n[l-1]-'0'), nil
}

func BtofV2(n []byte) (float32, error) {
	if len(n) < 3 {
		return 0, ErrFloatInvalidLenght
	}

	// decimal digit: n[len(n) - 1]
	// decimal point: n[len(n) - 2]
	// integer part: n[:len(n) - 2]

	l := len(n)

	negative := n[0] == '-'

	if negative {
		switch len(n[1 : l-2]) {
		case 1:
			return -float32((n[1] - '0')) - 0.1*float32(n[l-1]-'0'), nil
		case 2:
			return -10*float32((n[1]-'0')) - float32((n[2] - '0')) - 0.1*float32(n[l-1]-'0'), nil
		}
	} else {
		switch len(n[0 : l-2]) {
		case 1:
			return  float32((n[0] - '0')) + 0.1*float32(n[l-1]-'0'), nil
		case 2:
			return +10*float32((n[0]-'0')) + float32((n[1] - '0')) + 0.1*float32(n[l-1]-'0'), nil
		}
	}

    return 0, ErrWTF
}
