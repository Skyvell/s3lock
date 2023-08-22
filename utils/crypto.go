package utils

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

func GenerateRandomNumberInInterval(min, max int) (int, error) {
	if min > max {
		return 0, fmt.Errorf("Min: %d is greater than max: %d", min, max)
	}

	diff := max - min
	maxBigInt := big.NewInt(int64(diff + 1))
	n, err := rand.Int(rand.Reader, maxBigInt)
	if err != nil {
		return 0, err
	}

	return int(n.Int64()) + min, nil
}
