// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"encoding/binary"
	"math/bits"
	"testing"
)

func init() {
	if EliasGammaLen(1) != 1 {
		panic(EliasGammaLen(1))
	}
	if EliasGammaLen(2) != 3 {
		panic(EliasGammaLen(2))
	}
	if EliasGammaLen(3) != 3 {
		panic(EliasGammaLen(3))
	}
	if EliasGammaLen(4) != 5 {
		panic(EliasGammaLen(4))
	}
	if EliasDeltaLen(1) != 1 {
		panic(EliasDeltaLen(1))
	}
	if EliasDeltaLen(2) != 4 {
		panic(EliasDeltaLen(2))
	}
	if EliasDeltaLen(3) != 4 {
		panic(EliasDeltaLen(3))
	}
	if EliasDeltaLen(4) != 5 {
		panic(EliasDeltaLen(4))
	}
	if EliasDeltaLen(17) != 9 {
		panic(EliasDeltaLen(4))
	}
	if FibonacciLen(1) != 2 {
		panic(FibonacciLen(1))
	}
	if FibonacciLen(2) != 3 {
		panic(FibonacciLen(2))
	}
	if FibonacciLen(3) != 4 {
		panic(FibonacciLen(3))
	}
	if FibonacciLen(5) != 5 {
		panic(FibonacciLen(5))
	}
	if FibonacciLen(8) != 6 {
		panic(FibonacciLen(8))
	}
	if FibonacciLen(9) != 6 {
		panic(FibonacciLen(9))
	}
	if FibonacciLen(11) != 6 {
		panic(FibonacciLen(11))
	}
	if FibonacciLen(12) != 6 {
		panic(FibonacciLen(12))
	}
	if FibonacciLen(13) != 7 {
		panic(FibonacciLen(13))
	}
}

func EliasGammaLen(n int) int {
	lg1 := bits.Len(uint(n)) - 1
	if lg1 < 0 {
		panic("bad len")
	}
	return lg1 + 1 + lg1
}

func EliasDeltaLen(n int) int {
	lg1 := bits.Len(uint(n))
	return EliasGammaLen(lg1) + lg1 - 1
}

var fib = []int{
	1,
	2,
	3,
	5,
	8,
	13,
	21,
	34,
	55,
	89,
	144,
	233,
	377,
	610,
	987,
	1597,
	2584,
	4181,
	6765,
	10946,
	17711,
	28657,
	46368,
	75025,
	121393,
	196418,
	317811,
	514229,
	832040,
	1346269,
	2178309,
	3524578,
	5702887,
	9227465,
	14930352,
	24157817,
	39088169,
	63245986,
	102334155,
	165580141,
	267914296,
	433494437,
	701408733,
	1134903170,
	1836311903,
	2971215073,
	4807526976,
	7778742049,
	12586269025,
	20365011074,
	32951280099,
	53316291173,
	86267571272,
	139583862445,
	225851433717,
	365435296162,
	591286729879,
	956722026041,
	1548008755920,
	2504730781961,
	4052739537881,
	6557470319842,
	10610209857723,
	17167680177565,
	27777890035288,
	44945570212853,
	72723460248141,
	117669030460994,
	190392490709135,
	308061521170129,
	498454011879264,
	806515533049393,
	1304969544928657,
	2111485077978050,
	3416454622906707,
	5527939700884757,
	8944394323791464,
	14472334024676221,
	23416728348467685,
	37889062373143906,
	61305790721611591,
	99194853094755497,
	160500643816367088,
	259695496911122585,
	420196140727489673,
	679891637638612258,
	1100087778366101931,
	1779979416004714189,
	2880067194370816120,
	4660046610375530309,
	7540113804746346429,
	1<<63 - 1,
}

func FibonacciLen(n int) int {
	lg := 0
	for fib[lg] <= n {
		lg++
	}
	return lg + 1
}

func TestPostGamma(t *testing.T) {
	t.Skip("gamma")
	ix := Open("/Users/rsc/.csearchindex")
	post := ix.data.d[ix.postData:ix.nameIndex]
	println(len(post))
	countG, countD, countF, n := 0, 0, 0, 0
	for len(post) > 0 {
		n++
		post = post[3:]
		countG += 3 * 8
		countD += 3 * 8
		countF += 3 * 8
		for len(post) > 0 {
			d, n := binary.Uvarint(post)
			post = post[n:]
			countG += EliasGammaLen(int(d + 1))
			countD += EliasDeltaLen(int(d + 1))
			countF += FibonacciLen(int(d + 1))
			if d == 0 {
				break
			}
		}
	}
	println((countG+7)/8, (countD+7)/8, (countF+7)/8, n)
}
