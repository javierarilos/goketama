package hashbench

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/cespare/xxhash"
	"github.com/spaolacci/murmur3"
	"hash/crc32"
	"sync"
	"testing"
)

func BenchmarkMurmurHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes := make([]byte, 256)
		binary.LittleEndian.PutUint32(bytes, uint32(i))

		i := murmur3.Sum32(bytes)
		i++
	}
}

var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

func BenchmarkMurmurHashWithPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes := keyBufPool.Get().(*[]byte)
		binary.LittleEndian.PutUint32(*bytes, uint32(i))

		i := murmur3.Sum32(*bytes)
		i++

		keyBufPool.Put(bytes)
	}
}

func BenchmarkHashMd5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes := make([]byte, 256)
		binary.LittleEndian.PutUint32(bytes, uint32(i))

		// md5 calculation and obtaining an uint32 from the sum
		sum := md5.Sum(bytes)
		i := binary.LittleEndian.Uint32(sum[:4])
		i++
	}
}

func BenchmarkXxHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes := make([]byte, 256)
		binary.LittleEndian.PutUint32(bytes, uint32(i))

		i := uint32(xxhash.Sum64(bytes))
		i++

	}
}

func BenchmarkCrc32Hash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes := make([]byte, 256)
		binary.LittleEndian.PutUint32(bytes, uint32(i))

		crc32.ChecksumIEEE(bytes)

	}
}
