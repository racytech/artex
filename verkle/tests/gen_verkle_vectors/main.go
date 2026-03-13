// gen_verkle_vectors generates test vectors using go-verkle (reference implementation).
// Outputs a binary file consumed by test_verkle_cross_validation.c.
//
// Format:
//   [4 bytes LE: num_scenarios]
//   For each scenario:
//     [2 bytes LE: name_len]
//     [name_len bytes: scenario name (UTF-8)]
//     [1 byte: scenario_type]  0=build, 1=multiblock
//     [4 bytes LE: num_keys]
//     For each key:
//       [32 bytes: key]
//       [32 bytes: value]
//     [32 bytes: expected_root_hash]   (serialized commitment)
//     If scenario_type == 1 (multiblock):
//       [4 bytes LE: num_blocks]
//       For each block:
//         [4 bytes LE: num_ops]
//         For each op:
//           [32 bytes: key]
//           [32 bytes: value]
//         [32 bytes: expected_root_after_block]

package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	verkle "github.com/ethereum/go-verkle"
)

// ---- helpers ----

func makeKey(i uint64) []byte {
	h := sha256.Sum256(binary.LittleEndian.AppendUint64(nil, i))
	return h[:]
}

func makeValue(i uint64) []byte {
	v := make([]byte, 32)
	binary.LittleEndian.PutUint64(v, i)
	return v
}

func makeStemKey(stem []byte, suffix byte) []byte {
	k := make([]byte, 32)
	copy(k, stem[:31])
	k[31] = suffix
	return k
}

func rootHash(kvs [][2][]byte) []byte {
	root := verkle.New()
	for _, kv := range kvs {
		if err := root.Insert(kv[0], kv[1], nil); err != nil {
			panic(fmt.Sprintf("insert failed: %v", err))
		}
	}
	comm := root.Commit()
	b := comm.Bytes()
	return b[:]
}

// ---- binary writing ----

func writeU16(f *os.File, v uint16) {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	f.Write(b)
}

func writeU32(f *os.File, v uint32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	f.Write(b)
}

func writeBytes(f *os.File, data []byte) {
	f.Write(data)
}

// ---- scenario types ----

type kv = [2][]byte

type scenario struct {
	name     string
	typ      byte // 0=build, 1=multiblock
	initial  []kv
	blocks   [][]kv // only for multiblock
}

func writeBuildScenario(f *os.File, s scenario) {
	hash := rootHash(s.initial)
	fmt.Printf("  %s: %d keys, root=%x\n", s.name, len(s.initial), hash)

	// name
	writeU16(f, uint16(len(s.name)))
	writeBytes(f, []byte(s.name))
	// type
	f.Write([]byte{0})
	// keys
	writeU32(f, uint32(len(s.initial)))
	for _, kv := range s.initial {
		writeBytes(f, kv[0])
		writeBytes(f, kv[1])
	}
	// root
	writeBytes(f, hash)
}

func writeMultiblockScenario(f *os.File, s scenario) {
	// Compute initial root
	initialHash := rootHash(s.initial)
	fmt.Printf("  %s: %d initial, %d blocks\n", s.name, len(s.initial), len(s.blocks))

	// name
	writeU16(f, uint16(len(s.name)))
	writeBytes(f, []byte(s.name))
	// type
	f.Write([]byte{1})
	// initial keys
	writeU32(f, uint32(len(s.initial)))
	for _, kv := range s.initial {
		writeBytes(f, kv[0])
		writeBytes(f, kv[1])
	}
	writeBytes(f, initialHash)

	// blocks
	writeU32(f, uint32(len(s.blocks)))
	merged := make(map[[32]byte][]byte)
	for _, kv := range s.initial {
		var k [32]byte
		copy(k[:], kv[0])
		merged[k] = kv[1]
	}

	for i, block := range s.blocks {
		for _, kv := range block {
			var k [32]byte
			copy(k[:], kv[0])
			merged[k] = kv[1]
		}
		// Build full state for root
		all := make([]kv, 0, len(merged))
		for k, v := range merged {
			key := make([]byte, 32)
			copy(key, k[:])
			all = append(all, kv{key, v})
		}
		sort.Slice(all, func(i, j int) bool {
			for b := 0; b < 32; b++ {
				if all[i][0][b] != all[j][0][b] {
					return all[i][0][b] < all[j][0][b]
				}
			}
			return false
		})
		hash := rootHash(all)
		fmt.Printf("    block %d: %d ops, root=%x\n", i+1, len(block), hash)

		// Write block ops
		writeU32(f, uint32(len(block)))
		for _, kv := range block {
			writeBytes(f, kv[0])
			writeBytes(f, kv[1])
		}
		writeBytes(f, hash)
	}
}

func main() {
	var scenarios []scenario

	// =====================================================================
	// Build scenarios — insert keys, check root hash
	// =====================================================================

	// 1. Two keys, different stems (minimal tree with internal root)
	{
		stemA := make([]byte, 31)
		stemA[0] = 0xAA
		stemB := make([]byte, 31)
		stemB[0] = 0xBB
		scenarios = append(scenarios, scenario{
			name: "two keys different stems",
			typ:  0,
			initial: []kv{
				{makeStemKey(stemA, 0), makeValue(1)},
				{makeStemKey(stemB, 0), makeValue(2)},
			},
		})
	}

	// 2. Same stem, multiple suffixes (C1 + C2)
	{
		stem := make([]byte, 31)
		stem[0] = 0x55
		var kvs []kv
		for i := byte(0); i < 5; i++ {
			kvs = append(kvs, kv{makeStemKey(stem, i), makeValue(uint64(10 + i))})
		}
		// Also a C2 value
		kvs = append(kvs, kv{makeStemKey(stem, 200), makeValue(99)})
		// Anchor to force internal root
		anchor := make([]byte, 31)
		anchor[0] = 0xFF
		kvs = append(kvs, kv{makeStemKey(anchor, 0), makeValue(77)})
		scenarios = append(scenarios, scenario{
			name: "same stem C1+C2",
			typ:  0,
			initial: kvs,
		})
	}

	// 3. Ten different stems
	{
		var kvs []kv
		for i := 0; i < 10; i++ {
			stem := make([]byte, 31)
			for j := range stem { stem[j] = byte(i * 17) }
			kvs = append(kvs, kv{makeStemKey(stem, 0), makeValue(uint64(30 + i))})
		}
		scenarios = append(scenarios, scenario{
			name: "10 stems",
			typ:  0,
			initial: kvs,
		})
	}

	// 4. Leaf split — two stems share first byte (diverge at depth 1)
	{
		stemA := make([]byte, 31)
		for i := range stemA { stemA[i] = 0xAA }
		stemA[0] = 0x11; stemA[1] = 0x22
		stemB := make([]byte, 31)
		for i := range stemB { stemB[i] = 0xBB }
		stemB[0] = 0x11; stemB[1] = 0x33
		stemC := make([]byte, 31)
		for i := range stemC { stemC[i] = 0xFF }
		scenarios = append(scenarios, scenario{
			name: "split depth 1",
			typ:  0,
			initial: []kv{
				{makeStemKey(stemA, 0), makeValue(1)},
				{makeStemKey(stemB, 0), makeValue(2)},
				{makeStemKey(stemC, 0), makeValue(3)},
			},
		})
	}

	// 5. Leaf split — deeper divergence (share 3 prefix bytes)
	{
		stemA := make([]byte, 31)
		for i := range stemA { stemA[i] = 0xAA }
		stemA[0] = 0x11; stemA[1] = 0x22; stemA[2] = 0x33
		stemB := make([]byte, 31)
		for i := range stemB { stemB[i] = 0xBB }
		stemB[0] = 0x11; stemB[1] = 0x22; stemB[2] = 0x33
		stemZ := make([]byte, 31)
		scenarios = append(scenarios, scenario{
			name: "split depth 3",
			typ:  0,
			initial: []kv{
				{makeStemKey(stemZ, 0), makeValue(99)},
				{makeStemKey(stemA, 5), makeValue(0x41)},
				{makeStemKey(stemB, 10), makeValue(0x42)},
			},
		})
	}

	// 6. Triple collision — three stems share first byte
	{
		stemA := make([]byte, 31)
		for i := range stemA { stemA[i] = 0xAA }
		stemA[0] = 0x44; stemA[1] = 0x11
		stemB := make([]byte, 31)
		for i := range stemB { stemB[i] = 0xBB }
		stemB[0] = 0x44; stemB[1] = 0x22
		stemC := make([]byte, 31)
		for i := range stemC { stemC[i] = 0xCC }
		stemC[0] = 0x44; stemC[1] = 0x33
		stemZ := make([]byte, 31)
		scenarios = append(scenarios, scenario{
			name: "triple collision",
			typ:  0,
			initial: []kv{
				{makeStemKey(stemZ, 0), makeValue(0x99)},
				{makeStemKey(stemA, 0), makeValue(1)},
				{makeStemKey(stemB, 0), makeValue(2)},
				{makeStemKey(stemC, 0), makeValue(3)},
			},
		})
	}

	// 7. 100 random keys (deterministic via sha256)
	{
		var kvs []kv
		for i := uint64(0); i < 100; i++ {
			kvs = append(kvs, kv{makeKey(i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name: "100 random keys",
			typ:  0,
			initial: kvs,
		})
	}

	// 8. Keys with values in both C1 and C2 ranges across multiple stems
	{
		var kvs []kv
		for s := 0; s < 5; s++ {
			stem := make([]byte, 31)
			stem[0] = byte(s * 0x30)
			// C1 values (suffix 0-4)
			for i := byte(0); i < 5; i++ {
				kvs = append(kvs, kv{
					makeStemKey(stem, i),
					makeValue(uint64(s*100 + int(i))),
				})
			}
			// C2 values (suffix 128-132)
			for i := byte(128); i < 133; i++ {
				kvs = append(kvs, kv{
					makeStemKey(stem, i),
					makeValue(uint64(s*100 + int(i))),
				})
			}
		}
		scenarios = append(scenarios, scenario{
			name: "5 stems C1+C2 mix",
			typ:  0,
			initial: kvs,
		})
	}

	// =====================================================================
	// Multiblock scenarios — build initial, then apply blocks
	// =====================================================================

	// 9. Two blocks: create then update
	{
		stemA := make([]byte, 31)
		stemA[0] = 0x11
		stemB := make([]byte, 31)
		stemB[0] = 0x22
		initial := []kv{
			{makeStemKey(stemA, 0), makeValue(10)},
			{makeStemKey(stemB, 0), makeValue(20)},
		}
		blocks := [][]kv{
			// Block 1: update existing + add new
			{
				{makeStemKey(stemA, 0), makeValue(0xEE)}, // update
				{makeStemKey(make([]byte, 31), 3), makeValue(0xDD)}, // new stem
			},
		}
		scenarios = append(scenarios, scenario{
			name:    "update + new stem",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 10. Cross-block split: block 1 creates leaf, block 2 adds collider
	{
		stemA := make([]byte, 31)
		for i := range stemA { stemA[i] = 0xAA }
		stemA[0] = 0x55
		stemB := make([]byte, 31)
		for i := range stemB { stemB[i] = 0xBB }
		stemB[0] = 0x55
		stemZ := make([]byte, 31)
		initial := []kv{
			{makeStemKey(stemZ, 0), makeValue(0x77)},
			{makeStemKey(stemA, 0), makeValue(0x11)},
		}
		blocks := [][]kv{
			{{makeStemKey(stemB, 0), makeValue(0x22)}}, // collides with A
		}
		scenarios = append(scenarios, scenario{
			name:    "cross-block split",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 11. Five blocks of growing state (10 new keys per block)
	{
		var initial []kv
		for i := uint64(0); i < 5; i++ {
			initial = append(initial, kv{makeKey(i), makeValue(i)})
		}
		var blocks [][]kv
		for b := 0; b < 5; b++ {
			var ops []kv
			for j := 0; j < 10; j++ {
				idx := uint64(100 + b*10 + j)
				ops = append(ops, kv{makeKey(idx), makeValue(idx)})
			}
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "5 blocks growing",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 12. Mixed: updates + new keys per block
	{
		var initial []kv
		for i := uint64(0); i < 20; i++ {
			initial = append(initial, kv{makeKey(i), makeValue(i)})
		}
		var blocks [][]kv
		nextID := uint64(200)
		for b := 0; b < 5; b++ {
			var ops []kv
			// 3 new keys
			for j := 0; j < 3; j++ {
				ops = append(ops, kv{makeKey(nextID), makeValue(nextID)})
				nextID++
			}
			// 2 updates to existing
			ops = append(ops, kv{makeKey(uint64(b * 2)), makeValue(nextID)})
			nextID++
			ops = append(ops, kv{makeKey(uint64(b*2 + 1)), makeValue(nextID)})
			nextID++
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "5 blocks mixed",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 13. Scale: 500 initial keys, 10 blocks of 50 ops each
	{
		var initial []kv
		for i := uint64(0); i < 500; i++ {
			initial = append(initial, kv{makeKey(i), makeValue(i)})
		}
		var blocks [][]kv
		nextID := uint64(1000)
		for b := 0; b < 10; b++ {
			var ops []kv
			// 30 new keys
			for j := 0; j < 30; j++ {
				ops = append(ops, kv{makeKey(nextID), makeValue(nextID)})
				nextID++
			}
			// 20 updates
			for j := 0; j < 20; j++ {
				idx := uint64(b*20 + j)
				ops = append(ops, kv{makeKey(idx), makeValue(nextID)})
				nextID++
			}
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "500 keys + 10 blocks",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 14. Large build: 2000 random keys
	{
		var kvs []kv
		for i := uint64(0); i < 2000; i++ {
			kvs = append(kvs, kv{makeKey(5000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name: "2000 random keys",
			typ:  0,
			initial: kvs,
		})
	}

	// 15. Large build: 3000 random keys
	{
		var kvs []kv
		for i := uint64(0); i < 3000; i++ {
			kvs = append(kvs, kv{makeKey(10000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name: "3000 random keys",
			typ:  0,
			initial: kvs,
		})
	}

	// 16. Dense stem: 256 suffixes on one stem (full C1 + full C2)
	{
		stem := make([]byte, 31)
		stem[0] = 0xDE; stem[1] = 0xAD
		var kvs []kv
		for i := 0; i < 256; i++ {
			kvs = append(kvs, kv{makeStemKey(stem, byte(i)), makeValue(uint64(i + 1))})
		}
		// Anchor
		anchor := make([]byte, 31)
		anchor[0] = 0x01
		kvs = append(kvs, kv{makeStemKey(anchor, 0), makeValue(0xFF)})
		scenarios = append(scenarios, scenario{
			name: "full stem 256 suffixes",
			typ:  0,
			initial: kvs,
		})
	}

	// 17. Many splits: 50 stems sharing first 2 bytes (forced deep splitting)
	{
		var kvs []kv
		for i := 0; i < 50; i++ {
			stem := make([]byte, 31)
			stem[0] = 0x77; stem[1] = 0x88
			stem[2] = byte(i)
			kvs = append(kvs, kv{makeStemKey(stem, 0), makeValue(uint64(i))})
		}
		// Anchor
		anchor := make([]byte, 31)
		kvs = append(kvs, kv{makeStemKey(anchor, 0), makeValue(0xAA)})
		scenarios = append(scenarios, scenario{
			name: "50 stems shared prefix",
			typ:  0,
			initial: kvs,
		})
	}

	// 18. Large multiblock: 1000 initial + 10 blocks of 50 ops
	{
		var initial []kv
		for i := uint64(0); i < 1000; i++ {
			initial = append(initial, kv{makeKey(20000 + i), makeValue(i)})
		}
		var blocks [][]kv
		nextID := uint64(50000)
		for b := 0; b < 10; b++ {
			var ops []kv
			// 30 new keys
			for j := 0; j < 30; j++ {
				ops = append(ops, kv{makeKey(nextID), makeValue(nextID)})
				nextID++
			}
			// 20 updates to existing
			for j := 0; j < 20; j++ {
				idx := uint64(20000 + uint64(b*20+j))
				ops = append(ops, kv{makeKey(idx), makeValue(nextID)})
				nextID++
			}
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "1000 keys + 10 blocks",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 19. Cross-block multi-split: stems that collide across different blocks
	{
		base := make([]byte, 31)
		base[0] = 0xCC; base[1] = 0xDD
		anchor := make([]byte, 31)
		anchor[0] = 0x01
		initial := []kv{
			{makeStemKey(anchor, 0), makeValue(0xFF)},
		}
		// Add first stem
		stemA := make([]byte, 31)
		copy(stemA, base)
		stemA[2] = 0x11
		initial = append(initial, kv{makeStemKey(stemA, 0), makeValue(1)})

		var blocks [][]kv
		// Each block adds a new colliding stem
		for b := 0; b < 10; b++ {
			stem := make([]byte, 31)
			copy(stem, base)
			stem[2] = byte(0x20 + b)
			blocks = append(blocks, []kv{
				{makeStemKey(stem, 0), makeValue(uint64(10 + b))},
			})
		}
		scenarios = append(scenarios, scenario{
			name:    "cross-block multi-split",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 20. Large build: 3500 random keys (stress)
	{
		var kvs []kv
		for i := uint64(0); i < 3500; i++ {
			kvs = append(kvs, kv{makeKey(100000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name: "3500 random keys",
			typ:  0,
			initial: kvs,
		})
	}

	// 21. Large build: 5000 random keys
	{
		var kvs []kv
		for i := uint64(0); i < 5000; i++ {
			kvs = append(kvs, kv{makeKey(200000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name:    "5000 random keys",
			typ:     0,
			initial: kvs,
		})
	}

	// 22. Large build: 10000 random keys
	{
		var kvs []kv
		for i := uint64(0); i < 10000; i++ {
			kvs = append(kvs, kv{makeKey(300000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name:    "10000 random keys",
			typ:     0,
			initial: kvs,
		})
	}

	// 23. Large multiblock: 5000 initial + 20 blocks of 100 ops
	{
		var initial []kv
		for i := uint64(0); i < 5000; i++ {
			initial = append(initial, kv{makeKey(400000 + i), makeValue(i)})
		}
		var blocks [][]kv
		nextID := uint64(500000)
		for b := 0; b < 20; b++ {
			var ops []kv
			// 60 new keys
			for j := 0; j < 60; j++ {
				ops = append(ops, kv{makeKey(nextID), makeValue(nextID)})
				nextID++
			}
			// 40 updates to existing
			for j := 0; j < 40; j++ {
				idx := uint64(400000 + uint64(b*40+j))
				ops = append(ops, kv{makeKey(idx), makeValue(nextID)})
				nextID++
			}
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "5000 keys + 20 blocks",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// 24. Large build: 20000 random keys
	{
		var kvs []kv
		for i := uint64(0); i < 20000; i++ {
			kvs = append(kvs, kv{makeKey(600000 + i), makeValue(i)})
		}
		scenarios = append(scenarios, scenario{
			name:    "20000 random keys",
			typ:     0,
			initial: kvs,
		})
	}

	// 25. Dense stems: 200 stems × 2 suffixes each (verkle account pattern)
	{
		var kvs []kv
		for i := 0; i < 200; i++ {
			stem := makeKey(uint64(700000 + i))[:31]
			kvs = append(kvs, kv{makeStemKey(stem, 0), makeValue(uint64(i*2))})
			kvs = append(kvs, kv{makeStemKey(stem, 1), makeValue(uint64(i*2 + 1))})
		}
		scenarios = append(scenarios, scenario{
			name:    "200 stems x2 suffixes",
			typ:     0,
			initial: kvs,
		})
	}

	// 26. Large multiblock: 10000 initial + 10 blocks of 200 ops
	{
		var initial []kv
		for i := uint64(0); i < 10000; i++ {
			initial = append(initial, kv{makeKey(800000 + i), makeValue(i)})
		}
		var blocks [][]kv
		nextID := uint64(900000)
		for b := 0; b < 10; b++ {
			var ops []kv
			// 120 new keys
			for j := 0; j < 120; j++ {
				ops = append(ops, kv{makeKey(nextID), makeValue(nextID)})
				nextID++
			}
			// 80 updates
			for j := 0; j < 80; j++ {
				idx := uint64(800000 + uint64(b*80+j))
				ops = append(ops, kv{makeKey(idx), makeValue(nextID)})
				nextID++
			}
			blocks = append(blocks, ops)
		}
		scenarios = append(scenarios, scenario{
			name:    "10000 keys + 10 blocks",
			typ:     1,
			initial: initial,
			blocks:  blocks,
		})
	}

	// =====================================================================
	// Write binary file
	// =====================================================================

	outpath := filepath.Join(filepath.Dir(os.Args[0]), "verkle_vectors.bin")
	// Try to write next to the source file instead
	if _, err := os.Stat("main.go"); err == nil {
		outpath = "verkle_vectors.bin"
	}
	// Allow override via argument
	if len(os.Args) > 1 {
		outpath = os.Args[1]
	}

	f, err := os.Create(outpath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fmt.Printf("Generating %d Verkle test vectors...\n\n", len(scenarios))
	writeU32(f, uint32(len(scenarios)))

	for _, s := range scenarios {
		if s.typ == 0 {
			writeBuildScenario(f, s)
		} else {
			writeMultiblockScenario(f, s)
		}
	}

	fi, _ := f.Stat()
	fmt.Printf("\nWrote %d scenarios to %s (%d bytes)\n", len(scenarios), outpath, fi.Size())
}
