package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]Block
}

// Get a block based on its hash
func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	// log.Println(blockHash)
	block, ok := bs.BlockMap[blockHash]
	if ok {
		*blockData = block
		return nil
	}
	//log.Println("no exist")
	return errors.New("blockHash not exist")
	// panic("todo")
}

// Put a block
func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	sum := sha256.Sum256(block.BlockData)
	// fmt.Printf("%x", sum)
	//log.Println(block.BlockSize)
	sha256Str := hex.EncodeToString(sum[:])
	bs.BlockMap[string(sha256Str)] = block
	*succ = true
	return nil
	// panic("todo")
}

// Check if certain blocks are alredy present on the server
func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {

	for i := 0; i < len(blockHashesIn); i++ {
		_, ok := bs.BlockMap[blockHashesIn[i]]
		if ok {
			*blockHashesOut = append(*blockHashesOut, blockHashesIn[i])
		}
	}
	return nil
	// panic("todo")
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
