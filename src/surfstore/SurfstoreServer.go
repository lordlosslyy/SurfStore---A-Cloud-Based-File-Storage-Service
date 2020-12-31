package surfstore

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
}

func (s *Server) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	e := s.MetaStore.GetFileInfoMap(succ, serverFileInfoMap)
	if e != nil {
		log.Print(e)
		return e
	}
	return nil
	// panic("todo")
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {

	e := s.MetaStore.UpdateFile(fileMetaData, latestVersion)

	if e != nil {
		log.Println("UpdateFile Error:")
		log.Println(e)
		return e
	}
	return nil
	// panic("todo")
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {

	e := s.BlockStore.GetBlock(blockHash, blockData)
	if e != nil {
		log.Println("GetBlock Error")
		log.Print(e)
		return e
	}
	return nil
	// panic("todo")
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {

	e := s.BlockStore.PutBlock(blockData, succ)
	if e != nil {
		log.Println("PutBlock Error")
		log.Print(e)
		return e
	}
	return nil
	// panic("todo")
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {

	e := s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
	if e != nil {
		log.Println("HasBlock Error")
		log.Print(e)
		return e
	}
	return nil
	// panic("todo")
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}

	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
	}
}

// accept rpc request from clients
func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {
	// panic("todo")
	// panic("todo")
	rpc.Register(&surfstoreServer)
	rpc.HandleHTTP()
	// add for loop for multiple clients
	l, e := net.Listen("tcp", hostAddr)
	if e != nil {
		//log.Fatal("Listen error:", e)
	}
	err := http.Serve(l, nil)

	if err!= nil{
		log.Println(err)
		return err
	}
	for{

	}

	//fmt.Print("Press enter key to end server")
	//fmt.Scanln()
	//return nil
}
