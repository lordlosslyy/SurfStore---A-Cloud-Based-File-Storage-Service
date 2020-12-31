package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"errors"
	"strings"
	"reflect"
	"bytes"
)

// Implement the logic for a client syncing with the server here.
const filesInfoFileName = "index.txt"

// func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error
func ClientSync(client RPCClient) {
	// transverse whole directory
	files, err := ioutil.ReadDir(client.BaseDir)
	check(err)
	// check if there is index.txt
	isIndexFileExist := fileExists(client.BaseDir+"/index.txt")

	serverFileInfoMap := make(map[string]FileMetaData)
	clientFileInfoMap := make(map[string]FileMetaData)
	getFileInfo(files, client, &clientFileInfoMap)
	log.Println("client info after getFileInfo")
	PrintMetaMap(clientFileInfoMap)

	uploadList := make([]FileMetaData, 0, len(clientFileInfoMap))
	downloadList := make([]FileMetaData, 0, len(clientFileInfoMap))

	if isIndexFileExist{
		mergeIndexFile(client, &clientFileInfoMap)
	}

	ignore := true
	err = client.GetFileInfoMap(&ignore, &serverFileInfoMap)
	check(err)

	//PrintMetaMap(serverFileInfoMap)
	getUpDownloadList(&serverFileInfoMap, &clientFileInfoMap, &uploadList, &downloadList)

	log.Println("client info after getUpDownloadList")
	PrintMetaMap(clientFileInfoMap)

	e := uploadToServer(client, &serverFileInfoMap, &clientFileInfoMap, &uploadList, &downloadList)
	if e != err{
		log.Print("Upload Error ")
	}
	downloadFromServer(client, &serverFileInfoMap, &clientFileInfoMap, &downloadList)

	writeIndexFile(client, &clientFileInfoMap)
	
	
	serverFileInfoMap2 := make(map[string]FileMetaData)
	succ := true
	err = client.GetFileInfoMap(&succ, &serverFileInfoMap2)
	check(err)
	//log.Println("Updated serverFileInfoMap")
	PrintMetaMap(serverFileInfoMap2)
	
}

func writeIndexFile(client RPCClient, clientFileInfoMap *map[string]FileMetaData) {
	filePath := client.BaseDir + "/" + filesInfoFileName
	f, err := os.Create(filePath)
	check(err)
	
	for filename, fileinfo := range(*clientFileInfoMap) {
		_, err = f.Write([]byte(filename + "," + strconv.Itoa(fileinfo.Version) + ","))
		check(err)
		for i, hash := range(fileinfo.BlockHashList) {
			if i == 0 {
				_, err = f.Write([]byte(hash))
				check(err)
			} else {
				_, err = f.Write([]byte(" " + hash))
				check(err)
			} 
		}
		// possible wrong
		_, err = f.Write([]byte("\n"))
		check(err)
	}

}


func getBlocksFromLocal(client RPCClient, filename string, fileBlock *map[string]Block, hashList *[]string) {
	log.Println("getBlocksFromLocal start")
	f, err := os.Open(client.BaseDir + "/" + filename)
	check(err) 
	defer func() {
		if err = f.Close(); err != nil {
			log.Println(err)
		}
	}()
	
	s := bufio.NewReader(f)
	
	for {
		buf := make([]byte, client.BlockSize)
		log.Println(buf)
		count, err := s.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		
		sha256Str:= getSha256Value(buf[:count])
		*hashList = append(*hashList, sha256Str)
		(*fileBlock)[sha256Str] = Block{BlockData: buf[:count], BlockSize: count}
		/*if count < client.BlockSize {
			// log.Println(count)
			//tmpcount := count
			//log.Println((len(*fileBlock)-1)*client.BlockSize + tmpcount)
		}*/
	}
	log.Println("getBlocksFromLocal end")
}

func findList(element string, list []string) bool {
	if len(list) == 0{
		return false 
	}
	for _, str := range(list) {
		if element == str {
			return true
		}
	}

	return false
}

func uploadToServer(client RPCClient, serverFileInfoMap, clientFileInfoMap *map[string]FileMetaData, uploadList, downloadList *[]FileMetaData) error{
	log.Println("Upload to server Start")
	for _, uploadFile := range(*uploadList) {
		log.Println("Upload: "+ uploadFile.Filename)
		fileBlock := make(map[string]Block)
		hashList := make([]string, 0)
		getBlocksFromLocal(client, uploadFile.Filename, &fileBlock, &hashList)

		serverExistHash := make([]string, 0)
		e :=client.HasBlocks(hashList, &serverExistHash)
		check(e)
		if e!=nil{
			log.Println("HasBlocks Error")
			return e
		}
		// fmt.Printf("Before %d\n", len(hashList))
		// fmt.Printf("After %d\n", len(hashList) - len(serverExistHash))
		// Upload All Blocks To Server 
		for _, hash := range hashList {
			if findList(hash, serverExistHash) {
				continue
			}
			succ := true
			err := client.PutBlock(fileBlock[hash], &succ)
			if err != nil {
				log.Println("PutBlock Error ")
				//log.Println(succ)
				return err
			}
		}

		ver := uploadFile.Version
		err := client.UpdateFile(&uploadFile, &ver)
		check(err)
		if err != nil{
			// Someone have already upload file, go to download List
			log.Println("Upload Fail: "+ uploadFile.Filename)
			*downloadList = append(*downloadList, (*serverFileInfoMap)[uploadFile.Filename])
		}
		
	}
	log.Println("Upload to server End")
	return nil
} 

func downloadAndWriteFile(client RPCClient, filepath string, downloadFile FileMetaData){
	f, err := os.Create(filepath)
	check(err)
	for _, hash := range downloadFile.BlockHashList {
		block := new(Block)
		err = client.GetBlock(hash, block)
		check(err)
		_, err = f.Write(block.BlockData[:block.BlockSize])
		check(err)
	}
	f.Close()
}

func downloadFromServer(client RPCClient, serverFileInfoMap, clientFileInfoMap *map[string]FileMetaData, downloadList *[]FileMetaData){
	//log.Println("Download from server Start")
	for _, downloadFile := range(*downloadList){
		log.Println("Download " + downloadFile.Filename)
		filepath := client.BaseDir + "/" + downloadFile.Filename
		if len(downloadFile.BlockHashList) == 1 && downloadFile.BlockHashList[0] == "0" {
			downloadFile = (*serverFileInfoMap)[downloadFile.Filename]
			deleteFileFromLocal(client, downloadFile.Filename)
		} else {
			downloadAndWriteFile(client, filepath, downloadFile)
		}
		(*clientFileInfoMap)[downloadFile.Filename] = downloadFile
	}
	//log.Println("Download from server End")
}

func deleteFileFromLocal(client RPCClient, filename string){
	err := os.Remove(client.BaseDir+"/"+filename)
	check(err)
}

func getUpDownloadList(serverFileInfoMap, clientFileInfoMap *map[string]FileMetaData, uploadList, downloadList *[]FileMetaData) {	
	commonList := make([]string, 0, len(*clientFileInfoMap))	

	// if file exists in server, but not in client => downloadlist 
	// if file exists in both server and client => filename to commonlist for further comparison
	for filename, filedata := range(*serverFileInfoMap) {
		if _, ok := (*clientFileInfoMap)[filename]; !ok {
			*downloadList = append(*downloadList, filedata)
		} else {
			commonList = append(commonList, filename)
		}
	}

	// if file exists in client, but not in server => uploadlist 
	for filename, filedata := range(*clientFileInfoMap) {
		if _, ok := (*serverFileInfoMap)[filename]; !ok {
			*uploadList = append(*uploadList, filedata)
		}
	}

	// compare versions between server and client to determine upload or download 
	for _, filename := range(commonList) {
		if ((*serverFileInfoMap)[filename].Version >= (*clientFileInfoMap)[filename].Version) {
			if (!isFileNotChange((*serverFileInfoMap)[filename].BlockHashList, (*clientFileInfoMap)[filename].BlockHashList)){
				*downloadList = append(*downloadList, (*serverFileInfoMap)[filename])
			}
		} else {
			*uploadList = append(*uploadList, (*clientFileInfoMap)[filename])
		}
	}
}

func mergeIndexFile(client RPCClient, currDirData *map[string] FileMetaData){
	log.Println("merge index start")
	f, err := os.Open(client.BaseDir+ "/" + filesInfoFileName)
	defer func() {
		if err = f.Close(); err != nil {
			log.Println(err)
		}
	}()
	s := bufio.NewReader(f)
	var contentBuffer bytes.Buffer
	for{
		buffer := make([]byte, 256)
		currChunk, err := s.Read(buffer)
		// log.Println(string(buffer))
		if err != nil{
			if err == io.EOF{
			/// Wait For Finish File
				break
			}else{
				log.Println(err)
			}
		}
		contentBuffer.Write(buffer[:currChunk])
		for bytes.Contains(contentBuffer.Bytes(), []byte("\n")){

			index := bytes.Index(contentBuffer.Bytes(), []byte("\n"))

			currLine := string(contentBuffer.Bytes()[:index])
			contentBuffer.Next(index+1)
			//log.Print(currLine)
			elements := strings.Split(currLine, ",")
			file := elements[0]
			version, err := strconv.Atoi(elements[1])
			check(err)
			info, ok := (*currDirData)[file]
			// log.Println("CheckCheckCheck")
			if !ok{
				//delete
				//version needs to add 1 

				if !fileExists(client.BaseDir+"/"+file) && elements[2]=="0" {
					info = FileMetaData{Filename: file, Version:version, BlockHashList: []string {"0"}}
				}else{
					info = FileMetaData{Filename: file, Version:version+1, BlockHashList: []string {"0"}}
				}
			} else {

				//isFileNotChange()
				indexBlocks := strings.Split(elements[2]," ")
				if (isFileNotChange(info.BlockHashList,indexBlocks)){
					//log.Println("File Name: "+info.Filename+" Does Not Update")
					info.Version = version
				} else {
					//log.Println("File Name: "+info.Filename+" Update")
					info.Version = version+1 
				}
			}
			(*currDirData)[file] = info
		}
	}
	log.Println("merge index end")
	//log.Println("Merge With Index File Update:")
	//PrintMetaMap(*currDirData)
}

func isFileNotChange(newFile []string, oldFile []string) bool{
	return reflect.DeepEqual(newFile, oldFile)
}
func getNewHashlist(fileSize int, blockSize int) []string{
	totalBlock := fileSize/blockSize
	if fileSize % blockSize != 0{
		totalBlock++
	}
	return make([]string, 0, totalBlock)
}

func getSha256Value(data []byte) string{
	hashValue := sha256.Sum256(data)
	return hex.EncodeToString(hashValue[:])
}

func getFileInfo(currFiles []os.FileInfo, client RPCClient, nameToInfoMap *map[string]FileMetaData) error{
	//currMap := *nameToInfoMap
	//currBlockMap := *sha256ToBlockMap
	log.Println("getFileInfo start")
	for _, f := range currFiles{
		if f.Name() == ".DS_Store" ||f.Name() == filesInfoFileName{
			continue
		}
		targetFile, err:= os.Open(client.BaseDir+"/"+f.Name())

		check(err)
		reader:= bufio.NewReader(targetFile)
		
		currFileHashList := getNewHashlist(int(f.Size()), client.BlockSize) 
		//log.Println("Start to Read:" + f.Name())
		for{
			buffer := make([]byte, client.BlockSize)
			count, err := reader.Read(buffer)
			if err!= nil{
				return errors.New("Read File Error:"+f.Name())
			}
			sha256Value := getSha256Value(buffer[:count])
			//log.Println(sha256Value)
			currFileHashList = append(currFileHashList, sha256Value)
			//(*sha256ToBlockMap)[sha256Value] = Block{BlockData: buffer[:count], BlockSize: len(buffer[:count])}
			if count < client.BlockSize{
				break
			}		
		}
		(*nameToInfoMap)[f.Name()] = FileMetaData{Filename: f.Name(), Version:1, BlockHashList: currFileHashList}
		//log.Println(currFileHashList)
		targetFile.Close()
	}
	log.Println("getFileInfo end")
	return nil
}

func check(e error) {
	if e != nil {
		log.Println(e)
	}
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func writeFile(filePath string, filemetadat FileMetaData) {
	var f *os.File
	var err error
	if fileExists(filePath) == false {
		f, err = os.Create(filePath)
		check(err)
	} else {
		f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		check(err)
	}
	var hashstring string
	for index, blockHash := range filemetadat.BlockHashList {
		if index == 0 {
			hashstring += blockHash
		} else {
			hashstring += (" " + blockHash)
		}
	}

	_, err = f.Write([]byte(filemetadat.Filename + "," + strconv.Itoa(filemetadat.Version) + "," + hashstring + "\n"))
	check(err)

	f.Close()
}

/*
type FileMetaData struct {
	Filename      string
	Version       int
	BlockHashList []string
}
*/

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		/*for _, value := range filemeta.BlockHashList {
			fmt.Println(value)
		}*/
		// filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
