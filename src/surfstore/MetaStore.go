package surfstore

import (
	"errors"
	"log"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

// Retrieves the server's FileInfoMap
func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	log.Println("GetFileInfoMap start")

	*serverFileInfoMap = m.FileMetaMap

	log.Println("GetFileInfoMap end")
	return nil
	// panic("todo")
}

// Update a file's fileinfo entry
func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	//log.Println("UpdateFile start")
	filename := fileMetaData.Filename
	if _, ok := m.FileMetaMap[filename]; ok {
		if fileMetaData.Version <= m.FileMetaMap[filename].Version {
			return errors.New("Update to Server Fail, not the lastest version")
		}else if fileMetaData.Version == m.FileMetaMap[filename].Version + 1{
			m.FileMetaMap[filename] = *fileMetaData
			return nil
		}else {
			return errors.New("Local File Version cannot be larger server version than 2!")
		}
		
	}
	// log.Println(*fileMetaData)
	m.FileMetaMap[filename] = *fileMetaData
	//log.Println("UpdateFile end")
	
	return nil
}

var _ MetaStoreInterface = new(MetaStore)
