package slottedpage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

type PageManager struct {
	FileDirectory string
}

func (p PageManager) fullPath(file string) string {
	if p.FileDirectory == "" {
		return file
	}
	return fmt.Sprintf("%s/%s", p.FileDirectory, file)
}

func (p PageManager) ReadFromDisk(filelocation string) (*PageInformation, error) {
	fullpath := p.fullPath(filelocation)
	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	return readPageNew(f)
}

func (p PageManager) ReadSlotIDFromDisk(filelocation string, slotId int) (*PageInformation, error) {
	fullpath := p.fullPath(filelocation)
	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	return readPageAtSpecificSlot(f, slotId)
}

func (p PageManager) WriteItemToPage(filelocation string, contents []byte) (int, error) {
	fullpath := p.fullPath(filelocation)

	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		if err := p.createEmptyPage(fullpath); err != nil {
			return -1, err
		}
	}

	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return -1, err
	}
	return writeItemToPage(f, contents)
}

func (p PageManager) UpdateItem(filelocation string, slotID int, contents []byte) error {
	fullpath := p.fullPath(filelocation)

	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		return err
	}

	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return err
	}

	return updateItem(f, slotID, contents)
}

func (p PageManager) DeleteSlotIDFromPage(filelocation string, id int) error {
	fullpath := p.fullPath(filelocation)

	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		return err
	}

	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return err
	}
	if err := deleteItemAtSlotID(f, id); err != nil {
		return err
	}
	return nil
}

func (p PageManager) ReadRawBytes(filelocation string) ([]byte, error) {
	fullpath := p.fullPath(filelocation)
	return ioutil.ReadFile(fullpath)
}

func (p PageManager) CompactPage(filelocation string) error {
	fullpath := p.fullPath(filelocation)

	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		return err
	}

	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return err
	}

	if err := compactPage(f); err != nil {
		return err
	}
	return nil
}

func (p PageManager) createEmptyPage(fullpath string) error {

	f, err := os.Create(fullpath)
	if err != nil {
		return err
	}
	defer f.Close()
	emptyBytes := make([]byte, 4000)
	if _, err := f.WriteAt(emptyBytes, 0); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}
