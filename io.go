package slottedpage

import (
	"errors"
	"fmt"
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

// TODO - This should return the slotID which the item was saved too
func (p PageManager) WriteItemToPage(filelocation string, contents []byte) error {
	fullpath := p.fullPath(filelocation)

	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		if err := p.createEmptyPage(fullpath); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return err
	}
	return writeItemToPage(f, contents)
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
