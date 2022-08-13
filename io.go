package slottedpage

import (
	"fmt"
	"io/ioutil"
)

type PageManager struct {
	// TODO - Make below methods of this and rename
	FileDirectory string
}

func (p PageManager) fullPath(file string) string {
	if p.FileDirectory == "" {
		return file
	}
	return fmt.Sprintf("%s/%s", p.FileDirectory, file)
}

func (p PageManager) ReadPageFromDisk(fileLocation string) (*Page, error) {
	bytes, err := ioutil.ReadFile(p.fullPath(fileLocation))
	if err != nil {
		return nil, err
	}
	return ReadSlottedPage(bytes)
}

func (p PageManager) WriteNewSlottedPage(filelocation string, items [][]byte) error {
	page, err := NewSlottedPage(items)
	if err != nil {
		return err
	}
	// TODO - As our slotted pages are 4kb - we can use an atomic system call
	return ioutil.WriteFile(p.fullPath(filelocation), page, 0666)
}

func (p PageManager) DeleteSlotByID(filelocation string, idx int) error {
	bytes, err := ioutil.ReadFile(p.fullPath(filelocation))
	if err != nil {
		return err
	}
	newPage, err := DeleteSlotItemByID(bytes, idx)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(p.fullPath(filelocation), newPage, 0666)
}
