package slottedpage

import "io/ioutil"

type FileSystem struct {
	// TODO - Make below methods of this and rename
	FileDirectory string
}

func ReadPageFromDisk(fileLocation string) (*Page, error) {
	bytes, err := ioutil.ReadFile(fileLocation)
	if err != nil {
		return nil, err
	}
	return ReadSlottedPage(bytes)
}

func WriteNewSlottedPage(filelocation string, items [][]byte) error {
	page, err := NewSlottedPage(items)
	if err != nil {
		return err
	}
	// TODO - As our slotted pages are 4kb - we can use an atomic system call
	return ioutil.WriteFile(filelocation, page, 0666)
}

func DeleteSlotAtIndex(filelocation string, idx int) error {
	bytes, err := ioutil.ReadFile(filelocation)
	if err != nil {
		return err
	}
	newPage, err := DeleteItemAtIndex(bytes, idx)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filelocation, newPage, 0666)
}
