package slottedpage

import "io/ioutil"

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
