package slottedpage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Page struct {
	CountItems int
	Items      [][]byte
	RawPage    []byte
}

func NewSlottedPage(items [][]byte) ([]byte, error) {
	diskUsage := calculatePageSize(items)
	// TODO - If a single item is very large we should store in file blob
	// TODO - The page would then simply store a pointer to the file blob to be loaded
	if diskUsage > 4000 {
		return nil, errors.New("items would overflow page size")
	}
	headerBytes, offsets, err := buildHeaders(items)
	if err != nil {
		return nil, err
	}
	page := make([]byte, 4000)
	writeBytesAt(page, 0, headerBytes)
	for idx, item := range items {
		writeBytesAt(page, offsets[idx], item)
	}
	return page, nil
}

type slotOffset struct {
	offset uint32
	size   uint32
}

func ReadSlottedPage(page []byte) (*Page, error) {
	reader := bytes.NewReader(page)
	itemCount, err := readUint32At(reader, 0)
	if err != nil {
		return nil, err
	}
	offsets, err := readSlotInformation(reader, int(itemCount), 4)
	if err != nil {
		return nil, err
	}
	parsedPage := &Page{
		CountItems: int(itemCount),
		Items:      make([][]byte, int(itemCount)),
		RawPage:    page,
	}
	for idx, off := range offsets {
		rawBytes, err := readNBytesAt(reader, int64(off.offset), int(off.size))
		if err != nil {
			return nil, err
		}
		parsedPage.Items[idx] = rawBytes
	}
	return parsedPage, nil
}

func readNBytesAt(reader *bytes.Reader, offset int64, size int) ([]byte, error) {
	store := make([]byte, size)
	_, err := reader.ReadAt(store, offset)
	return store, err
}

func readUint32At(reader *bytes.Reader, offset int64) (uint32, error) {
	data, err := readNBytesAt(reader, offset, 4)
	if err != nil {
		return 0, err
	}
	var result uint32
	dataReader := bytes.NewReader(data)
	err = binary.Read(dataReader, binary.BigEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, err
}

func readSlotInformation(page *bytes.Reader, itemCount int, startingOffset int64) ([]slotOffset, error) {
	var slotOffsets []slotOffset
	for i := 0; i < itemCount; i++ {
		loc, err := readUint32At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += 4
		size, err := readUint32At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += 4
		slotOffsets = append(slotOffsets, slotOffset{
			offset: loc,
			size:   size,
		})
	}
	return slotOffsets, nil
}

func writeBytesAt(page []byte, offsetIdx int, toWrite []byte) {
	for i := 0; i < len(toWrite); i++ {
		page[offsetIdx+i] = toWrite[i]
	}
}

func buildHeaders(items [][]byte) ([]byte, []int, error) {
	buffer := new(bytes.Buffer)
	err := binary.Write(buffer, binary.BigEndian, uint32(len(items)))
	if err != nil {
		return nil, nil, err
	}
	startingIdx := 4000
	var offsets []int
	for _, item := range items {
		size := len(item)
		offset := startingIdx - size
		err = binary.Write(buffer, binary.BigEndian, uint32(offset))
		if err != nil {
			return nil, nil, err
		}
		err = binary.Write(buffer, binary.BigEndian, uint32(size))
		if err != nil {
			return nil, nil, err
		}
		startingIdx = offset
		offsets = append(offsets, offset)
	}
	return buffer.Bytes(), offsets, nil
}

func DeleteItemAtIndex(page []byte, idx int) {
	// TODO - Given page bytes and idx delete item at idx
	return
}

func UpdateItemAtIndex(page []byte, idx int) {
	// TODO - Given page bytes and idx update item at idx
	return
}

func WriteItemToPage(page []byte, item []byte) {
	// TODO - Given some bytes - write item to existing page
	return
}

func calculatePageSize(items [][]byte) int {
	size := 4 + (8 * len(items))
	for _, item := range items {
		size += len(item)
	}
	return size
}
