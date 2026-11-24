package fwprovider

import (
	"errors"
	"testing"
)

func TestFetchAllPages(t *testing.T) {
	total := int(listPageSize)*2 + 5
	var offsets []int32
	got, err := fetchAllPages(func(limit, offset int32) ([]int, error) {
		offsets = append(offsets, offset)
		var page []int
		for i := int(offset); i < total && i < int(offset+limit); i++ {
			page = append(page, i)
		}
		return page, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != total {
		t.Errorf("got %d items, want %d", len(got), total)
	}
	if want := []int32{0, listPageSize, 2 * listPageSize}; len(offsets) != len(want) || offsets[2] != want[2] {
		t.Errorf("offsets = %v, want %v", offsets, want)
	}

	if _, err := fetchAllPages(func(int32, int32) ([]int, error) { return nil, errors.New("boom") }); err == nil {
		t.Error("expected error to be returned")
	}
}
