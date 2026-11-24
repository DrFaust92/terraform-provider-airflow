package fwprovider

// listPageSize is the page size used when listing collections that the
// Airflow API paginates.
const listPageSize int32 = 100

// fetchAllPages calls fetch with increasing offsets until a page comes back
// shorter than listPageSize, and returns every item.
func fetchAllPages[T any](fetch func(limit, offset int32) ([]T, error)) ([]T, error) {
	var all []T
	for offset := int32(0); ; offset += listPageSize {
		page, err := fetch(listPageSize, offset)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if int32(len(page)) < listPageSize {
			return all, nil
		}
	}
}
