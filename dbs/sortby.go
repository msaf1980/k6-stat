package dbs

import (
	"strings"
)

// ErrorInvalidSortBy represents an sortBy wrapped error
type ErrorInvalidSortBy struct {
	Value string
}

func (e ErrorInvalidSortBy) Error() string {
	return e.Value + " not a sortBy key"
}

type SortBy uint8

const (
	SortByMax SortBy = iota
	SortByP99
	SortByP95
	SortByP90
	SortByP50
	SortByErrors
	SortByCount
)

var (
	sortByStrings []string = []string{"max", "p99", "p95", "p90", "p50", "errors", "count"}
	sortByString  string   = "[" + strings.Join(sortByStrings, ",") + "]"
)

func SortByValues() []string {
	return sortByStrings
}

func SortByValuesString() string {
	return sortByString
}

func SortByFromString(value string) (SortBy, error) {
	switch value {
	case "max":
		return SortByMax, nil
	case "p99":
		return SortByP99, nil
	case "p95":
		return SortByP95, nil
	case "p90":
		return SortByP90, nil
	case "p50":
		return SortByP50, nil
	case "errors":
		return SortByErrors, nil
	case "count":
		return SortByCount, nil
	default:
		return SortByMax, ErrorInvalidSortBy{value}
	}
}

func (u *SortBy) String() string {
	return sortByStrings[*u]
}

type SortByValue SortBy

func NewSortByValue(val SortBy, p *SortBy) *SortByValue {
	*p = val
	return (*SortByValue)(p)
}

func NewSortByValueFromString(val string, p *SortBy) *SortByValue {
	v, err := SortByFromString(val)
	if err != nil {
		panic(err)
	}
	*p = v
	return (*SortByValue)(p)
}

func (u *SortByValue) Set(val string, _ bool) error {
	v, err := SortByFromString(val)
	if err == nil {
		*u = SortByValue(v)
	}
	return err
}

func (u *SortByValue) Reset(i interface{}) {
	v := i.(SortBy)
	*u = SortByValue(v)
}

func (*SortByValue) Type() string {
	return "sortBy"
}

func (u *SortByValue) Get() interface{} {
	return u.GetSortBy()
}

func (u *SortByValue) GetSortBy() SortBy {
	return SortBy(*u)
}

func (u *SortByValue) String() string {
	return sortByStrings[*u]
}
