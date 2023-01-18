package dbs

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestSortSamplesDurations(t *testing.T) {
	tests := []struct {
		name      string
		durations []SampleDurations
		sortBy    SortBy
		want      []SampleDurations
	}{
		{
			name:   "P99",
			sortBy: SortByP99,
			durations: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
				},
			},
			want: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 80,
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
			},
		},
		{
			name:   "P95",
			sortBy: SortByP95,
			durations: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
				},
			},
			want: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 80,
				},
			},
		},
		{
			name:   "Errors",
			sortBy: SortByErrors,
			durations: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
				},
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
				},
			},
			want: []SampleDurations{
				{
					P50: 1, P90: 2, P95: 2, P99: 4, Max: 4,
					Status: map[string]float64{
						"400": 1, "404": 1,
						"500": 5, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 80,
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 5, Max: 5,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
				{
					P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
					Status: map[string]float64{
						"200": 1, "400": 1, "404": 1,
						"500": 4, "503": 3,
					},
					Count:      10,
					ErrorsPcnt: 70,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range tt.durations {
				tt.durations[i].Count, tt.durations[i].ErrorsPcnt = HttpErrosPcnt(tt.durations[i].Status)
			}
			SortSamplesDurations(tt.durations, tt.sortBy)
			assert.Equal(t, tt.want, tt.durations)
		})
	}
}

func TestDiffSamples(t *testing.T) {
	tests := []struct {
		test *TestSamples
		ref  *TestSamples
		want *TestSamplesDiff
	}{
		{
			test: &TestSamples{
				Test: Test{
					Id: 2, Ts: time.Unix(1674196902, 0).UTC(),
					Name: "carbonapi 1.5.6", Params: "USERS=2",
				},
				Samples: map[string][]SampleDurations{
					"find": {
						{
							Url: "q=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 5,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
						{
							Url: "q=b.*", P50: 2, P90: 2, P95: 3, P99: 4, Max: 4,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
					},
					"render 1h": {
						{
							Url: "target=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 5,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
					},
				},
			},
			ref: &TestSamples{
				Test: Test{
					Id: 1, Ts: time.Unix(1674196900, 0).UTC(),
					Name: "carbonapi 1.1.2", Params: "USERS=2",
				},
				Samples: map[string][]SampleDurations{
					"find": {
						{
							Url: "q=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 4,
							Status: map[string]float64{"200": 8, "400": 2},
							Count:  10, ErrorsPcnt: 0.0,
						},
					},
					"render 1d": {
						{
							Url: "target=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 5,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
					},
				},
			},
			want: &TestSamplesDiff{
				Test: Test{
					Id: 2, Ts: time.Unix(1674196902, 0).UTC(),
					Name: "carbonapi 1.5.6", Params: "USERS=2",
				},
				Reference: Test{
					Id: 1, Ts: time.Unix(1674196900, 0).UTC(),
					Name: "carbonapi 1.1.2", Params: "USERS=2",
				},
				Samples: map[string][]SampleDurationsDiff{
					"find": {
						{
							Url: "q=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 5,
							Status: map[string]float64{"200": 9, "400": 0, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
							// diff
							MaxDiff: 1, ErrorsPcntDiff: 10.0,
							StatusDiff: map[string]float64{"200": 1, "400": -2, "504": 1},
						},
						{
							Url: "q=b.*", P50: 2, P90: 2, P95: 3, P99: 4, Max: 4,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
					},
					"render 1h": {
						{
							Url: "target=a.*", P50: 1, P90: 2, P95: 3, P99: 4, Max: 5,
							Status: map[string]float64{"200": 9, "504": 1},
							Count:  10, ErrorsPcnt: 10.0,
						},
					},
				},
			},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if got := DiffSamples(tt.test, tt.ref); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DiffSamples() = %s", cmp.Diff(tt.want, got))
			}
		})
	}
}
