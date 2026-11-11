package xdrive

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestDeleteRefreshesDuplicateSet(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var mu sync.Mutex
		left := map[string]string{"a1": "sessions~a", "a2": "sessions~a", "c1": "sessions~a~c"}
		var deleted []string
		lists := 0
		failed := false
		s := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			if r.Method == http.MethodDelete {
				id := strings.TrimPrefix(r.URL.Path, "/files/")
				if fail && id == "a2" && !failed {
					failed = true
					return regressionResponse(http.StatusBadRequest, "temporary failure"), nil
				}
				if _, ok := left[id]; !ok {
					return regressionResponse(http.StatusNotFound, ""), nil
				}
				delete(left, id)
				deleted = append(deleted, id)
				return regressionResponse(http.StatusNoContent, ""), nil
			}
			lists++
			q := r.URL.Query().Get("q")
			var files []string
			page := r.URL.Query().Get("pageToken")
			for _, id := range []string{"a1", "a2", "c1"} {
				name, ok := left[id]
				if !ok || (page == "" && id != "a1") || (page != "" && id == "a1") {
					continue
				}
				if (name == "sessions~a" && strings.Contains(q, "name = 'sessions~a'")) ||
					(name != "sessions~a" && strings.Contains(q, "name contains 'sessions~a~'")) {
					files = append(files, `{"id":"`+id+`","name":"`+name+`"}`)
				}
			}
			token := ""
			if page == "" {
				token = `,"nextPageToken":"second"`
			}
			return regressionResponse(http.StatusOK, `{"files":[`+strings.Join(files, ",")+`]`+token+`}`), nil
		}))
		s.rememberID("sessions~a", "a1", nil) // populated before a2 became visible
		err := s.Delete(context.Background(), "sessions/a")
		if fail && err == nil {
			t.Error("partial failure reported success")
		}
		if fail && err != nil {
			err = s.Delete(context.Background(), "sessions/a")
		}
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(deleted)
		if len(left) != 0 || !slices.Equal(deleted, []string{"a1", "a2", "c1"}) {
			t.Errorf("remaining=%v deleted=%v", left, deleted)
		}
		wantLists := 2
		if fail {
			wantLists = 4
		}
		if lists != wantLists {
			t.Errorf("listing requests=%d, want %d", lists, wantLists)
		}
		t.Logf("partial failure=%v: listing requests=%d deletes=%v", fail, lists, deleted)
	}
}
