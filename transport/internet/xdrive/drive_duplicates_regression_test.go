package xdrive

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
)

// A retried create can leave two objects under one name; Delete must remove
// every one of them, whether the name came from the cache or from a listing.
func TestDeleteRemovesEveryDuplicate(t *testing.T) {
	for _, cached := range []bool{false, true} {
		var mu sync.Mutex
		var deleted []string
		st := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodDelete {
				mu.Lock()
				deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/files/"))
				mu.Unlock()
				return regressionResponse(http.StatusNoContent, ""), nil
			}
			q := r.URL.Query().Get("q")
			switch {
			case strings.Contains(q, " or "):
				return regressionResponse(http.StatusOK, `{"files":[{"id":"a1","name":"sessions~a"},{"id":"a2","name":"sessions~a"},{"id":"c1","name":"sessions~a~c"},{"id":"c2","name":"sessions~a~c"}]}`), nil
			case strings.Contains(q, "name = 'sessions~a'"):
				return regressionResponse(http.StatusOK, `{"files":[{"id":"a1","name":"sessions~a"},{"id":"a2","name":"sessions~a"}]}`), nil
			case strings.Contains(q, "name contains 'sessions~a~'"):
				return regressionResponse(http.StatusOK, `{"files":[{"id":"c1","name":"sessions~a~c"},{"id":"c2","name":"sessions~a~c"}]}`), nil
			}
			return regressionResponse(http.StatusOK, `{"files":[]}`), nil
		}))
		if cached {
			if _, err := st.query(context.Background(), "name = 'sessions~a'"); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Delete(context.Background(), "sessions/a"); err != nil {
			t.Fatal(err)
		}
		slices.Sort(deleted)
		if want := []string{"a1", "a2", "c1", "c2"}; !slices.Equal(deleted, want) {
			t.Errorf("cached=%v deleted %v, want %v", cached, deleted, want)
		}
	}
}
