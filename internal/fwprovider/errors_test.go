package fwprovider

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestProblemDetail(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"rfc7807 detail", `{"detail":"Pool already exists","title":"Conflict","status":409}`, "Pool already exists"},
		{"unauthenticated 401", `{"detail":"Not authenticated","status":401,"title":"Unauthorized"}`, "Not authenticated"},
		{"permission denied 403", `{"detail":"The user does not have permission to perform this action","status":403,"title":"Forbidden"}`, "The user does not have permission to perform this action"},
		{"title only", `{"title":"Bad Request","status":400}`, "Bad Request"},
		{"non-json body", "boom, not json", "boom, not json"},
		{"empty", "", ""},
		{"whitespace only", "  \n\t ", ""},
	}
	for _, c := range cases {
		if got := problemDetail([]byte(c.body)); got != c.want {
			t.Errorf("%s: problemDetail(%q) = %q, want %q", c.name, c.body, got, c.want)
		}
	}
}

func TestApiErrorDetailNonAPIError(t *testing.T) {
	if got := apiErrorDetail(errors.New("plain error")); got != "" {
		t.Errorf("apiErrorDetail(non-API error) = %q, want empty", got)
	}
}

func TestClientError(t *testing.T) {
	got := clientError("create", "my-pool", &http.Response{Status: "409 Conflict"}, errors.New("boom"))
	for _, want := range []string{"create", `"my-pool"`, "409 Conflict", "boom"} {
		if !strings.Contains(got, want) {
			t.Errorf("clientError() = %q, missing %q", got, want)
		}
	}

	if got := clientError("read", "x", nil, nil); got != `failed to read "x"` {
		t.Errorf("clientError(no resp/err) = %q", got)
	}
}
