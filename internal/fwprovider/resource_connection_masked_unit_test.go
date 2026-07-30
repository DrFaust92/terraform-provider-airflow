package fwprovider

import (
	"encoding/json"
	"strings"
	"testing"
)

// airflowSensitiveKeywords mirrors Airflow's DEFAULT_SENSITIVE_FIELDS used by
// SecretsMasker.should_hide_value_for_key: a key is masked when its lowercased
// form contains any of these as a substring.
var airflowSensitiveKeywords = []string{
	"access_token", "api_key", "apikey", "authorization",
	"passphrase", "passwd", "password", "private_key", "secret", "token",
}

func airflowShouldHide(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	for _, s := range airflowSensitiveKeywords {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// maskLikeAirflow simulates what Airflow returns for a connection `extra`: it
// recurses the decoded JSON and replaces every string value whose *key* is
// sensitive with "***" — including empty-string values, exactly as Airflow's
// masker does. It returns the re-serialized JSON.
func maskLikeAirflow(t *testing.T, extra string) string {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal([]byte(extra), &v); err != nil {
		t.Fatalf("test fixture is not valid JSON: %v", err)
	}
	masked := maskValue(v, "")
	out, err := json.Marshal(masked)
	if err != nil {
		t.Fatalf("re-marshal failed: %v", err)
	}
	return string(out)
}

func maskValue(item interface{}, key string) interface{} {
	switch v := item.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, val := range v {
			out[k] = maskValue(val, k)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, val := range v {
			out[i] = maskValue(val, key)
		}
		return out
	case string:
		if key != "" && airflowShouldHide(key) {
			return "***"
		}
		return v
	default:
		return v
	}
}

// TestJSONEqualIgnoringMasked_Issue98 reproduces DrFaust92/terraform-provider-airflow#98
// (reopen of #83/#96): a connection `extra` where a sensitive key holds an
// EMPTY-string value. Airflow masks it to "***" on read; the provider must
// still treat the API value as equal to the configured state, otherwise it
// writes the masked blob into state and Terraform reports
// "Provider produced inconsistent result after apply: .extra: inconsistent
// values for sensitive attribute".
func TestJSONEqualIgnoringMasked_Issue98(t *testing.T) {
	// Exact payload from the issue report.
	state := `{"headers":{"x-libertify-api-key":"blablablabla"},"token_type":"x-test-api-key","token_prefix":""}`
	api := maskLikeAirflow(t, state)

	// Sanity: the masking must have hidden the token_* keys (token_prefix is the
	// empty-value trap) while leaving the hyphenated header key untouched.
	if !strings.Contains(api, `"token_type":"***"`) || !strings.Contains(api, `"token_prefix":"***"`) {
		t.Fatalf("fixture did not mask as expected: %s", api)
	}
	if !strings.Contains(api, `"x-libertify-api-key":"blablablabla"`) {
		t.Fatalf("fixture masked the header key it should not have: %s", api)
	}

	if !jsonEqualIgnoringMasked(state, api) {
		t.Fatalf("jsonEqualIgnoringMasked = false; masked API extra %s\nis not recognised as equal to state %s\n=> provider writes the masked blob to state and Terraform errors with 'inconsistent values for sensitive attribute'", api, state)
	}
}

// TestUnmaskAgainst_EmptyStateValue isolates the root cause: a masked leaf whose
// corresponding state value is the empty string must be restored to "".
func TestUnmaskAgainst_EmptyStateValue(t *testing.T) {
	api := map[string]interface{}{"token_prefix": "***"}
	state := map[string]interface{}{"token_prefix": ""}

	got := unmaskAgainst(api, state).(map[string]interface{})
	if got["token_prefix"] != "" {
		t.Fatalf("unmaskAgainst kept %q for an empty-string state value; want \"\"", got["token_prefix"])
	}
}
