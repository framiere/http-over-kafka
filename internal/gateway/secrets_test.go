package gateway

import (
	"testing"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
)

func TestRawQueryDropsOnlySecretsAndKeepsBytes(t *testing.T) {
	f := newSecretFilter(apispec.Secrets{Query: []string{"access_token"}})
	for in, want := range map[string]string{
		"":                                    "",
		"a=1":                                 "a=1",
		"access_token=s":                      "",
		"b=%20x&access_token=s&a=1&a=2":       "b=%20x&a=1&a=2",
		"access%5Ftoken=s&k=v":                "k=v", // encoded name is the same parameter
		"access_token&k=v":                    "k=v", // valueless
		"access_tokenx=1&x_access_token=2":    "access_tokenx=1&x_access_token=2",
		"k=%zz&access_token=s":                "k=%zz", // invalid escapes elsewhere kept as is
		"access_token=a&access_token=b&z=%2F": "z=%2F",
	} {
		if got := f.rawQuery(in); got != want {
			t.Errorf("rawQuery(%q) = %q, want %q", in, got, want)
		}
	}
}
