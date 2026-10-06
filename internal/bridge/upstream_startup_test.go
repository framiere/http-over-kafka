package bridge

import (
	"strings"
	"testing"
	"time"
)

func TestUpstreamConfigurationErrorsOmitCredentials(t *testing.T) {
	for _, base := range []string{
		"http://startup-user:startup-secret@127.0.0.1/?access_token=query-secret",
		"http://startup-user:startup-secret@127.0.0.1/#fragment-secret",
		"http://startup-user:startup-secret@127.0.0.1/\n",
		"http://startup-user:startup-secret@127.0.0.1/%zz",
		"ftp://startup-user:startup-secret@127.0.0.1/",
	} {
		_, err := newUpstream(base, time.Second)
		if err == nil {
			t.Fatal("invalid upstream accepted")
		}
		for _, secret := range []string{"startup-user", "startup-secret", "query-secret", "fragment-secret", "127.0.0.1"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("configuration error exposes URL data: %v", err)
			}
		}
		if !strings.Contains(err.Error(), "upstream URL") {
			t.Errorf("configuration error lost its field: %v", err)
		}
	}
}
