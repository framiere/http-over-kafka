// Command keygen prints a fresh Ed25519 key pair for one role in the env
// format read by identity.SignerFromEnv and identity.TrustedKeysFromEnv.
// The first line goes only to processes of that role; the second to the
// processes that read its messages.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/sderosiaux/http-over-kafka/internal/identity"
)

func main() {
	role := flag.String("role", "", "gateway (signs commands) or bridge (signs responses and results)")
	kid := flag.String("kid", "", "key id, used for rotation (default: <role>-1)")
	flag.Parse()
	r := identity.Role(*role)
	if *kid == "" {
		*kid = *role + "-1"
	}
	signing, trusted, err := identity.Generate(*kid)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := identity.ParseTrustedKeys(r, trusted); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s=%s\n%s=%s\n", identity.SigningKeyEnv(r), signing, identity.TrustedKeysEnv(r), trusted)
}
