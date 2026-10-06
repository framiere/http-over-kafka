// Command devtoken mints a caller JWT signed with the INSECURE dev IdP key, for
// calling the gateway from a laptop:
//
//	curl -H "Authorization: Bearer $(go run ./cmd/devtoken -app checkout)" ...
//
// Settings come from -env (default deploy/dev-idp.env); variables already set
// in the environment win.
package main

import (
	"bufio"
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/devidp"
)

func main() {
	app := flag.String("app", "checkout", "calling application (sub)")
	instance := flag.String("instance", "", "calling instance (default <app>-1)")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime")
	envFile := flag.String("env", "deploy/dev-idp.env", "env file with HOK_DEV_IDP_KEY, HOK_JWT_ISSUER, HOK_JWT_AUDIENCE")
	flag.Parse()
	if *instance == "" {
		*instance = *app + "-1"
	}
	env, err := readEnv(*envFile)
	if err != nil {
		log.Fatal(err)
	}
	kid, b64, ok := strings.Cut(env("HOK_DEV_IDP_KEY"), ":")
	seed, err := base64.StdEncoding.DecodeString(b64)
	if !ok || err != nil || len(seed) != ed25519.SeedSize {
		log.Fatal("HOK_DEV_IDP_KEY must be <kid>:<base64 32-byte seed>")
	}
	tok, err := devidp.Token{
		KeyID: kid, Key: ed25519.NewKeyFromSeed(seed),
		Issuer: env("HOK_JWT_ISSUER"), Audience: env("HOK_JWT_AUDIENCE"),
		Application: *app, Instance: *instance,
		IssuedAt: time.Now(), TTL: *ttl,
	}.Sign()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(tok)
}

func readEnv(path string) (func(string) string, error) {
	vals := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if k, v, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(line, "#") {
			vals[k] = v
		}
	}
	return func(k string) string {
		if v, ok := os.LookupEnv(k); ok {
			return v
		}
		return vals[k]
	}, sc.Err()
}
