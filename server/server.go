// Package server is the HTTP API that `ingestion serve` answers. Every route
// but the probes and the RFC 9728 metadata answers only a request carrying a
// bearer token the identity provider signed. A person's token reaches every
// route. A service's token reaches only the routes its scopes list.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/datapointchris/goapiauth"
	"github.com/datapointchris/goapiauth/clientid"

	"github.com/datapointchris/ingestion/store"
)

// Product names the client id prefixes the API accepts: ingestion-cli- for a
// person's device login, ingestion-svc- for a service.
const Product = "ingestion"

// Router is where a package registers a handler for each of its rows in the
// route table.
type Router struct {
	mux    *http.ServeMux
	scopes map[string][]string
}

// Routes registers one package's routes.
type Routes func(*Router)

// TokenGate verifies a bearer token, and says why it cannot yet.
type TokenGate interface {
	goapiauth.TokenVerifier
	Ready() error
}

// Config is what the API serves from.
type Config struct {
	Store *store.Store
	Gate  TokenGate
	// Issuer is the identity provider whose tokens the API accepts.
	Issuer string
	// PublicURL is where callers reach the API. Where it is set, the
	// API serves RFC 9728 metadata there, from which `ingestion auth login`
	// learns the issuer.
	PublicURL string
	Version   string
	Log       *slog.Logger
}

// GateConfig is the token check for issuer, accepting this product's person
// and service clients.
func GateConfig(issuer string) goapiauth.Config {
	return goapiauth.Config{
		Issuer:        issuer,
		PersonPrefix:  clientid.PersonPrefix(Product),
		ServicePrefix: clientid.ServicePrefix(Product),
	}
}

// Handler is every route: the probes, which answer with no token so a kubelet
// can call them, the metadata, and each of routes behind the token check.
func Handler(cfg Config, routes ...Routes) http.Handler {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	r := &Router{mux: http.NewServeMux(), scopes: map[string][]string{}}
	r.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"status": "ok", "version": cfg.Version})
	})
	r.mux.HandleFunc("GET /ready", func(w http.ResponseWriter, req *http.Request) {
		if err := ready(req.Context(), cfg); err != nil {
			JSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting", "detail": err.Error(), "version": cfg.Version})
			return
		}
		JSON(w, http.StatusOK, map[string]string{"status": "ready", "version": cfg.Version})
	})
	if cfg.PublicURL != "" {
		r.mux.Handle("GET "+goapiauth.ProtectedResourcePath, goapiauth.ProtectedResourceMetadata{Resource: cfg.PublicURL, Issuer: cfg.Issuer})
	} else {
		log.Warn("no public_url is configured, so no RFC 9728 metadata is served and `ingestion auth login <url>` cannot discover the issuer")
	}
	r.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		Refuse(w, http.StatusNotFound, CodeRouteNotFound, "no route serves %s %s", req.Method, req.URL.Path)
	})
	// The version a caller compares its own with, behind the same token as
	// the routes it calls.
	r.Handle(RouteVersion, func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"version": cfg.Version})
	})
	for _, register := range routes {
		register(r)
	}
	return goapiauth.RequireBearer(cfg.Gate, log, "/health", "/ready")(goapiauth.LimitServices(r.mux, r.scopes, log))
}

// ready is nil once the store answers a read and the identity provider's keys
// are read, which is when a token can be verified.
func ready(ctx context.Context, cfg Config) error {
	if err := cfg.Store.Ping(ctx); err != nil {
		return fmt.Errorf("the store does not answer: %w", err)
	}
	return cfg.Gate.Ready()
}

// Refusal codes, by the status each answers with. A client branches on the
// code, and the message is for a person.
const (
	CodeInvalidBody   = "invalid_body"
	CodeInvalidQuery  = "invalid_query"
	CodeRouteNotFound = "route_not_found"
	CodeTaskNotFound  = "task_not_found"
	CodeItemNotFound  = "item_not_found"
	CodeStaleLease    = "stale_lease"
	CodeInternal      = "internal"
	CodeUnknownField  = "unknown_field"
	CodeInvalidKey    = "invalid_key"
	CodeInvalidBlob   = "invalid_blob"
	CodeBlobNotFound  = "blob_not_found"
	// CodeBlobBytesMissing is a blob whose row holds and whose bytes are gone,
	// which the unpaired report also names.
	CodeBlobBytesMissing = "blob_bytes_missing"
	CodeDigestMismatch   = "digest_mismatch"
)

// refusal is the body of every answer that refuses a request, as goapiauth's
// own refusals write it.
type refusal struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// JSON answers status with body encoded as JSON.
func JSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// Refuse answers status with a refusal carrying code and the sentence that
// format and args make.
func Refuse(w http.ResponseWriter, status int, code, format string, args ...any) {
	JSON(w, status, refusal{Error: http.StatusText(status), Message: fmt.Sprintf(format, args...), Code: code})
}
