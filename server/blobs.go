package server

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// MaxBlobBytes bounds one blob that a worker writes.
const MaxBlobBytes = 4 << 30

// BlobAnswer is what a blob write answers: schemas/blob.schema.json.
type BlobAnswer struct {
	SchemaVersion int    `json:"schema_version"`
	SHA256        string `json:"sha256"`
	MD5           string `json:"md5"`
	SizeBytes     int64  `json:"size_bytes"`
	MediaType     string `json:"media_type"`
	CreatedTS     string `json:"created_ts"`
}

// UnpairedAnswer is what GET /api/v1/blobs/unpaired answers:
// schemas/blobs-unpaired.schema.json.
type UnpairedAnswer struct {
	SchemaVersion int            `json:"schema_version"`
	Files         int            `json:"files"`
	Rows          int            `json:"rows"`
	Unrowed       []UnpairedFile `json:"unrowed"`
	Missing       []BlobAnswer   `json:"missing"`
	Leftovers     []UnpairedFile `json:"leftovers"`
}

// UnpairedFile is one file in the volume that no row pairs with.
type UnpairedFile struct {
	SHA256     *string `json:"sha256"`
	Path       *string `json:"path"`
	SizeBytes  int64   `json:"size_bytes"`
	ModifiedTS string  `json:"modified_ts"`
}

// BlobRoutes are the routes for a worker to read and write bytes through, and
// the report for a person of where the volume and the blob rows disagree.
func BlobRoutes(l *landing.Lander, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := blobHandlers{lander: l, log: log}
	return func(r *Router) {
		r.Handle("GET /api/v1/blobs/{sha256}", ScopeTasksClaim, h.read)
		r.Handle("PUT /api/v1/blobs/{sha256}", ScopeTasksClaim, h.write)
		r.Handle("GET /api/v1/blobs/unpaired", "", h.unpaired)
	}
}

type blobHandlers struct {
	lander *landing.Lander
	log    *slog.Logger
}

// read serves the bytes with their media type, and answers a range or a HEAD
// as http.ServeContent does. The ETag and Repr-Digest carry the SHA-256.
func (h blobHandlers) read(w http.ResponseWriter, r *http.Request) {
	sum := r.PathValue("sha256")
	if err := landing.CheckSHA256(sum); err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBlob, "%v", err)
		return
	}
	f, blob, err := h.lander.Open(r.Context(), sum)
	switch {
	case errors.Is(err, store.ErrNoBlob):
		Refuse(w, http.StatusNotFound, CodeBlobNotFound, "no blob %s", sum)
		return
	case errors.Is(err, landing.ErrBytesMissing):
		h.log.ErrorContext(r.Context(), "a blob row's bytes are missing", "sha256", sum)
		Refuse(w, http.StatusNotFound, CodeBlobBytesMissing, "blob %s has a row, and its bytes are missing", sum)
		return
	case err != nil:
		h.internal(w, r, "read a blob", err)
		return
	}
	defer func() { _ = f.Close() }()
	raw, _ := hex.DecodeString(sum)
	w.Header().Set("Content-Type", blob.MediaType)
	w.Header().Set("ETag", `"`+sum+`"`)
	w.Header().Set("Repr-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(raw)+":")
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	created, _ := time.Parse(store.StampLayout, blob.CreatedTS)
	http.ServeContent(w, r, "", created, f)
}

// write keeps the body under the SHA-256 the path names, with the
// Content-Type as its media type. The same bytes written again answer 200 with
// the row already held, so a retry is safe.
func (h blobHandlers) write(w http.ResponseWriter, r *http.Request) {
	sum := r.PathValue("sha256")
	if err := landing.CheckSHA256(sum); err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBlob, "%v", err)
		return
	}
	mediaType := "application/octet-stream"
	if raw := r.Header.Get("Content-Type"); raw != "" {
		parsed, params, err := mime.ParseMediaType(raw)
		if err != nil {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the Content-Type %q is not a media type: %v", raw, err)
			return
		}
		mediaType = mime.FormatMediaType(parsed, params)
	}
	blob, added, err := h.lander.Keep(r.Context(), http.MaxBytesReader(w, r.Body, MaxBlobBytes), sum, mediaType)
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		Refuse(w, http.StatusRequestEntityTooLarge, CodeInvalidBody, "a blob holds at most %d bytes", int64(MaxBlobBytes))
		return
	case errors.Is(err, landing.ErrDigestMismatch):
		Refuse(w, http.StatusUnprocessableEntity, CodeDigestMismatch, "%v", err)
		return
	case err != nil:
		h.internal(w, r, "write a blob", err)
		return
	}
	status := http.StatusOK
	if added {
		status = http.StatusCreated
		h.log.Info("kept a blob", "sha256", sum, "size_bytes", blob.SizeBytes, "media_type", blob.MediaType)
	}
	JSON(w, status, blobAnswer(blob))
}

func (h blobHandlers) unpaired(w http.ResponseWriter, r *http.Request) {
	u, err := h.lander.Unpaired(r.Context())
	if err != nil {
		h.internal(w, r, "compare the blob volume with its rows", err)
		return
	}
	answer := UnpairedAnswer{
		SchemaVersion: 1, Files: u.Files, Rows: u.Rows,
		Unrowed: filesOf(u.Unrowed), Missing: []BlobAnswer{}, Leftovers: filesOf(u.Leftovers),
	}
	for _, b := range u.Missing {
		answer.Missing = append(answer.Missing, blobAnswer(b))
	}
	JSON(w, http.StatusOK, answer)
}

func (h blobHandlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.log.ErrorContext(r.Context(), what, "error", err, "path", r.URL.Path)
	Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not %s", what)
}

func blobAnswer(b store.Blob) BlobAnswer {
	return BlobAnswer{SchemaVersion: 1, SHA256: b.SHA256, MD5: b.MD5, SizeBytes: b.SizeBytes, MediaType: b.MediaType, CreatedTS: b.CreatedTS}
}

func filesOf(files []landing.StoredFile) []UnpairedFile {
	out := make([]UnpairedFile, 0, len(files))
	for _, f := range files {
		u := UnpairedFile{SizeBytes: f.SizeBytes, ModifiedTS: f.ModifiedTS}
		if f.SHA256 != "" {
			u.SHA256 = &f.SHA256
		} else {
			u.Path = &f.Path
		}
		out = append(out, u)
	}
	return out
}
