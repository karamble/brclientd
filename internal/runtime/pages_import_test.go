// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// jpegBytes returns a payload http.DetectContentType sniffs as image/jpeg.
func jpegBytes(size int) []byte {
	b := make([]byte, size)
	copy(b, []byte{0xff, 0xd8, 0xff, 0xe0})
	return b
}

func newImportTestServer(t *testing.T) (*StatusServer, string) {
	t.Helper()
	dataDir := t.TempDir()
	s := &StatusServer{
		DataDir:  dataDir,
		PagesDir: filepath.Join(dataDir, "pages"),
	}
	return s, dataDir
}

func writeEmbed(t *testing.T, dataDir, uid16, name string, data []byte) {
	t.Helper()
	dir := filepath.Join(dataDir, "db", "embeds", uid16)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func doImport(t *testing.T, s *StatusServer, source, dest string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"source": source, "dest": dest})
	req := httptest.NewRequest(http.MethodPost, "/pages/local/import-embed", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handlePagesLocalImportEmbed(rec, req)
	return rec
}

const testUID16 = "00aabbccddeeff11"

func TestPagesImportEmbed(t *testing.T) {
	s, dataDir := newImportTestServer(t)
	payload := jpegBytes(4096)
	writeEmbed(t, dataDir, testUID16, "20260709_120000.jfif", payload)

	rec := doImport(t, s, "embeds/"+testUID16+"/20260709_120000.jfif", "articles/img/hero.jfif")
	if rec.Code != http.StatusOK {
		t.Fatalf("import: got %d body %q", rec.Code, rec.Body.String())
	}
	var resp struct {
		Dest        string `json:"dest"`
		SizeBytes   int    `json:"sizeBytes"`
		ContentType string `json:"contentType"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ContentType != "image/jpeg" || resp.SizeBytes != len(payload) {
		t.Fatalf("unexpected response: %+v", resp)
	}
	dest := filepath.Join(s.PagesDir, "articles", "img", "hero.jfif")
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("dest bytes differ from source")
	}
	fi, err := os.Stat(dest)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("dest mode = %v, want 0600", fi.Mode().Perm())
	}

	// Re-import onto the same dest must refuse: imports never overwrite.
	if rec := doImport(t, s, "embeds/"+testUID16+"/20260709_120000.jfif", "articles/img/hero.jfif"); rec.Code != http.StatusConflict {
		t.Fatalf("re-import: got %d, want 409", rec.Code)
	}
}

func TestPagesImportEmbedRejectsBadSources(t *testing.T) {
	s, dataDir := newImportTestServer(t)
	writeEmbed(t, dataDir, testUID16, "ok.jfif", jpegBytes(64))
	// A file outside the embeds root that traversal would love to reach.
	if err := os.WriteFile(filepath.Join(dataDir, "secret.jpg"), jpegBytes(64), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, src := range []string{
		"",
		"embeds/" + testUID16 + "/../ok.jfif",
		"embeds/" + testUID16 + "/../../secret.jpg",
		"/etc/passwd",
		"embeds/0011/short.jfif",
		"embeds/" + strings.ToUpper(testUID16) + "/ok.jfif",
		"embeds/" + testUID16 + "/sub/dir.jfif",
		"downloads/" + testUID16 + "/ok.jfif",
		"embeds/" + testUID16 + "/bad\\name.jfif",
	} {
		if rec := doImport(t, s, src, "a.jpg"); rec.Code != http.StatusBadRequest {
			t.Errorf("source %q: got %d, want 400", src, rec.Code)
		}
	}

	if rec := doImport(t, s, "embeds/"+testUID16+"/missing.jfif", "a.jpg"); rec.Code != http.StatusNotFound {
		t.Errorf("missing source: got %d, want 404", rec.Code)
	}
}

func TestPagesImportEmbedRejectsBadDests(t *testing.T) {
	s, dataDir := newImportTestServer(t)
	writeEmbed(t, dataDir, testUID16, "ok.jfif", jpegBytes(64))
	src := "embeds/" + testUID16 + "/ok.jfif"

	for _, dest := range []string{
		"",
		"../escape.jpg",
		"a/../../escape.jpg",
		"/abs.jpg",
		"page.md",
		"tool.exe",
		"noext",
		"img/photo.svg",
		"bad\\name.jpg",
	} {
		if rec := doImport(t, s, src, dest); rec.Code != http.StatusBadRequest {
			t.Errorf("dest %q: got %d, want 400", dest, rec.Code)
		}
	}
}

func TestPagesImportEmbedContentGuards(t *testing.T) {
	s, dataDir := newImportTestServer(t)

	// Not a raster image despite the extension.
	writeEmbed(t, dataDir, testUID16, "fake.jpg", []byte("#!/bin/sh\necho pwned\n"))
	if rec := doImport(t, s, "embeds/"+testUID16+"/fake.jpg", "a.jpg"); rec.Code != http.StatusBadRequest {
		t.Errorf("non-image content: got %d, want 400", rec.Code)
	}

	// Oversized source.
	writeEmbed(t, dataDir, testUID16, "big.jfif", jpegBytes(maxImportEmbedBytes+1))
	if rec := doImport(t, s, "embeds/"+testUID16+"/big.jfif", "b.jpg"); rec.Code != http.StatusBadRequest {
		t.Errorf("oversize source: got %d, want 400", rec.Code)
	}

	// Symlink source refused even when it points at a valid image inside the root.
	writeEmbed(t, dataDir, testUID16, "real.jfif", jpegBytes(64))
	link := filepath.Join(dataDir, "db", "embeds", testUID16, "link.jfif")
	if err := os.Symlink("real.jfif", link); err != nil {
		t.Fatal(err)
	}
	if rec := doImport(t, s, "embeds/"+testUID16+"/link.jfif", "c.jpg"); rec.Code != http.StatusBadRequest {
		t.Errorf("symlink source: got %d, want 400", rec.Code)
	}

	// A dest that already exists as a symlink is refused (exists check + O_EXCL).
	if err := os.MkdirAll(s.PagesDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dataDir, "outside.jpg"), filepath.Join(s.PagesDir, "d.jpg")); err != nil {
		t.Fatal(err)
	}
	if rec := doImport(t, s, "embeds/"+testUID16+"/real.jfif", "d.jpg"); rec.Code != http.StatusConflict {
		t.Errorf("symlink dest: got %d, want 409", rec.Code)
	}
}
