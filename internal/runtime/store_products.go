// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	toml "github.com/pelletier/go-toml"
)

// storeProduct mirrors the fields simplestore reads from products/*.toml. The
// dashboard manages products through these; the store picks up changes via its
// live-reload watcher. Price is in USD (the store converts to DCR at order
// time using the exchange rate).
type storeProduct struct {
	Title       string   `toml:"title" json:"title"`
	SKU         string   `toml:"sku" json:"sku"`
	Description string   `toml:"description" json:"description"`
	Tags        []string `toml:"tags,omitempty" json:"tags"`
	Price       float64  `toml:"price" json:"price"`
	Shipping    bool     `toml:"shipping,omitempty" json:"shipping"`
	Disabled    bool     `toml:"disabled,omitempty" json:"disabled"`
	// SendFilename is a path (relative to the store dir) to a file the store
	// delivers to the buyer once the order's invoice settles - i.e. a digital
	// download. The toml key is "sendfilename" to match simplestore's Product
	// field (go-toml matches it case-insensitively).
	SendFilename string `toml:"sendfilename,omitempty" json:"sendfilename,omitempty"`
}

type storeProductsFile struct {
	Products []storeProduct `toml:"products"`
}

var storeSKURE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// storeTagRE bounds a category tag to a short label of letters/digits/spaces and
// a few separators. Tags are interpolated into generated storefront templates
// (section headings and `{{ if eq . "<tag>" }}` guards), so they must not carry
// template or embed metacharacters such as { } [ ] " or =.
var storeTagRE = regexp.MustCompile(`^[\p{L}\p{N} &.,'/+-]{1,48}$`)

// storeFieldInjectionTokens are markup sequences that must never appear in a
// product field, since the theme generator places title/tags into generated
// template source. They would otherwise allow store template/embed injection
// (and, via an absolute-path embed, arbitrary server-file read).
var storeFieldInjectionTokens = []string{"--embed", "--form", "{{", "}}"}

const storeProductsFilename = "products.toml"

// validateProductContent rejects product titles/tags that could inject into the
// generated storefront templates. Description is rendered as a template data
// value (escaped at render), so it is not constrained here.
func validateProductContent(p storeProduct) error {
	for _, tok := range storeFieldInjectionTokens {
		if strings.Contains(p.Title, tok) {
			return fmt.Errorf("title may not contain %q", tok)
		}
	}
	for _, t := range p.Tags {
		if !storeTagRE.MatchString(t) {
			return fmt.Errorf("tag %q must be 1-48 chars of letters, digits, spaces or & . , ' / + -", t)
		}
	}
	return nil
}

func (s *storeController) productsDir() string {
	return filepath.Join(s.storeDir, "products")
}

// listProducts reads every products/*.toml and returns the products in stable
// order, last SKU winning on collision.
func (s *storeController) listProducts() ([]storeProduct, error) {
	dir := s.productsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []storeProduct{}, nil
		}
		return nil, err
	}
	bySKU := map[string]storeProduct{}
	var order []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".toml" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var pf storeProductsFile
		if err := toml.Unmarshal(data, &pf); err != nil {
			continue
		}
		for _, p := range pf.Products {
			if _, seen := bySKU[p.SKU]; !seen {
				order = append(order, p.SKU)
			}
			bySKU[p.SKU] = p
		}
	}
	out := make([]storeProduct, 0, len(order))
	for _, sku := range order {
		out = append(out, bySKU[sku])
	}
	return out, nil
}

// writeAllProducts consolidates the full product set into products.toml (via a
// temp file + atomic rename so the store's watcher never reads a partial file)
// and removes any other *.toml in products/. This keeps the dashboard the
// single source of truth and prevents the store from seeing a duplicate SKU
// across files.
func (s *storeController) writeAllProducts(products []storeProduct) error {
	dir := s.productsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := toml.Marshal(storeProductsFile{Products: products})
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, storeProductsFilename+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, storeProductsFilename)); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == storeProductsFilename || filepath.Ext(e.Name()) != ".toml" {
			continue
		}
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	return nil
}

// saveProduct upserts a product by SKU. When create is true the SKU must not
// already exist (so adding a product never silently overwrites an existing one);
// when false it updates the matching product, or adds it if absent.
func (s *storeController) saveProduct(p storeProduct, create bool) error {
	if !storeSKURE.MatchString(p.SKU) {
		return fmt.Errorf("sku must be 1-64 chars of letters, digits, dash or underscore")
	}
	if p.Title == "" {
		return fmt.Errorf("title is required")
	}
	if err := validateProductContent(p); err != nil {
		return err
	}
	products, err := s.listProducts()
	if err != nil {
		return err
	}
	replaced := false
	for i := range products {
		if products[i].SKU == p.SKU {
			if create {
				return fmt.Errorf("a product with SKU %q already exists", p.SKU)
			}
			products[i] = p
			replaced = true
			break
		}
	}
	if !replaced {
		products = append(products, p)
	}
	return s.writeAllProducts(products)
}

// saveStoreFile writes an uploaded file to relPath under the store dir
// (creating parent dirs), returning the cleaned relative path. The path is
// gated by validateStoreMediaRel (no templates, no operational dirs, no
// traversal). When overwrite is false an existing file is left untouched and an
// error returned, so an upload never silently clobbers a cover/download/banner.
// A pre-existing symlink at the target is refused so a write can't be redirected
// out of the store dir. These are the files products reference via SendFilename
// for digital delivery on payment, plus cover images and the header banner.
func (s *storeController) saveStoreFile(relPath string, overwrite bool, src io.Reader) (string, error) {
	rel, err := s.validateStoreMediaRel(relPath)
	if err != nil {
		return "", err
	}
	full := filepath.Join(s.storeDir, rel)
	if fi, err := os.Lstat(full); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("refusing to write through a symlink")
		}
		if !overwrite {
			return "", fmt.Errorf("file already exists: %s", rel)
		}
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return "", err
	}
	// O_EXCL on create closes the create-time TOCTOU/symlink window; O_TRUNC is
	// only used when the caller explicitly opted into overwriting an existing
	// regular file (verified above).
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if overwrite {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(full, flags, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, src); err != nil {
		return "", err
	}
	return rel, nil
}

// deleteProduct removes a product by SKU.
func (s *storeController) deleteProduct(sku string) error {
	products, err := s.listProducts()
	if err != nil {
		return err
	}
	out := make([]storeProduct, 0, len(products))
	for _, p := range products {
		if p.SKU != sku {
			out = append(out, p)
		}
	}
	return s.writeAllProducts(out)
}
