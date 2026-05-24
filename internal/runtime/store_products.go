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

const storeProductsFilename = "products.toml"

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

// saveProduct upserts a product by SKU.
func (s *storeController) saveProduct(p storeProduct) error {
	if !storeSKURE.MatchString(p.SKU) {
		return fmt.Errorf("sku must be 1-64 chars of letters, digits, dash or underscore")
	}
	if p.Title == "" {
		return fmt.Errorf("title is required")
	}
	products, err := s.listProducts()
	if err != nil {
		return err
	}
	replaced := false
	for i := range products {
		if products[i].SKU == p.SKU {
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
// (creating parent dirs), returning the cleaned relative path. relPath is
// sanitized to stay within the store dir. These are the files products
// reference via SendFilename for digital delivery on payment.
func (s *storeController) saveStoreFile(relPath string, src io.Reader) (string, error) {
	rel := strings.TrimPrefix(filepath.Clean("/"+strings.TrimSpace(relPath)), "/")
	if rel == "" || strings.Contains(rel, "..") {
		return "", fmt.Errorf("invalid file path")
	}
	full := filepath.Join(s.storeDir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return "", err
	}
	f, err := os.Create(full)
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
