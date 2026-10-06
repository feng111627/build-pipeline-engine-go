package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Cache computes stable cache keys and persists run metadata on disk.
type Cache struct {
	root string
}

// NewCache opens a cache rooted at dir (created on first use).
func NewCache(dir string) *Cache { return &Cache{root: dir} }

// keyInput is the canonical, order-independent representation hashed
// into a cache key.
type keyInput struct {
	Name    string `json:"name"`
	Value   string `json:"value,omitempty"`
	File    string `json:"file,omitempty"`
	Content string `json:"content,omitempty"` // sha256 of the file
}

// Key computes the cache key for a step. The key is a function of the
// command, declared inputs (literal values or file contents) and the
// cache keys of every dependency. Inputs are sorted by name and JSON
// is canonicalized, so the key never depends on declaration or
// execution order.
func (c *Cache) Key(spec StepSpec, depKeys map[string]string) (string, error) {
	ins := make([]keyInput, 0, len(spec.Inputs))
	h := sha256.New()
	for _, in := range spec.Inputs {
		ki := keyInput{Name: in.Name, Value: in.Value, File: in.File}
		if in.File != "" {
			sum, err := fileSHA256(in.File)
			if err != nil {
				return "", fmt.Errorf("hashing input %q of step %q: %w", in.Name, spec.Name, err)
			}
			ki.Content = sum
		}
		ins = append(ins, ki)
	}
	sort.Slice(ins, func(i, j int) bool { return ins[i].Name < ins[j].Name })

	depNames := make([]string, 0, len(depKeys))
	for n := range depKeys {
		depNames = append(depNames, n)
	}
	sort.Strings(depNames)
	deps := make([][2]string, 0, len(depNames))
	for _, n := range depNames {
		deps = append(deps, [2]string{n, depKeys[n]})
	}

	canonical := struct {
		Command string      `json:"command"`
		Inputs  []keyInput  `json:"inputs"`
		Deps    [][2]string `json:"deps"`
	}{spec.Command, ins, deps}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// path returns the record file for a key (sharded by first 2 chars).
func (c *Cache) path(key string) string {
	return filepath.Join(c.root, key[:2], key+".json")
}

// Has reports whether a successful run for key is cached.
func (c *Cache) Has(key string) bool {
	if len(key) < 2 {
		return false
	}
	_, err := os.Stat(c.path(key))
	return err == nil
}

// RecordEntry is one executed-command attempt stored in a cache record.
type RecordEntry struct {
	Attempt int      `json:"attempt"`
	Cmd     string   `json:"cmd"`
	Args    []string `json:"args"`
	Output  string   `json:"output"`
}

// Record is what gets persisted per successful step run.
type Record struct {
	Step    string        `json:"step"`
	Key     string        `json:"key"`
	Entries []RecordEntry `json:"entries"`
}

// Save persists a successful run record under key, atomically.
func (c *Cache) Save(key string, rec Record) error {
	if len(key) < 2 {
		return fmt.Errorf("invalid cache key %q", key)
	}
	dir := filepath.Join(c.root, key[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	final := c.path(key)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, final)
}

// Load reads a previously saved record.
func (c *Cache) Load(key string) (Record, error) {
	var rec Record
	b, err := os.ReadFile(c.path(key))
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(b, &rec)
	return rec, err
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
