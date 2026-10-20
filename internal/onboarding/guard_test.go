package onboarding

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providerruntime"
)

// The guard works on import sets so its rules are tested without breaking the
// real tree; the real run uses go list.
const module = "github.com/cbl/cbl-connector/"

func isCore(path string) bool {
	switch path {
	case module + "internal/usecase",
		module + "internal/transport",
		module + "internal/storage",
		module + "internal/domain",
		module + "internal/gateway":
		return true
	}
	return false
}

func isProvider(path string) bool {
	return path == module+"internal/providers" || strings.HasPrefix(path, module+"internal/providers/")
}

// checkImports returns the boundary violations of an import graph (G1 - G3).
func checkImports(pkgs map[string][]string) []string {
	violations := make([]string, 0)
	for importer, imports := range pkgs {
		for _, dep := range imports {
			switch {
			case isProvider(importer):
				switch dep {
				case module + "internal/usecase",
					module + "internal/transport",
					module + "internal/storage",
					module + "internal/gateway",
					module + "internal/providerruntime":
					violations = append(violations, importer+" imports "+dep)
				}
			}
			if isProvider(dep) {
				allowed := isProvider(importer) || importer == module+"internal/providerruntime"
				if !allowed {
					violations = append(violations, importer+" imports provider package "+dep)
				}
			}
			if isCore(importer) && (isProvider(dep) || dep == module+"internal/providerruntime") {
				violations = append(violations, importer+" imports "+dep)
			}
		}
	}
	sort.Strings(violations)
	return violations
}

func TestImportGuardRules(t *testing.T) {
	cases := []struct {
		id      string
		graph   map[string][]string
		wantHit bool
	}{
		{
			id: "clean graph",
			graph: map[string][]string{
				module + "internal/providers/acme":  {module + "internal/domain"},
				module + "internal/usecase":         {module + "internal/domain"},
				module + "internal/providerruntime": {module + "internal/providers/acme"},
			},
		},
		{
			id: "O1 provider imports core",
			graph: map[string][]string{
				module + "internal/providers/acme": {module + "internal/usecase"},
			},
			wantHit: true,
		},
		{
			id: "O2 core imports provider",
			graph: map[string][]string{
				module + "internal/usecase": {module + "internal/providers/acme"},
			},
			wantHit: true,
		},
		{
			id: "O3 third package imports provider",
			graph: map[string][]string{
				module + "internal/transport": {module + "internal/providers/wire"},
			},
			wantHit: true,
		},
		{
			id: "provider imports provider helper",
			graph: map[string][]string{
				module + "internal/providers/acme": {module + "internal/providers/wire"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			violations := checkImports(tc.graph)
			if tc.wantHit && len(violations) == 0 {
				t.Fatal("the guard missed a violation")
			}
			if !tc.wantHit && len(violations) != 0 {
				t.Fatalf("the guard reported %v", violations)
			}
		})
	}
}

// listedPackage is the subset of go list -json the guard needs.
type listedPackage struct {
	ImportPath string
	Imports    []string
}

// moduleRoot locates the repository root from the test file location.
func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	_, err := os.Stat(filepath.Join(root, "go.mod"))
	require.NoError(t, err, "the module root must contain go.mod")
	return root
}

// listGraph runs go list over the module and returns the import graph.
func listGraph(t *testing.T, root string) map[string][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-json", "./...")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	graph := make(map[string][]string)
	decoder := json.NewDecoder(bytes.NewReader(out))
	for {
		var pkg listedPackage
		if decodeErr := decoder.Decode(&pkg); decodeErr == io.EOF {
			break
		} else if decodeErr != nil {
			t.Fatalf("decode go list output: %v", decodeErr)
		}
		graph[pkg.ImportPath] = pkg.Imports
	}
	require.NotEmpty(t, graph)
	return graph
}

func TestImportGuard(t *testing.T) {
	graph := listGraph(t, moduleRoot(t))
	assert.Empty(t, checkImports(graph), "provider and core import boundaries are violated")
}

func TestManifestFactoryParity(t *testing.T) {
	manifests := domain.BuiltinManifests()
	byID := make(map[string]domain.ProviderManifest, len(manifests))
	for _, manifest := range manifests {
		require.NoError(t, manifest.Validate(), "manifest %q", manifest.ID)
		_, duplicate := byID[manifest.ID]
		require.False(t, duplicate, "duplicate manifest %q", manifest.ID)
		byID[manifest.ID] = manifest
	}

	factoryIDs := make(map[string]bool)
	for _, factory := range providerruntime.BuiltinFactories() {
		assert.False(t, factoryIDs[factory.Provider], "duplicate factory %q", factory.Provider)
		factoryIDs[factory.Provider] = true
	}
	for _, manifest := range manifests {
		if manifest.Sandbox {
			assert.False(t, factoryIDs[manifest.ID], "sandbox provider %q must not have a runtime factory", manifest.ID)
			continue
		}
		assert.True(t, factoryIDs[manifest.ID], "provider %q has no runtime factory", manifest.ID)
	}
	for provider := range factoryIDs {
		_, known := byID[provider]
		assert.True(t, known, "factory %q has no manifest", provider)
	}
}
