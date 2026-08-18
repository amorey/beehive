package beehive

import (
	"go/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTheTestSeamHasOneProducer(t *testing.T) {
	// The seam is the one door to a controller's columns from outside a pass.
	// A second assignment would be a second door, and nothing else would say so.
	var sites []string
	forEachSourceFile(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assign.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				pkg, ok := sel.X.(*ast.Ident)
				if ok && pkg.Name == "testseam" && sel.Sel.Name == "Open" {
					sites = append(sites, path)
				}
			}
			return true
		})
	})
	assert.Equal(t, []string{"testseam.go"}, sites)
}
