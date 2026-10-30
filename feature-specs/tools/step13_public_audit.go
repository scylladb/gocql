//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"encoding/json"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var auditRoot = func() string {
	if p := os.Getenv("GOCQL_STEP13_AUDIT_DIR"); p != "" {
		return p
	}
	return "/tmp/gocql-step13-audit"
}()

func main() {
	exports := map[string]string{}
	fd, _ := os.Open(auditRoot + "/packages-all.json")
	dec := json.NewDecoder(fd)
	for {
		var p struct{ ImportPath, Export string }
		if dec.Decode(&p) != nil {
			break
		}
		exports[p.ImportPath] = p.Export
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", func(p string) (io.ReadCloser, error) { return os.Open(exports[p]) })
	root, e := imp.Import("github.com/scylladb/gocql/v2")
	if e != nil {
		panic(e)
	}
	seen := map[types.Type]bool{}
	var issues []map[string]string
	named := map[string]bool{}
	var visit func(types.Type)
	visit = func(t types.Type) {
		if t == nil || seen[t] {
			return
		}
		seen[t] = true
		switch x := t.(type) {
		case *types.Alias:
			visit(types.Unalias(x))
		case *types.Named:
			if x.Obj().Pkg() != nil && strings.HasPrefix(x.Obj().Pkg().Path(), "github.com/scylladb/gocql/v2/internal/") {
				key := x.Obj().Pkg().Path() + "." + x.Obj().Name()
				named[key] = true
				ms := types.NewMethodSet(types.NewPointer(x))
				for i := 0; i < ms.Len(); i++ {
					m := ms.At(i).Obj()
					if m.Exported() {
						if strings.HasPrefix(m.Name(), "Internal") {
							issues = append(issues, map[string]string{"kind": "method", "type": key, "name": m.Name()})
						}
						sig := m.Type().(*types.Signature)
						visit(sig.Params())
						visit(sig.Results())
					}
				}
				if st, ok := x.Underlying().(*types.Struct); ok {
					for i := 0; i < st.NumFields(); i++ {
						f := st.Field(i)
						if f.Exported() {
							if strings.HasPrefix(f.Name(), "Internal") {
								issues = append(issues, map[string]string{"kind": "field", "type": key, "name": f.Name()})
							}
							visit(f.Type())
						}
					}
				}
			}
			visit(x.Underlying())
		case *types.Pointer:
			visit(x.Elem())
		case *types.Slice:
			visit(x.Elem())
		case *types.Array:
			visit(x.Elem())
		case *types.Map:
			visit(x.Key())
			visit(x.Elem())
		case *types.Chan:
			visit(x.Elem())
		case *types.Signature:
			visit(x.Params())
			visit(x.Results())
		case *types.Tuple:
			for i := 0; i < x.Len(); i++ {
				visit(x.At(i).Type())
			}
		case *types.Interface:
			for i := 0; i < x.NumMethods(); i++ {
				m := x.Method(i)
				if m.Exported() {
					visit(m.Type())
				}
			}
		case *types.Struct:
			for i := 0; i < x.NumFields(); i++ {
				if x.Field(i).Exported() {
					visit(x.Field(i).Type())
				}
			}
		}
	}
	for _, name := range root.Scope().Names() {
		if ast.IsExported(name) {
			visit(root.Scope().Lookup(name).Type())
		}
	}

	public := map[string]string{}
	for _, name := range root.Scope().Names() {
		if ast.IsExported(name) {
			public[name] = types.ObjectString(root.Scope().Lookup(name), func(p *types.Package) string { return p.Path() })
		}
	}
	required := strings.Fields("Config DefaultConfig Connect OpenSingleHost Session Statement Prepare Prepared PrepareOptions BindOwned BindBorrowed BindUnpreparedOwned BindUnpreparedBorrowed Bound Exec Run RunPage RunUnpaged RunStream ExecBatch RunBatch BatchEntry BatchItem Options Result Profile ProfileHandle NewProfile AuthProvider AuthExchange PasswordAuth ReconnectPolicy ConvictionPolicy RoutingPolicy RoutingInfo QueryPlan Target TokenPartitioner PartitionKey Observer RequestEvent AttemptEvent MetadataSnapshot HostSnapshot Event EventFilter Subscription DialTarget Dialer DialerFunc DNSResolver AddressTranslator StreamLimits StreamOptions StreamRowDecoder StreamDecoder StreamRowPlan StreamValuePlan StreamResetter StreamRowReader StreamValueReader StreamRow StreamColumn StreamList StreamBlob StreamText StreamCapacityError StreamCapabilityError")
	missing := []string{}
	for _, name := range required {
		if root.Scope().Lookup(name) == nil {
			missing = append(missing, name)
		}
	}
	removed := strings.Fields("NewCluster ClusterConfig Query Iter Batch Marshal Unmarshal Marshaler Unmarshaler UDTMarshaler UDTUnmarshaler ClusterOption ClientRoutesConfig ClientRoutesOption HostSelectionPolicy TokenAwareHostPolicy RoundRobinHostPolicy DCAwareRoundRobinPolicy RackAwareRoundRobinPolicy HostFilter HostInfo HostInfoBuilder Authenticator PasswordAuthenticator SslOptions ReconnectionPolicy ConstantReconnectionPolicy ExponentialReconnectionPolicy NoReconnectionPolicy ConnectObserver ObservedConnect PoolConfig HostPoolInfo HostInformation SelectedHost Partitioner Token Compressor SegmentCompressor NewTracer TracerEnhanced NewTraceWriter TraceWriter NewNativeType NewCustomType NewTupleType NewUDTType NewCollectionType SerialConsistency SnappyCompressor KeyspaceUpdateEvent DialedHost NodeDown NodeUp")
	remaining := []string{}
	for _, name := range removed {
		if root.Scope().Lookup(name) != nil {
			remaining = append(remaining, name)
		}
	}
	sessionMethods := map[string]string{}
	session := types.Unalias(root.Scope().Lookup("Session").Type())
	ms := types.NewMethodSet(types.NewPointer(session))
	allowed := map[string]bool{}
	for _, name := range strings.Fields("ID AwaitSchemaAgreement Close Closed Metadata RefreshMetadata Metrics Subscribe Trace") {
		allowed[name] = true
	}
	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i).Obj()
		if m.Exported() {
			sessionMethods[m.Name()] = types.ObjectString(m, func(p *types.Package) string { return p.Path() })
			if !allowed[m.Name()] {
				issues = append(issues, map[string]string{"kind": "noncanonical Session method", "name": m.Name()})
			}
		}
	}
	// Every exported facade function forwards directly to one implementation.
	// Standalone serialization and streaming therefore share those implementations.
	facadeFunctions := map[string]string{}
	files, _ := filepath.Glob("v2_*facade.go")
	for _, path := range files {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			panic(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !fn.Name.IsExported() {
				continue
			}
			valid := false
			owner := ""
			if len(fn.Body.List) == 1 {
				var e ast.Expr
				switch stmt := fn.Body.List[0].(type) {
				case *ast.ReturnStmt:
					if len(stmt.Results) == 1 {
						e = stmt.Results[0]
					}
				case *ast.ExprStmt:
					e = stmt.X
				}
				if call, ok := e.(*ast.CallExpr); ok {
					target := call.Fun
					switch x := target.(type) {
					case *ast.IndexExpr:
						target = x.X
					case *ast.IndexListExpr:
						target = x.X
					}
					if sel, ok := target.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok {
							owner = id.Name
							valid = true
						}
					}
				}
			}
			facadeFunctions[fn.Name.Name] = owner
			if !valid {
				issues = append(issues, map[string]string{"kind": "non-thin facade", "name": fn.Name.Name, "file": path})
			}
		}
	}

	var acceptedDeclarations []struct{ File, Name, Receiver string }
	acceptedRaw, err := os.ReadFile(auditRoot + "/accepted-declarations.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(acceptedRaw, &acceptedDeclarations); err != nil {
		panic(err)
	}
	acceptedPublic := map[string]bool{}
	for _, d := range acceptedDeclarations {
		if !strings.Contains(d.File, "/") && !strings.HasSuffix(d.File, "_test.go") && d.Receiver == "" && ast.IsExported(d.Name) {
			acceptedPublic[d.Name] = true
		}
	}
	actualRemoved := []string{}
	for name := range acceptedPublic {
		if public[name] == "" {
			actualRemoved = append(actualRemoved, name)
		}
	}
	sort.Strings(actualRemoved)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"public_exports": public, "removed_export_declarations": actualRemoved, "accepted_export_declaration_count": len(acceptedPublic), "required_typed_extension_and_execution_exports": required, "missing_required_exports": missing, "removed_compatibility_exports": removed, "remaining_compatibility_exports": remaining, "canonical_session_methods": sessionMethods, "thin_facade_functions": facadeFunctions, "reachable_internal_types": named, "issues": issues, "retained_value_metadata": "Existing value-only metadata/error descriptors and UUID/domain utilities remain available as permitted by the API contract; they execute no reflection serializer."})
	if len(issues) > 0 || len(missing) > 0 || len(remaining) > 0 {
		os.Exit(1)
	}
}
