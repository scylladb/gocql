//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
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

	// A public alias name must exist for every internal named type appearing in
	// an implementable extension signature. Merely exporting the interface does
	// not let a consumer name a concrete internal parameter such as HostInfo.
	publicTypeNames := map[string][]string{}
	for _, name := range root.Scope().Names() {
		obj, ok := root.Scope().Lookup(name).(*types.TypeName)
		if !ok || !obj.Exported() {
			continue
		}
		if n, ok := types.Unalias(obj.Type()).(*types.Named); ok && n.Obj().Pkg() != nil {
			key := n.Obj().Pkg().Path() + "." + n.Obj().Name()
			publicTypeNames[key] = append(publicTypeNames[key], name)
		}
	}
	extensionNames := strings.Fields("AuthProvider AuthExchange ConvictionPolicy ReconnectPolicy Dialer DNSResolver AddressTranslator AddressTranslatorV2 AddressTranslatorHostInfo Observer LifecycleObserver RoutingPolicy QueryPlan TokenPartitioner RetryPolicy SpeculationPolicy Encoder Decoder Codec ParamEncoder ParamPlan RowDecoder RowPlan StreamDecoder StreamRowDecoder StreamValuePlan StreamRowPlan StreamResetter")

	// Include every other externally implementable public interface as well as
	// the mandatory extension list, so newly retained hooks cannot evade checks.
	extensionSet := map[string]bool{}
	for _, name := range extensionNames {
		extensionSet[name] = true
	}
	sealedInterfaces := map[string][]string{}
	for _, name := range root.Scope().Names() {
		obj, ok := root.Scope().Lookup(name).(*types.TypeName)
		if !ok || !obj.Exported() {
			continue
		}
		iface, ok := types.Unalias(obj.Type()).Underlying().(*types.Interface)
		if !ok {
			continue
		}
		sealed := false
		for i := 0; i < iface.NumMethods(); i++ {
			if !iface.Method(i).Exported() {
				sealed = true
				sealedInterfaces[name] = append(sealedInterfaces[name], iface.Method(i).Name())
			}
		}
		if !sealed && !extensionSet[name] {
			extensionNames = append(extensionNames, name)
			extensionSet[name] = true
		}
	}
	interfaceSignatures := map[string][]string{}
	var unnameable []map[string]string
	for _, name := range extensionNames {
		obj := root.Scope().Lookup(name)
		if obj == nil {
			panic("missing extension " + name)
		}
		iface, ok := types.Unalias(obj.Type()).Underlying().(*types.Interface)
		if !ok {
			panic("extension is not interface: " + name)
		}
		visited := map[types.Type]bool{}
		var checkNameable func(types.Type, string)
		checkNameable = func(t types.Type, path string) {
			if t == nil || visited[t] {
				return
			}
			visited[t] = true
			switch x := t.(type) {
			case *types.Alias:
				checkNameable(types.Unalias(x), path)
			case *types.Named:
				if x.Obj().Pkg() != nil {
					key := x.Obj().Pkg().Path() + "." + x.Obj().Name()
					if strings.HasPrefix(x.Obj().Pkg().Path(), "github.com/scylladb/gocql/v2/internal/") && len(publicTypeNames[key]) == 0 {
						unnameable = append(unnameable, map[string]string{"interface": name, "signature_path": path, "unnameable_type": key})
					}
				}
				if args := x.TypeArgs(); args != nil {
					for i := 0; i < args.Len(); i++ {
						checkNameable(args.At(i), path+" type argument")
					}
				}
			case *types.Pointer:
				checkNameable(x.Elem(), path+" pointer")
			case *types.Slice:
				checkNameable(x.Elem(), path+" slice")
			case *types.Array:
				checkNameable(x.Elem(), path+" array")
			case *types.Map:
				checkNameable(x.Key(), path+" map key")
				checkNameable(x.Elem(), path+" map value")
			case *types.Chan:
				checkNameable(x.Elem(), path+" channel")
			case *types.Signature:
				checkNameable(x.Params(), path+" parameter")
				checkNameable(x.Results(), path+" result")
			case *types.Tuple:
				for i := 0; i < x.Len(); i++ {
					checkNameable(x.At(i).Type(), path+" "+x.At(i).Name())
				}
			case *types.Interface:
				for i := 0; i < x.NumMethods(); i++ {
					checkNameable(x.Method(i).Type(), path+" method "+x.Method(i).Name())
				}
			}
		}
		for i := 0; i < iface.NumMethods(); i++ {
			method := iface.Method(i)
			interfaceSignatures[name] = append(interfaceSignatures[name], types.ObjectString(method, func(p *types.Package) string { return p.Path() }))
			checkNameable(method.Type(), name+"."+method.Name())
		}
	}

	// Compile concrete implementations in a genuinely separate consumer module.
	// The generated bodies are never called; this is a signature/nameability test.
	// Behavioral failure/decision/reset tests live beside the pool and native lane.
	consumer := map[string]any{}
	if len(unnameable) == 0 {
		var body strings.Builder
		imports := map[string]string{"github.com/scylladb/gocql/v2": "gocql"}
		qualify := func(p *types.Package) string {
			if strings.HasPrefix(p.Path(), "github.com/scylladb/gocql/v2/internal/") {
				return "gocql"
			}
			imports[p.Path()] = p.Name()
			return p.Name()
		}
		for _, name := range extensionNames {
			typ := root.Scope().Lookup(name).Type()
			var parameters *types.TypeParamList
			switch x := typ.(type) {
			case *types.Alias:
				parameters = x.TypeParams()
			case *types.Named:
				parameters = x.TypeParams()
			}
			var declarations, arguments, concrete []string
			if parameters != nil {
				for i := 0; i < parameters.Len(); i++ {
					parameter := parameters.At(i)
					declarations = append(declarations, parameter.Obj().Name()+" any")
					arguments = append(arguments, parameter.Obj().Name())
					concrete = append(concrete, "any")
				}
			}
			generic, applied, instantiated := "", "", ""
			if len(declarations) > 0 {
				generic = "[" + strings.Join(declarations, ",") + "]"
				applied = "[" + strings.Join(arguments, ",") + "]"
				instantiated = "[" + strings.Join(concrete, ",") + "]"
			}
			fmt.Fprintf(&body, "type implementation%s%s struct{}\n", name, generic)
			iface := types.Unalias(typ).Underlying().(*types.Interface)
			for i := 0; i < iface.NumMethods(); i++ {
				method := iface.Method(i)
				signature := strings.TrimPrefix(types.TypeString(method.Type(), qualify), "func")
				fmt.Fprintf(&body, "func (implementation%s%s) %s%s { panic(\"compile-only implementation\") }\n", name, applied, method.Name(), signature)
			}
			fmt.Fprintf(&body, "var _ gocql.%s%s = implementation%s%s{}\n", name, instantiated, name, instantiated)
		}
		var source bytes.Buffer
		source.WriteString("package contracts\nimport (\n")
		importPaths := []string{}
		for path := range imports {
			importPaths = append(importPaths, path)
		}
		sort.Strings(importPaths)
		for _, path := range importPaths {
			fmt.Fprintf(&source, "%s %q\n", imports[path], path)
		}
		source.WriteString(")\n")
		source.WriteString(body.String())
		formatted, err := format.Source(source.Bytes())
		if err != nil {
			panic(err)
		}
		dir := filepath.Join(auditRoot, "external-contracts")
		if err := os.MkdirAll(dir, 0700); err != nil {
			panic(err)
		}
		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		module := "module example.com/gocql-external-contracts\n\ngo 1.25.0\n\nrequire github.com/scylladb/gocql/v2 v2.0.0\n\nreplace github.com/scylladb/gocql/v2 => " + cwd + "\n"
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "contracts.go"), formatted, 0600); err != nil {
			panic(err)
		}

		behavioral := `package contracts
import (
 "errors"
 "net/netip"
 "testing"
 "github.com/scylladb/gocql/v2"
)
type statefulConviction struct { failures map[gocql.UUID]int }
func (p *statefulConviction) AddFailure(_ error,host gocql.HostSnapshot)bool {p.failures[host.ID]++;return p.failures[host.ID]>=2}
func (p *statefulConviction) Reset(host gocql.HostSnapshot){delete(p.failures,host.ID)}
func TestExternalConvictionImplementation(t *testing.T){
 policy:=&statefulConviction{failures:map[gocql.UUID]int{}}
 config:=gocql.DefaultConfig("127.0.0.1");config.Conviction=policy
 host:=gocql.HostSnapshot{ID:gocql.ParseUUIDMust("ed8ea335-19d6-4652-9d9a-4519c3b6eff0"),Address:netip.MustParseAddrPort("127.0.0.1:9042"),Datacenter:"dc1",Rack:"r1",Tokens:[]int64{-1,1},Enabled:true,Up:true}
 failure:=errors.New("external failure")
 if config.Conviction.AddFailure(failure,host)||!config.Conviction.AddFailure(failure,host){t.Fatal("stateful conviction decision")}
 config.Conviction.Reset(host)
 if config.Conviction.AddFailure(failure,host){t.Fatal("reset did not clear the selected host")}
 var simple gocql.ConvictionPolicy=&gocql.SimpleConvictionPolicy{}
 if !simple.AddFailure(failure,host){t.Fatal("simple conviction behavior changed")};simple.Reset(host)
 if !simple.AddFailure(failure,host){t.Fatal("simple reset behavior changed")}
}
`
		behaviorSource, err := format.Source([]byte(behavioral))
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "contracts_test.go"), behaviorSource, 0600); err != nil {
			panic(err)
		}
		consumer = map[string]any{"directory": dir, "source": string(formatted), "behavior_test_source": string(behaviorSource), "interfaces": extensionNames, "command": []string{"go", "test", "-mod=mod", "./..."}, "normal_root_build_without_test_aliases": true}
	}

	consumerCompileFailed := false
	if len(consumer) > 0 {
		command := exec.Command("go", "test", "-count=1", "-mod=mod", "./...")
		command.Dir = consumer["directory"].(string)
		output, err := command.CombinedOutput()
		consumer["output"] = string(output)
		consumer["exit_code"] = 0
		consumer["command"] = []string{"go", "test", "-count=1", "-mod=mod", "./..."}
		if err != nil {
			consumerCompileFailed = true
			consumer["exit_code"] = 1
			consumer["error"] = err.Error()
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
	enc.Encode(map[string]any{"sealed_public_interfaces": sealedInterfaces, "external_implementation_consumer": consumer, "required_interface_signatures": interfaceSignatures, "unnameable_required_interface_types": unnameable, "public_type_aliases": publicTypeNames, "public_exports": public, "removed_export_declarations": actualRemoved, "accepted_export_declaration_count": len(acceptedPublic), "required_typed_extension_and_execution_exports": required, "missing_required_exports": missing, "removed_compatibility_exports": removed, "remaining_compatibility_exports": remaining, "canonical_session_methods": sessionMethods, "thin_facade_functions": facadeFunctions, "reachable_internal_types": named, "issues": issues, "retained_value_metadata": "Existing value-only metadata/error descriptors and UUID/domain utilities remain available as permitted by the API contract; they execute no reflection serializer."})
	if len(issues) > 0 || len(missing) > 0 || len(remaining) > 0 || len(unnameable) > 0 || consumerCompileFailed {
		os.Exit(1)
	}
}
