{{- $alias := .Aliases.Table .Table.Name -}}
{{- $colDefs := sqlColDefinitions .Table.Columns .Table.PKey.Columns -}}
{{- $pkNames := $colDefs.Names | stringMap (aliasCols $alias) | stringMap .StringFuncs.camelCase | stringMap .StringFuncs.replaceReserved -}}
{{- $pkArgs := joinSlices " " $pkNames $colDefs.Types | join ", " -}}
{{- $canSoftDelete := .Table.CanSoftDelete }}
{{if .AddGlobal -}}
// Find{{$alias.UpSingular}}G retrieves a single record by ID.
func Find{{$alias.UpSingular}}G({{if not .NoContext}}ctx context.Context, {{end -}} {{$pkArgs}}, selectCols ...string) (*{{$alias.UpSingular}}, error) {
	return Find{{$alias.UpSingular}}({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, {{$pkNames | join ", "}}, selectCols...)
}

{{end -}}

{{if .AddPanic -}}
// Find{{$alias.UpSingular}}P retrieves a single record by ID with an executor, and panics on error.
func Find{{$alias.UpSingular}}P({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, {{$pkArgs}}, selectCols ...string) *{{$alias.UpSingular}} {
	retobj, err := Find{{$alias.UpSingular}}({{if not .NoContext}}ctx, {{end -}} exec, {{$pkNames | join ", "}}, selectCols...)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return retobj
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// Find{{$alias.UpSingular}}GP retrieves a single record by ID, and panics on error.
func Find{{$alias.UpSingular}}GP({{if not .NoContext}}ctx context.Context, {{end -}} {{$pkArgs}}, selectCols ...string) *{{$alias.UpSingular}} {
	retobj, err := Find{{$alias.UpSingular}}({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, {{$pkNames | join ", "}}, selectCols...)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return retobj
}

{{end -}}

// Find{{$alias.UpSingular}} retrieves a single record by ID with an executor.
// If selectCols is empty Find will return all columns.
func Find{{$alias.UpSingular}}({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, {{$pkArgs}}, selectCols ...string) (*{{$alias.UpSingular}}, error) {
	{{$alias.DownSingular}}Obj := &{{$alias.UpSingular}}{}

	sel := "*"
	if len(selectCols) > 0 {
		sel = strings.Join(strmangle.IdentQuoteSlice(dialect.LQ, dialect.RQ, selectCols), ",")
	}
	query := fmt.Sprintf(
		"select %s from {{.Table.Name | .SchemaTable}} where {{if .Dialect.UseIndexPlaceholders}}{{whereClause .LQ .RQ 1 .Table.PKey.Columns}}{{else}}{{whereClause .LQ .RQ 0 .Table.PKey.Columns}}{{end}}{{if and .AddSoftDeletes $canSoftDelete}} and {{"deleted_at" | $.Quotes}} is null{{end}}", sel,
	)

	q := queries.Raw(query, {{$pkNames | join ", "}})

	err := q.Bind({{if not .NoContext}}ctx{{else}}nil{{end}}, exec, {{$alias.DownSingular}}Obj)
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Wrap(err, "{{.PkgName}}: unable to select from {{.Table.Name}}")
	}

	return {{$alias.DownSingular}}Obj, nil
}

{{ range $uniqKey := .Table.UniqKeys -}}
{{- $ukColDefs := sqlColDefinitions $.Table.Columns $uniqKey.Columns -}}
{{- $ukColNames := $ukColDefs.Names | stringMap (aliasCols $alias) | stringMap $.StringFuncs.camelCase | stringMap $.StringFuncs.replaceReserved -}}
{{- $ukArgs := joinSlices " " $ukColNames $ukColDefs.Types | join ", " -}}
{{- $fnName := $ukColNames | stringMap $.StringFuncs.titleCase | join "" -}}

// Find{{$alias.UpSingular}}By{{$fnName}} retrieves a single record by uniq key {{$ukColNames}} with an executor.
func Find{{$alias.UpSingular}}By{{$fnName}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, {{$ukArgs}}, selectCols ...string) (*{{$alias.UpSingular}}, error) {
	{{$alias.DownSingular}}Obj := &{{$alias.UpSingular}}{}

	sel := "*"
	if len(selectCols) > 0 {
		sel = strings.Join(strmangle.IdentQuoteSlice(dialect.LQ, dialect.RQ, selectCols), ",")
	}
	query := fmt.Sprintf(
		"select %s from {{$.Table.Name | $.SchemaTable}} where {{if $.Dialect.UseIndexPlaceholders}}{{whereClause $.LQ $.RQ 1 $uniqKey.Columns}}{{else}}{{whereClause $.LQ $.RQ 0 $uniqKey.Columns}}{{end}}{{if and $.AddSoftDeletes $canSoftDelete}} and {{"deleted_at" | $.Quotes}} is null{{end}}", sel,
	)

	q := queries.Raw(query, {{$ukColNames | join ", "}})

	err := q.Bind({{if not $.NoContext}}ctx{{else}}nil{{end}}, exec, {{$alias.DownSingular}}Obj)
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Wrap(err, "{{$.PkgName}}: unable to select from {{$.Table.Name}}")
	}

	return {{$alias.DownSingular}}Obj, nil
}

{{ if gt (len $uniqKey.Columns) 1 -}}
{{- $columns := slice $uniqKey.Columns 0 1 -}}
{{- $ukColDefs := sqlColDefinitions $.Table.Columns $columns -}}
{{- $ukColNames := $ukColDefs.Names | stringMap (aliasCols $alias) | stringMap $.StringFuncs.camelCase | stringMap $.StringFuncs.replaceReserved -}}
{{- $ukArgs := joinSlices " " $ukColNames $ukColDefs.Types | join ", " -}}
{{- $fnName := $ukColNames | stringMap $.StringFuncs.titleCase | join "" -}}

// Find{{$alias.UpSingular}}SliceBy{{$fnName}} retrieves a slice of records by first field of uniq key {{$ukColNames}} with an executor.
func Find{{$alias.UpSingular}}SliceBy{{$fnName}} ({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, {{$ukArgs}}) (*{{$alias.UpSingular}}Slice, error) {
    var o []*{{$alias.UpSingular}}

	sel := "*"
	if len(selectCols) > 0 {
		sel = strings.Join(strmangle.IdentQuoteSlice(dialect.LQ, dialect.RQ, selectCols), ",")
	}
	query := fmt.Sprintf(
		"select {{$.Table.Name | $.SchemaTable}}.* from {{$.Table.Name | $.SchemaTable}} where {{if $.Dialect.UseIndexPlaceholders}}{{whereClause $.LQ $.RQ 1 $columns}}{{else}}{{whereClause $.LQ $.RQ 0 $columns}}{{end}}{{if and $.AddSoftDeletes $canSoftDelete}} and {{"deleted_at" | $.Quotes}} is null{{end}}", sel,
	)

	q := queries.Raw(query, {{$ukColNames | join ", "}})

	err := q.Bind({{if not $.NoContext}}ctx{{else}}nil{{end}}, exec, &o)
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Wrap(err, "{{$.PkgName}}: unable to select from {{$.Table.Name}}")
	}

	return o, nil
}
{{ end -}}
{{ end -}}
