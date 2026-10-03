# Fine-grained authorization for every Trino query: humans, BI tools and AI agents
# acting on behalf of a colleague all hit the same rules.
#
#   * default deny: anything not explicitly allowed is refused
#   * schema access by persona (analysts never see silver; only platform admins see
#     bronze, and never the raw record payloads inside it)
#   * row filters: colleagues only see customers of the brands they serve
#   * column masks: PII is masked by tag and persona clearance, not by table
#
# Entitlements live in data.entitlements (see ../data). In production they arrive
# as a signed bundle built from IdP groups and data-catalog tags.
package trino

import rego.v1

ents := data.entitlements

default allow := false

user := input.context.identity.user

profile := ents.users[user]

persona := ents.personas[profile.persona]

is_admin if persona.admin == true

# ---------------------------------------------------------------------------
# Query-level operations
# ---------------------------------------------------------------------------
allow if {
	input.action.operation == "ExecuteQuery"
	profile
}

# Users may see and kill only their own queries; admins may see everything.
allow if {
	input.action.operation in {"ViewQueryOwnedBy", "KillQueryOwnedBy", "FilterViewQueryOwnedBy"}
	input.action.resource.user.user == user
}

allow if {
	input.action.operation in {"ViewQueryOwnedBy", "KillQueryOwnedBy", "FilterViewQueryOwnedBy"}
	is_admin
}

allow if {
	input.action.operation == "ReadSystemInformation"
	is_admin
}

# Machine identities (e.g. the metrics scraper) may read system metrics and nothing
# else: no query execution, no catalogs, no data.
allow if {
	input.action.operation == "ReadSystemInformation"
	ents.machines[user].read_system_information == true
}

# Only the query-timeout guard rails may be set by non-admins.
allow if {
	input.action.operation == "SetSystemSessionProperty"
	profile
	input.action.resource.systemSessionProperty.name in {"query_max_execution_time", "query_max_run_time"}
}

# ---------------------------------------------------------------------------
# Catalog / schema / table visibility
# ---------------------------------------------------------------------------
allowed_catalog(name) if {
	profile
	name in {"lakehouse", "system"}
}

allowed_schema(catalog, schema) if {
	catalog == "lakehouse"
	schema != "bronze"
	schema in persona.schemas
}

# Bronze has no brand filter (brand is added in silver), so it needs every brand too.
allowed_schema("lakehouse", "bronze") if {
	"bronze" in persona.schemas
	"*" in profile.brands
}

allowed_schema(catalog, schema) if {
	allowed_catalog(catalog)
	schema == "information_schema"
}

# system.jdbc / system.metadata back client metadata calls; system.runtime is admin-only.
allowed_schema("system", schema) if {
	profile
	schema in {"jdbc", "metadata"}
}

allowed_schema("system", schema) if {
	is_admin
	schema == "runtime"
}

allow if {
	input.action.operation == "AccessCatalog"
	allowed_catalog(input.action.resource.catalog.name)
}

allow if {
	input.action.operation == "FilterCatalogs"
	allowed_catalog(input.action.resource.catalog.name)
}

# ShowSchemas carries a catalog resource; FilterSchemas/ShowCreateSchema carry a schema.
allow if {
	input.action.operation == "ShowSchemas"
	allowed_catalog(input.action.resource.catalog.name)
}

allow if {
	input.action.operation in {"ShowSchemas", "FilterSchemas", "ShowCreateSchema"}
	s := input.action.resource.schema
	allowed_schema(s.catalogName, s.schemaName)
}

table_ops := {"ShowTables", "FilterTables", "ShowColumns", "FilterColumns", "SelectFromColumns", "ShowCreateTable"}

allow if {
	input.action.operation in table_ops
	t := input.action.resource.table
	allowed_schema(t.catalogName, t.schemaName)
}

# ShowTables arrives with a schema resource rather than a table.
allow if {
	input.action.operation == "ShowTables"
	s := input.action.resource.schema
	allowed_schema(s.catalogName, s.schemaName)
}

# Built-in functions don't reach here; this covers system-catalog table functions.
allow if {
	input.action.operation == "ExecuteFunction"
	profile
	input.action.resource.function.catalogName == "system"
}

# Everything else (DDL, DML, grants, impersonation...) is denied by default.
# Writes happen only through the ETL principal at the catalog layer, never via Trino.

# ---------------------------------------------------------------------------
# Tenant identities (ADR 14): a tenant's service account queries as itself and reads
# its own namespaces, unmasked and unfiltered (it owns that data). Nothing else: no
# platform schemas, no other tenant, no DDL (views arrive in the next step).
# ---------------------------------------------------------------------------
tenant := ents.tenants[user]

tenant_schema(catalog, schema) if {
	catalog == "lakehouse"
	schema in tenant.schemas
}

tenant_schema(catalog, schema) if {
	catalog in {"lakehouse", "system"}
	schema == "information_schema"
}

allow if {
	input.action.operation == "ExecuteQuery"
	tenant
}

allow if {
	input.action.operation in {"ViewQueryOwnedBy", "KillQueryOwnedBy", "FilterViewQueryOwnedBy"}
	tenant
	input.action.resource.user.user == user
}

allow if {
	input.action.operation in {"AccessCatalog", "FilterCatalogs", "ShowSchemas"}
	tenant
	input.action.resource.catalog.name == "lakehouse"
}

allow if {
	input.action.operation in {"ShowSchemas", "FilterSchemas", "ShowCreateSchema", "ShowTables"}
	tenant
	s := input.action.resource.schema
	tenant_schema(s.catalogName, s.schemaName)
}

allow if {
	input.action.operation in table_ops
	tenant
	t := input.action.resource.table
	tenant_schema(t.catalogName, t.schemaName)
}

# Views, inside its own namespaces only (the policy has no rule for any other schema).
# A view runs as its owner, so it would carry a tagged (PII) column past the masks every colleague
# gets: a view may not select one. Share such columns through the table, which Trino masks.
tenant_view_ops := {"CreateView", "DropView", "RenameView", "SetViewComment"}

allow if {
	input.action.operation in tenant_view_ops
	tenant
	t := input.action.resource.table
	tenant_schema(t.catalogName, t.schemaName)
	object.get(input.action, "targetResource", {"table": t}).table.schemaName in tenant.schemas
}

allow if {
	input.action.operation == "CreateViewWithSelectFromColumns"
	tenant
	t := input.action.resource.table
	tenant_schema(t.catalogName, t.schemaName)
	count(tagged_columns(t)) == 0
}

tagged_columns(t) := {c |
	some c in t.columns
	ents.column_tags[table_key(t)][c]
}

batch_tenant_columns contains i if {
	input.action.operation == "FilterColumns"
	tenant
	table := input.action.filterResources[0].table
	tenant_schema(table.catalogName, table.schemaName)
	some i, _ in table.columns
}

# ---------------------------------------------------------------------------
# Batch endpoint: Trino sends a list of resources and gets back the allowed indices.
# ---------------------------------------------------------------------------
batch contains i if {
	input.action.operation != "FilterColumns"
	some i, resource in input.action.filterResources
	allow with input.action.resource as resource
}

batch contains i if batch_tenant_columns[i]

batch contains i if {
	input.action.operation == "FilterColumns"
	table := input.action.filterResources[0].table
	some i, _ in table.columns
	allowed_schema(table.catalogName, table.schemaName)
}

# ---------------------------------------------------------------------------
# Row filters: a colleague only sees customers of the brands they serve.
# Brand is denormalised onto every customer-scoped table in silver/gold, so the
# filter is a cheap predicate that Iceberg can prune on — no joins or subqueries.
# ---------------------------------------------------------------------------
table_key(t) := concat(".", [t.schemaName, t.tableName])

rowFilters contains {"expression": expr} if {
	t := input.action.resource.table
	t.catalogName == "lakehouse"
	table_key(t) in ents.brand_scoped_tables
	not "*" in profile.brands
	quoted := [sprintf("'%s'", [b]) | some b in profile.brands]
	expr := sprintf("brand IN (%s)", [concat(", ", quoted)])
}

# Unknown users get a filter that matches nothing, as a second line of defence.
rowFilters contains {"expression": "false"} if {
	not profile
	not tenant
}

# ---------------------------------------------------------------------------
# Column masks, chosen by the column's catalog tag and the persona's clearance.
# Every mask keeps the column's type so queries and dashboards don't break.
# ---------------------------------------------------------------------------
batchColumnMasks contains {"index": i, "viewExpression": {"expression": expr}} if {
	some i, resource in input.action.filterResources
	c := resource.column
	c.catalogName == "lakehouse"
	tag := ents.column_tags[concat(".", [c.schemaName, c.tableName])][c.columnName]
	expr := mask(tag, c.columnName, lower(c.columnType))
}

typed_null(col_type) := sprintf("CAST(NULL AS %s)", [col_type])

# Special-category data (vulnerability) is visible only to personas that need it.
mask("special_category", _, col_type) := typed_null(col_type) if not persona.special_category

mask("pii.contact", col, _) := sprintf("regexp_replace(%s, '^(.).*(@.*)$', '$1***$2')", [col]) if {
	persona.pii == "partial"
	col == "email"
}

mask("pii.contact", col, _) := sprintf("concat('*******', substr(%s, -4))", [col]) if {
	persona.pii == "partial"
	col != "email"
}

mask("pii.contact", _, col_type) := typed_null(col_type) if persona.pii == "none"

mask("pii.dob", col, _) := sprintf("date_trunc('year', %s)", [col]) if persona.pii == "partial"

mask("pii.dob", _, col_type) := typed_null(col_type) if persona.pii == "none"

mask("pii.name", col, _) := sprintf("concat(substr(%s, 1, 1), '.')", [col]) if persona.pii == "none"

mask("pii.location", col, _) := sprintf("split_part(%s, ' ', 1)", [col]) if persona.pii != "full"

mask("pii.financial_id", col, _) := sprintf("concat('****', substr(%s, -4))", [col]) if persona.pii != "full"

# A whole source record serialised as text (raw change events, quarantined rows): no
# column mask can reach inside it, so it is all-or-nothing and needs full PII clearance.
mask("pii.raw_record", _, col_type) := typed_null(col_type) if persona.pii != "full"
