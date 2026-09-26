package trino_test

import rego.v1

import data.trino

ctx(user) := {"identity": {"user": user, "groups": []}}

select(user, schema, table) := {
	"context": ctx(user),
	"action": {
		"operation": "SelectFromColumns",
		"resource": {"table": {"catalogName": "lakehouse", "schemaName": schema, "tableName": table, "columns": ["customer_id"]}},
	},
}

column(user, schema, table, name, col_type) := {
	"context": ctx(user),
	"action": {
		"operation": "GetColumnMask",
		"filterResources": [{"column": {
			"catalogName": "lakehouse", "schemaName": schema, "tableName": table,
			"columnName": name, "columnType": col_type,
		}}],
	},
}

row_filter(user, schema, table) := {
	"context": ctx(user),
	"action": {
		"operation": "GetRowFilters",
		"resource": {"table": {"catalogName": "lakehouse", "schemaName": schema, "tableName": table}},
	},
}

mask_for(inp) := m if {
	some entry in trino.batchColumnMasks with input as inp
	m := entry.viewExpression.expression
}

# --- access -----------------------------------------------------------------
access_catalog(user, name) := {
	"context": ctx(user),
	"action": {"operation": "AccessCatalog", "resource": {"catalog": {"name": name}}},
}

test_known_user_accesses_lakehouse_catalog if {
	trino.allow with input as access_catalog("alice", "lakehouse")
}

test_unknown_catalog_denied if {
	not trino.allow with input as access_catalog("alice", "postgres_raw")
}

test_batch_filter_catalogs if {
	result := trino.batch with input as {
		"context": ctx("alice"),
		"action": {"operation": "FilterCatalogs", "filterResources": [
			{"catalog": {"name": "system"}}, {"catalog": {"name": "other"}}, {"catalog": {"name": "lakehouse"}},
		]},
	}
	result == {0, 2}
}

test_unknown_user_denied if {
	not trino.allow with input as {"context": ctx("mallory"), "action": {"operation": "ExecuteQuery"}}
}

test_known_user_can_query if {
	trino.allow with input as {"context": ctx("alice"), "action": {"operation": "ExecuteQuery"}}
}

test_contact_centre_reads_gold if {
	trino.allow with input as select("alice", "gold", "customer_360")
}

test_contact_centre_cannot_read_bronze if {
	not trino.allow with input as select("alice", "bronze", "customers")
}

test_analyst_cannot_read_silver if {
	not trino.allow with input as select("carol", "silver", "customers")
}

# Platform admins may inspect bronze to debug ingestion; colleagues never see it.
test_admin_reads_bronze if {
	trino.allow with input as select("ops_admin", "bronze", "cdc_events")
}

test_investigator_cannot_read_bronze if {
	not trino.allow with input as select("bob", "bronze", "cdc_events")
}

test_analyst_cannot_read_bronze if {
	not trino.allow with input as select("carol", "bronze", "customers")
}

# Bronze has no brand filter: a persona limited to some brands never sees it.
test_bronze_needs_every_brand if {
	not trino.allow with input as select("ops_admin", "bronze", "cdc_events")
		with data.entitlements.users.ops_admin.brands as ["Meridian"]
}

# Raw record payloads can't be column-masked, so they are nulled below full PII clearance.
test_raw_payload_nulled_for_admin if {
	mask_for(column("ops_admin", "bronze", "cdc_events", "payload", "varchar")) == "CAST(NULL AS varchar)"
}

test_quarantine_payload_nulled_for_admin if {
	mask_for(column("ops_admin", "ops", "quarantine", "payload", "varchar")) == "CAST(NULL AS varchar)"
}

test_raw_payload_visible_with_full_clearance if {
	not mask_for(column("bob", "bronze", "cdc_events", "payload", "varchar"))
}

test_bronze_copy_masks_pii_like_silver if {
	mask_for(column("ops_admin", "bronze", "customers", "email", "varchar")) == "CAST(NULL AS varchar)"
}

test_admin_reads_ops if {
	trino.allow with input as select("ops_admin", "ops", "dq_results")
}

test_writes_are_denied_even_for_admin if {
	not trino.allow with input as {
		"context": ctx("ops_admin"),
		"action": {"operation": "InsertIntoTable", "resource": {"table": {"catalogName": "lakehouse", "schemaName": "gold", "tableName": "customer_360"}}},
	}
}

test_impersonation_denied if {
	not trino.allow with input as {
		"context": ctx("alice"),
		"action": {"operation": "ImpersonateUser", "resource": {"user": {"user": "bob"}}},
	}
}

test_batch_filter_tables if {
	result := trino.batch with input as {
		"context": ctx("carol"),
		"action": {"operation": "FilterTables", "filterResources": [
			{"table": {"catalogName": "lakehouse", "schemaName": "gold", "tableName": "customer_360"}},
			{"table": {"catalogName": "lakehouse", "schemaName": "silver", "tableName": "customers"}},
		]},
	}
	result == {0}
}

# --- row filters --------------------------------------------------------------
test_alice_row_filter_single_brand if {
	trino.rowFilters == {{"expression": "brand IN ('Meridian')"}} with input as row_filter("alice", "gold", "customer_360")
}

test_admin_has_no_row_filter if {
	count(trino.rowFilters) == 0 with input as row_filter("ops_admin", "gold", "customer_360")
}

test_unscoped_table_has_no_row_filter if {
	count(trino.rowFilters) == 0 with input as row_filter("alice", "gold", "brand_daily_kpis")
}

# --- column masks -----------------------------------------------------------
test_partial_pii_masks_phone if {
	mask_for(column("alice", "gold", "customer_360", "phone", "varchar")) == "concat('*******', substr(phone, -4))"
}

test_full_pii_sees_phone if {
	not mask_for(column("bob", "gold", "customer_360", "phone", "varchar"))
}

test_analyst_gets_typed_null_dob if {
	mask_for(column("carol", "gold", "customer_360", "date_of_birth", "date")) == "CAST(NULL AS date)"
}

test_analyst_cannot_see_vulnerability if {
	mask_for(column("carol", "gold", "customer_360", "vulnerability_flag", "boolean")) == "CAST(NULL AS boolean)"
}

test_contact_centre_sees_vulnerability if {
	not mask_for(column("alice", "gold", "customer_360", "vulnerability_flag", "boolean"))
}

test_untagged_column_not_masked if {
	not mask_for(column("carol", "gold", "customer_360", "total_balance", "decimal(18,2)"))
}

test_metrics_scraper_reads_system_information if {
	trino.allow with input as {"context": ctx("service-account-prometheus"), "action": {"operation": "ReadSystemInformation"}}
}

test_metrics_scraper_cannot_query if {
	not trino.allow with input as {"context": ctx("service-account-prometheus"), "action": {"operation": "ExecuteQuery"}}
}

test_colleague_cannot_read_system_information if {
	not trino.allow with input as {"context": ctx("alice"), "action": {"operation": "ReadSystemInformation"}}
}
