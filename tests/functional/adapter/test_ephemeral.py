import pytest
import os
from dbt.tests.util import run_dbt, get_manifest
from dbt.tests.adapter import check_relations_equal, check_result_nodes_by_name, relation_from_name
from tests.functional.adapter.files import (
    seeds_base_csv,
    base_ephemeral_sql,
    ephemeral_view_sql,
    ephemeral_table_sql,
    schema_base_yml,
)


@pytest.fixture
def project_config_update():
    return {"name": "ephemeral"}


@pytest.fixture
def seeds():
    return {"base.csv": seeds_base_csv}


@pytest.fixture
def models():
    return {
        "ephemeral.sql": base_ephemeral_sql,
        "view_model.sql": ephemeral_view_sql,
        "table_model.sql": ephemeral_table_sql,
        "schema.yml": schema_base_yml,
    }


def test_ephemeral(project):
    # seed command
    results = run_dbt(["seed"])
    assert len(results) == 1
    check_result_nodes_by_name(results, ["base"])

    # run command
    results = run_dbt(["run"])
    assert len(results) == 2
    check_result_nodes_by_name(results, ["view_model", "table_model"])

    # base table rowcount
    relation = relation_from_name(project.adapter, "base")
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == 10

    # relations equal
    check_relations_equal(project.adapter, ["base", "view_model", "table_model"])

    # catalog node count
    catalog = run_dbt(["docs", "generate"])
    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    assert os.path.exists(catalog_path)
    assert len(catalog.nodes) == 3
    assert len(catalog.sources) == 1

    # manifest (not in original)
    manifest = get_manifest(project.project_root)
    assert len(manifest.nodes) == 4
    assert len(manifest.sources) == 1
