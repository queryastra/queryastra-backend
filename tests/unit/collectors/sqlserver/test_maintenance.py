"""Logica pura das varreduras de manutencao SQL Server (`sqlserver/maintenance.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.collectors.sqlserver import maintenance as m


def _idx(index_id: int, keys: list[str], **kw) -> m.IndexDef:
    return m.IndexDef(
        object_id=kw.pop("object_id", 1),
        index_id=index_id,
        schema_name="dbo",
        table_name="t",
        index_name=kw.pop("name", f"ix{index_id}"),
        is_clustered=kw.pop("is_clustered", False),
        key_columns=tuple((k, False) for k in keys),
        included_columns=frozenset(kw.pop("includes", [])),
        **kw,
    )


class TestRedundancy:
    def test_exact_duplicate_drops_newer_non_unique(self) -> None:
        a, b = _idx(2, ["a", "b"]), _idx(3, ["a", "b"])
        [f] = m.find_redundant_indexes([a, b])
        assert f.category == "duplicate"
        assert f.candidate.index_id == 3 and f.keeper.index_id == 2

    def test_unique_is_kept_over_non_unique(self) -> None:
        a = _idx(2, ["a"])
        b = _idx(3, ["a"], is_unique=True)
        [f] = m.find_redundant_indexes([a, b])
        assert f.keeper.index_id == 3 and f.candidate.index_id == 2

    def test_different_includes_is_not_duplicate(self) -> None:
        a = _idx(2, ["a"], includes=["x"])
        b = _idx(3, ["a"], includes=["y"])
        assert m.find_redundant_indexes([a, b]) == []

    def test_different_filter_is_not_duplicate(self) -> None:
        a = _idx(2, ["a"], filter_definition="([x]>(1))")
        b = _idx(3, ["a"])
        assert m.find_redundant_indexes([a, b]) == []

    def test_direction_matters(self) -> None:
        a = _idx(2, ["a"])
        b = m.IndexDef(1, 3, "dbo", "t", "ix3", False, key_columns=(("a", True),))
        assert m.find_redundant_indexes([a, b]) == []

    def test_nonclustered_same_keys_as_clustered_is_duplicate(self) -> None:
        cl = _idx(1, ["id"], is_clustered=True, is_primary_key=True, is_unique=True)
        nc = _idx(2, ["id"], includes=["x"])
        [f] = m.find_redundant_indexes([cl, nc])
        assert f.candidate.index_id == 2 and f.keeper.index_id == 1

    def test_left_prefix_overlap(self) -> None:
        a = _idx(2, ["a"])
        b = _idx(3, ["a", "b"])
        [f] = m.find_redundant_indexes([a, b])
        assert f.category == "overlapping"
        assert f.candidate.index_id == 2 and f.keeper.index_id == 3

    def test_overlap_requires_includes_covered(self) -> None:
        a = _idx(2, ["a"], includes=["z"])
        b = _idx(3, ["a", "b"])
        assert m.find_redundant_indexes([a, b]) == []
        c = _idx(4, ["a", "b"], includes=["z"])
        [f] = m.find_redundant_indexes([a, c])
        assert f.candidate.index_id == 2

    def test_non_prefix_is_not_overlap(self) -> None:
        a = _idx(2, ["b"])
        b = _idx(3, ["a", "b"])
        assert m.find_redundant_indexes([a, b]) == []

    def test_different_tables_never_match(self) -> None:
        a = _idx(2, ["a"], object_id=1)
        b = _idx(2, ["a"], object_id=2)
        assert m.find_redundant_indexes([a, b]) == []

    def test_build_index_defs_orders_keys_and_splits_includes(self) -> None:
        meta = [
            {
                "object_id": 1, "index_id": 2, "schema_name": "dbo", "table_name": "t",
                "index_name": "ix", "type": 2, "type_desc": "NONCLUSTERED", "is_unique": False,
                "is_primary_key": False, "is_unique_constraint": False, "has_filter": False,
                "filter_definition": None, "size_bytes": 8192,
            }
        ]
        cols = [
            {"object_id": 1, "index_id": 2, "column_name": "b", "key_ordinal": 2,
             "is_included_column": False, "is_descending_key": True},
            {"object_id": 1, "index_id": 2, "column_name": "a", "key_ordinal": 1,
             "is_included_column": False, "is_descending_key": False},
            {"object_id": 1, "index_id": 2, "column_name": "c", "key_ordinal": 0,
             "is_included_column": True, "is_descending_key": False},
        ]
        [d] = m._build_index_defs(meta, cols)
        assert d.key_columns == (("a", False), ("b", True))
        assert d.included_columns == frozenset({"c"})
        assert d.key_text == "a, b DESC"


class TestCommandsAndSeverity:
    def test_quote_ident_escapes_bracket(self) -> None:
        assert m.quote_ident("a]b") == "[a]]b]"

    def test_fragmentation_thresholds(self) -> None:
        assert m.classify_fragmentation(45) == ("warning", "REBUILD")
        assert m.classify_fragmentation(30) == ("warning", "REBUILD")
        assert m.classify_fragmentation(12) == ("info", "REORGANIZE")
        assert m.classify_fragmentation(4.9) is None

    def test_fragmentation_command_online_hint_only_enterprise(self) -> None:
        cmd = m.fragmentation_command("db", "dbo", "t", "ix", "REBUILD", enterprise=True)
        assert cmd.startswith("ALTER INDEX [ix] ON [db].[dbo].[t] REBUILD;")
        assert "ONLINE = ON" in cmd
        assert "ONLINE" not in m.fragmentation_command("db", "dbo", "t", "ix", "REBUILD", False)
        assert m.fragmentation_command(None, "dbo", "t", "ix", "REORGANIZE", True) == (
            "ALTER INDEX [ix] ON [dbo].[t] REORGANIZE;"
        )

    def test_drop_index_command(self) -> None:
        assert m.drop_index_command("db", "s", "t", "ix") == "DROP INDEX [ix] ON [db].[s].[t];"

    def test_unused_severity(self) -> None:
        assert m.classify_unused(5000) == "warning"
        assert m.classify_unused(10) == "info"

    def test_resolve_target_databases(self) -> None:
        assert m.resolve_target_databases(["a", "b"], None) == (["a", "b"], [])
        assert m.resolve_target_databases(["a", "b"], "b") == (["b"], [])
        assert m.resolve_target_databases(["a"], "x; DROP") == ([], ["x; DROP"])

    def test_usage_notes(self) -> None:
        now = datetime(2026, 11, 10, tzinfo=UTC)
        assert m.usage_notes(now - timedelta(days=2), now) == [
            m.NOTE_USAGE_SINCE_RESTART,
            m.NOTE_USAGE_RECENT,
        ]
        assert m.usage_notes(now - timedelta(days=30), now) == [m.NOTE_USAGE_SINCE_RESTART]


class TestStatistics:
    now = datetime(2026, 11, 10, tzinfo=UTC)

    def test_small_table_ignored(self) -> None:
        assert m.assess_statistics(10, 10, None, self.now) is None

    def test_never_updated_is_stale(self) -> None:
        a = m.assess_statistics(50_000, None, None, self.now)
        assert a is not None and a.is_stale and a.severity == "warning"

    def test_percent_threshold(self) -> None:
        a = m.assess_statistics(10_000, 2_500, self.now - timedelta(days=1), self.now)
        assert a is not None and a.is_stale and a.modified_pct == 25

    def test_critical_over_half(self) -> None:
        a = m.assess_statistics(10_000, 6_000, self.now - timedelta(days=1), self.now)
        assert a is not None and a.severity == "critical"

    def test_dynamic_threshold_for_large_table(self) -> None:
        # 10M linhas: limiar dinamico ~100k (1%), bem abaixo de 20%
        a = m.assess_statistics(10_000_000, 150_000, self.now - timedelta(days=1), self.now)
        assert a is not None and a.is_stale and a.severity == "warning"

    def test_old_with_changes_is_info(self) -> None:
        a = m.assess_statistics(1_000_000, 10, self.now - timedelta(days=45), self.now)
        assert a is not None and not a.is_stale and a.severity == "info"

    def test_fresh_is_ignored(self) -> None:
        assert m.assess_statistics(1_000_000, 10, self.now - timedelta(days=2), self.now) is None

    def test_update_command_sample_for_huge_tables(self) -> None:
        assert m.update_statistics_command("db", "dbo", "t", "st", 100) == (
            "UPDATE STATISTICS [db].[dbo].[t] ([st]) WITH FULLSCAN;"
        )
        cmd = m.update_statistics_command("db", "dbo", "t", None, 50_000_000)
        assert "SAMPLE 10 PERCENT" in cmd


class TestMemoryGrants:
    def test_classify(self) -> None:
        assert m.classify_memory_query(5 * 1024 * 1024, 0, 0) == "critical"
        assert m.classify_memory_query(1024, 0, 2048 * 1024) == "critical"
        assert m.classify_memory_query(600 * 1024, 0, 0) == "warning"
        assert m.classify_memory_query(100 * 1024, 90, 0) == "warning"
        assert m.classify_memory_query(1024, 10, 8) == "warning"
        assert m.classify_memory_query(1024, 10, None) == "info"

    def test_aggregate_merges_plans_of_same_statement(self) -> None:
        base = {
            "statement_text": "SELECT * FROM t WHERE id = 42",
            "database_name": "db",
            "total_grant_kb": 1000,
            "total_used_grant_kb": 100,
            "max_grant_kb": 600,
            "max_used_grant_kb": 60,
            "max_ideal_grant_kb": 700,
            "total_spills": 2,
            "last_execution_time": datetime(2026, 11, 1),
            "last_grant_kb": 500,
        }
        rows = [
            {**base, "plan_handle": b"p1", "statement_start_offset": 0, "execution_count": 2},
            {**base, "plan_handle": b"p2", "statement_start_offset": 0, "execution_count": 3,
             "statement_text": "SELECT * FROM t WHERE id = 7"},
            # mesma linha retornada pelas 2 consultas (por grant e por spill) — nao duplica
            {**base, "plan_handle": b"p1", "statement_start_offset": 0, "execution_count": 2},
        ]
        [q] = m.aggregate_top_queries(rows)
        assert q.executions == 5
        assert q.avg_grant_kb == 400.0
        assert q.unused_grant_pct == 90.0
        assert q.spill_kb == 32  # 4 paginas * 8KB
        assert q.detail["plans"] == 2
        assert q.sql_text_masked.startswith("SELECT * FROM t")

    def test_active_grant_waiting_flag(self) -> None:
        g = m.build_active_grant(
            {"session_id": 55, "grant_time": None, "requested_memory_kb": 1024,
             "statement_text": "SELECT 1"}
        )
        assert g.is_waiting and g.session_id == "55" and g.requested_kb == 1024
        assert g.sql_hash is not None


class TestCompression:
    def test_savings_formula(self) -> None:
        assert m.compression_savings_pct(1000, 300) == 70.0
        assert m.compression_savings_pct(0, 10) is None
        assert m.compression_savings_pct(1000, None) is None

    def test_commands(self) -> None:
        assert m.compression_command("db", "dbo", "t", 1, "pk", "PAGE") == (
            "ALTER TABLE [db].[dbo].[t] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);"
        )
        assert m.compression_command("db", "dbo", "t", 3, "ix", "ROW") == (
            "ALTER INDEX [ix] ON [db].[dbo].[t] REBUILD PARTITION = ALL "
            "WITH (DATA_COMPRESSION = ROW);"
        )

    def test_supported_editions(self) -> None:
        def info(edition: int, version: str) -> m._ServerInfo:
            return m._ServerInfo(engine_edition=edition, product_version=version)

        assert m.compression_supported(info(3, "11.0.1"))
        assert m.compression_supported(info(2, "13.0.4001.0"))
        assert m.compression_supported(info(2, "15.0.2000.5"))
        assert not m.compression_supported(info(2, "13.0.1601.5"))

    def test_candidate_selection_largest_first_and_skips_compressed(self) -> None:
        rows_db1 = [
            {"schema_name": "dbo", "table_name": "big", "index_id": 1, "used_kb": 900,
             "min_compression": "NONE", "max_compression": "NONE"},
            {"schema_name": "dbo", "table_name": "done", "index_id": 1, "used_kb": 5000,
             "min_compression": "PAGE", "max_compression": "PAGE"},
        ]
        rows_db2 = [
            {"schema_name": "dbo", "table_name": "mid", "index_id": 1, "used_kb": 500,
             "min_compression": "NONE", "max_compression": "NONE"},
            {"schema_name": "dbo", "table_name": "small", "index_id": 1, "used_kb": 10,
             "min_compression": "ROW", "max_compression": "ROW"},
        ]
        selected, truncated = m.select_compression_candidates(
            [("db1", rows_db1), ("db2", rows_db2)], "PAGE", 2
        )
        assert [c.table_name for c in selected] == ["big", "mid"]
        assert truncated

    def test_build_items_aggregates_partitions(self) -> None:
        cand = m._CompressionCandidate("db", "dbo", "t", 100)
        cand.indexes = {
            1: {"index_name": "pk", "type_desc": "CLUSTERED",
                "min_compression": "NONE", "max_compression": "NONE"},
            2: {"index_name": "ix", "type_desc": "NONCLUSTERED",
                "min_compression": "PAGE", "max_compression": "PAGE"},
        }
        est = [
            {"index_id": 1, m._EST_CURRENT: 600, m._EST_REQUESTED: 200},
            {"index_id": 1, m._EST_CURRENT: 400, m._EST_REQUESTED: 100},
            {"index_id": 2, m._EST_CURRENT: 100, m._EST_REQUESTED: 100},
        ]
        [item] = m.build_compression_items(cand, est, "PAGE")
        assert item.index_id == 1 and item.current_size_kb == 1000
        assert item.savings_pct == 70.0 and item.detail["recommended"]
        assert item.command and item.command.startswith("ALTER TABLE")


class TestPartitions:
    now = datetime(2026, 11, 10, tzinfo=UTC)

    def _rows(self, last_rows: int, boundaries: list[str | None]) -> list[dict]:
        rows = []
        for n, b in enumerate(boundaries, start=1):
            rows.append(
                {
                    "schema_name": "dbo", "table_name": "fact", "index_id": 1,
                    "index_name": "pk", "index_type": "CLUSTERED", "partition_scheme": "ps",
                    "partition_function": "pf", "boundary_value_on_right": True,
                    "partition_number": n, "rows": 10 if b else last_rows,
                    "data_compression_desc": "PAGE", "filegroup_name": "FG1",
                    "partition_key": "dt", "boundary_type": "date", "boundary_value": b,
                }
            )
        return rows

    def test_rows_beyond_last_boundary_is_warning(self) -> None:
        [p] = m.summarize_partitions(
            "db", self._rows(5, ["2027-01-01 00:00:00.0000000", None]), self.now
        )
        assert p.partition_count == 2 and p.range_type == "RIGHT"
        assert p.rows_beyond_last_boundary == 5 and p.severity == "warning"
        assert p.last_boundary_value.startswith("2027-01-01")
        assert "SPLIT RANGE" in (p.recommendation or "") and "[FG1]" in (p.recommendation or "")
        assert p.total_rows == 15 and p.data_compression == "PAGE"
        assert p.detail["partitions"][-1]["boundary"] is None

    def test_near_boundary_without_rows_is_warning(self) -> None:
        [p] = m.summarize_partitions(
            None, self._rows(0, ["2026-11-12 00:00:00.0000000", None]), self.now
        )
        assert p.severity == "warning" and p.detail["reason"] == "last_boundary_near"

    def test_healthy_is_info(self) -> None:
        [p] = m.summarize_partitions(
            None, self._rows(0, ["2027-06-01 00:00:00.0000000", None]), self.now
        )
        assert p.severity == "info" and p.recommendation is None
