"""
Tests for RelationshipGraphBuilderAgent
NAM Intelligence Pipeline

Comprehensive test coverage for knowledge graph construction,
querying, metrics, export, and persistence.
"""

import json
from collections import defaultdict
from unittest.mock import AsyncMock, patch

import pytest

from middleware.secrets import _reset_secrets_manager
from models.ontology import EntityType, GraphEdge, GraphNode, RelationshipType

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def _create_graph_builder(agent_config=None, graph_dir=None):
    from agents.intelligence.relationship_graph_builder import (
        RelationshipGraphBuilderAgent,
    )

    cfg = agent_config or {}
    if graph_dir:
        cfg["graph_dir"] = str(graph_dir)
    nested = {"intelligence": {"relationship_graph_builder": cfg}}
    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = nested
        agent = RelationshipGraphBuilderAgent(
            agent_type="intelligence.relationship_graph_builder",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def builder(tmp_path):
    return _create_graph_builder(graph_dir=tmp_path / "graph")


# -- Sample data fixtures ---------------------------------------------------

SAMPLE_COMPANIES = [
    {
        "company_name": "Acme Manufacturing",
        "id": "comp-1",
        "domain": "acme.com",
        "city": "Detroit",
        "state": "MI",
        "associations": ["PMA"],
    },
    {
        "company_name": "Beta Industries",
        "id": "comp-2",
        "domain": "beta.com",
        "city": "Chicago",
        "state": "IL",
        "associations": ["PMA", "NEMA"],
    },
]

SAMPLE_EVENTS = [
    {
        "title": "FABTECH 2024",
        "id": "evt-1",
        "event_type": "TRADE_SHOW",
        "organizer_association": "PMA",
    },
    {
        "title": "Annual Meeting",
        "id": "evt-2",
        "event_type": "CONFERENCE",
        "organizer_association": "NEMA",
    },
]

SAMPLE_ASSOCIATIONS = [
    {
        "code": "PMA",
        "name": "Precision Metalforming Association",
        "url": "https://pma.org",
    },
    {"code": "NEMA", "name": "National Electrical Manufacturers Association"},
]

SAMPLE_PARTICIPANTS = [
    {
        "event_id": "evt-1",
        "company_name": "Acme Manufacturing",
        "participant_type": "SPONSOR",
        "sponsor_tier": "GOLD",
    },
    {
        "event_id": "evt-1",
        "company_name": "New Company Inc",
        "participant_type": "EXHIBITOR",
        "booth_number": "101",
    },
]

SAMPLE_SIGNALS = [
    {
        "source_company_id": "comp-1",
        "competitor_name": "SAP",
        "competitor_normalized": "sap",
        "signal_type": "MEMBER_USAGE",
        "confidence": 0.9,
    },
]


# ============================================================================
# TestInitialization
# ============================================================================


class TestInitialization:
    def test_graph_dir_created(self, tmp_path):
        graph_dir = tmp_path / "graph"
        assert not graph_dir.exists()
        _create_graph_builder(graph_dir=graph_dir)
        assert graph_dir.exists()

    def test_empty_nodes(self, builder):
        assert builder.nodes == {}

    def test_empty_edges(self, builder):
        assert builder.edges == []

    def test_empty_adjacency(self, builder):
        assert isinstance(builder.adjacency, defaultdict)
        assert len(builder.adjacency) == 0

    def test_custom_graph_dir(self, tmp_path):
        custom = tmp_path / "custom_graph"
        b = _create_graph_builder(graph_dir=custom)
        assert b.graph_dir == custom


# ============================================================================
# TestCreateAssociationNode
# ============================================================================


class TestCreateAssociationNode:
    def test_normal_case(self, builder):
        node = builder._create_association_node(
            {"code": "PMA", "name": "Precision Metalforming Association"}
        )
        assert node is not None
        assert node.id == "assoc_PMA"
        assert node.entity_type == EntityType.ASSOCIATION
        assert node.name == "Precision Metalforming Association"

    def test_code_from_code_key(self, builder):
        node = builder._create_association_node({"code": "NEMA", "name": "NEMA"})
        assert node.id == "assoc_NEMA"

    def test_code_from_association_code_key(self, builder):
        node = builder._create_association_node(
            {"association_code": "AGMA", "name": "AGMA Assoc"}
        )
        assert node is not None
        assert node.id == "assoc_AGMA"

    def test_missing_code_returns_none(self, builder):
        node = builder._create_association_node({"name": "No Code"})
        assert node is None

    def test_properties_include_code_and_url(self, builder):
        node = builder._create_association_node(
            {"code": "PMA", "name": "PMA", "url": "https://pma.org"}
        )
        assert node.properties["code"] == "PMA"
        assert node.properties["url"] == "https://pma.org"

    def test_name_defaults_to_code(self, builder):
        node = builder._create_association_node({"code": "XYZ"})
        assert node.name == "XYZ"


# ============================================================================
# TestCreateCompanyNode
# ============================================================================


class TestCreateCompanyNode:
    def test_normal_case(self, builder):
        node = builder._create_company_node(
            {"company_name": "Acme Manufacturing", "id": "comp-1"}
        )
        assert node is not None
        assert node.entity_type == EntityType.COMPANY
        assert node.name == "Acme Manufacturing"

    def test_uses_provided_id(self, builder):
        node = builder._create_company_node(
            {"company_name": "Test", "id": "my-id-123"}
        )
        assert node.id == "my-id-123"

    def test_auto_generates_id_from_hash(self, builder):
        node = builder._create_company_node({"company_name": "Acme Manufacturing"})
        expected_id = f"company_{hash('acme manufacturing')}"
        assert node.id == expected_id

    def test_missing_company_name_returns_none(self, builder):
        node = builder._create_company_node({"domain": "test.com"})
        assert node is None

    def test_properties_mapped(self, builder):
        node = builder._create_company_node(
            {
                "company_name": "Test Co",
                "id": "t1",
                "domain": "test.com",
                "city": "Detroit",
                "state": "MI",
                "industry": "Manufacturing",
                "employee_count_min": 100,
                "erp_system": "SAP",
            }
        )
        assert node.properties["domain"] == "test.com"
        assert node.properties["city"] == "Detroit"
        assert node.properties["state"] == "MI"
        assert node.properties["industry"] == "Manufacturing"
        assert node.properties["employee_count_min"] == 100
        assert node.properties["erp_system"] == "SAP"


# ============================================================================
# TestCreateEventNode
# ============================================================================


class TestCreateEventNode:
    def test_normal_case(self, builder):
        node = builder._create_event_node({"title": "FABTECH 2024", "id": "evt-1"})
        assert node is not None
        assert node.entity_type == EntityType.EVENT
        assert node.name == "FABTECH 2024"

    def test_uses_provided_id(self, builder):
        node = builder._create_event_node({"title": "Test Event", "id": "evt-99"})
        assert node.id == "evt-99"

    def test_auto_generates_id_from_hash(self, builder):
        node = builder._create_event_node({"title": "Annual Meeting"})
        expected_id = f"event_{hash('annual meeting')}"
        assert node.id == expected_id

    def test_missing_title_returns_none(self, builder):
        node = builder._create_event_node({"event_type": "CONFERENCE"})
        assert node is None

    def test_properties_mapped(self, builder):
        node = builder._create_event_node(
            {
                "title": "FABTECH",
                "id": "e1",
                "event_type": "TRADE_SHOW",
                "start_date": "2024-10-15",
                "city": "Orlando",
                "state": "FL",
                "organizer_association": "PMA",
            }
        )
        assert node.properties["event_type"] == "TRADE_SHOW"
        assert node.properties["start_date"] == "2024-10-15"
        assert node.properties["city"] == "Orlando"
        assert node.properties["state"] == "FL"
        assert node.properties["organizer"] == "PMA"


# ============================================================================
# TestCreateEdge
# ============================================================================


class TestCreateEdge:
    def test_returns_graph_edge(self, builder):
        edge = builder._create_edge(
            source_id="a",
            target_id="b",
            relationship_type=RelationshipType.ASSOCIATION_HAS_MEMBER,
        )
        assert isinstance(edge, GraphEdge)
        assert edge.source_id == "a"
        assert edge.target_id == "b"
        assert edge.relationship_type == RelationshipType.ASSOCIATION_HAS_MEMBER

    def test_properties_passed_through(self, builder):
        edge = builder._create_edge(
            source_id="a",
            target_id="b",
            relationship_type=RelationshipType.EVENT_HAS_SPONSOR,
            properties={"sponsor_tier": "GOLD"},
        )
        assert edge.properties["sponsor_tier"] == "GOLD"

    def test_default_empty_properties(self, builder):
        edge = builder._create_edge(
            source_id="a",
            target_id="b",
            relationship_type=RelationshipType.ASSOCIATION_HOSTS_EVENT,
        )
        assert edge.properties == {}


# ============================================================================
# TestBuildAdjacency
# ============================================================================


class TestBuildAdjacency:
    def test_bidirectional_edges(self, builder):
        edge = builder._create_edge(
            "src", "tgt", RelationshipType.ASSOCIATION_HAS_MEMBER
        )
        builder.edges = [edge]
        builder._build_adjacency()
        assert len(builder.adjacency["src"]) == 1
        assert len(builder.adjacency["tgt"]) == 1
        assert builder.adjacency["src"][0][1] == "tgt"
        assert builder.adjacency["tgt"][0][1] == "src"

    def test_multiple_edges(self, builder):
        e1 = builder._create_edge("a", "b", RelationshipType.ASSOCIATION_HAS_MEMBER)
        e2 = builder._create_edge("a", "c", RelationshipType.ASSOCIATION_HOSTS_EVENT)
        builder.edges = [e1, e2]
        builder._build_adjacency()
        assert len(builder.adjacency["a"]) == 2

    def test_empty_graph(self, builder):
        builder.edges = []
        builder._build_adjacency()
        assert len(builder.adjacency) == 0


# ============================================================================
# TestBuildGraph
# ============================================================================


class TestBuildGraph:
    @pytest.mark.asyncio
    async def test_companies_create_nodes_and_member_edges(self, builder):
        result = await builder._build_graph({"companies": SAMPLE_COMPANIES})
        assert result["success"] is True
        assert result["nodes_created"] == 2
        # 3 membership edges: comp-1 has PMA, comp-2 has PMA + NEMA
        assert result["edges_created"] == 3
        assert "comp-1" in builder.nodes
        assert "comp-2" in builder.nodes

    @pytest.mark.asyncio
    async def test_events_create_nodes_and_hosts_edges(self, builder):
        result = await builder._build_graph({"events": SAMPLE_EVENTS})
        assert result["success"] is True
        assert result["nodes_created"] == 2
        # 2 ASSOCIATION_HOSTS_EVENT edges
        assert result["edges_created"] == 2
        assert "evt-1" in builder.nodes
        assert "evt-2" in builder.nodes

    @pytest.mark.asyncio
    async def test_participants_with_known_company(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [SAMPLE_PARTICIPANTS[0]],  # Acme = SPONSOR
        }
        await builder._build_graph(task)
        sponsor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.EVENT_HAS_SPONSOR
        ]
        assert len(sponsor_edges) == 1
        assert sponsor_edges[0].target_id == "comp-1"

    @pytest.mark.asyncio
    async def test_participant_exhibitor_relationship(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [SAMPLE_PARTICIPANTS[1]],  # New Company = EXHIBITOR
        }
        await builder._build_graph(task)
        exhibitor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.EVENT_HAS_EXHIBITOR
        ]
        assert len(exhibitor_edges) == 1
        assert exhibitor_edges[0].properties["booth_number"] == "101"

    @pytest.mark.asyncio
    async def test_participant_default_relationship(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [
                {
                    "event_id": "evt-1",
                    "company_name": "Acme Manufacturing",
                    "participant_type": "ATTENDEE",
                }
            ],
        }
        await builder._build_graph(task)
        participant_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.EVENT_HAS_PARTICIPANT
        ]
        assert len(participant_edges) == 1

    @pytest.mark.asyncio
    async def test_auto_creates_company_for_unknown_participant(self, builder):
        task = {
            "events": SAMPLE_EVENTS,
            "participants": [SAMPLE_PARTICIPANTS[1]],  # "New Company Inc"
        }
        await builder._build_graph(task)
        # Should have auto-created a company node for "New Company Inc"
        company_nodes = [
            n
            for n in builder.nodes.values()
            if n.entity_type == EntityType.COMPANY
        ]
        assert len(company_nodes) == 1
        assert company_nodes[0].name == "New Company Inc"

    @pytest.mark.asyncio
    async def test_competitor_signals(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "signals": SAMPLE_SIGNALS,
        }
        await builder._build_graph(task)
        assert "competitor_sap" in builder.nodes
        assert builder.nodes["competitor_sap"].entity_type == EntityType.COMPETITOR
        competitor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ENTITY_MENTIONED_COMPETITOR
        ]
        assert len(competitor_edges) == 1
        assert competitor_edges[0].source_id == "comp-1"
        assert competitor_edges[0].properties["confidence"] == 0.9

    @pytest.mark.asyncio
    async def test_associations_create_nodes(self, builder):
        result = await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        assert result["nodes_created"] == 2
        assert "assoc_PMA" in builder.nodes
        assert "assoc_NEMA" in builder.nodes

    @pytest.mark.asyncio
    async def test_empty_input(self, builder):
        result = await builder._build_graph({})
        assert result["success"] is True
        assert result["nodes_created"] == 0
        assert result["edges_created"] == 0

    @pytest.mark.asyncio
    async def test_counts_correct(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "associations": SAMPLE_ASSOCIATIONS,
            "participants": SAMPLE_PARTICIPANTS,
            "signals": SAMPLE_SIGNALS,
        }
        result = await builder._build_graph(task)
        assert result["total_nodes"] == len(builder.nodes)
        assert result["total_edges"] == len(builder.edges)
        assert result["records_processed"] == result["nodes_created"]

    @pytest.mark.asyncio
    async def test_graph_saved_after_build(self, builder):
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        nodes_file = builder.graph_dir / "nodes.json"
        edges_file = builder.graph_dir / "edges.json"
        assert nodes_file.exists()
        assert edges_file.exists()

    @pytest.mark.asyncio
    async def test_duplicate_competitor_node_not_created(self, builder):
        signals = [
            {
                "source_company_id": "comp-1",
                "competitor_name": "SAP",
                "competitor_normalized": "sap",
                "signal_type": "MEMBER_USAGE",
                "confidence": 0.9,
            },
            {
                "source_company_id": "comp-2",
                "competitor_name": "SAP",
                "competitor_normalized": "sap",
                "signal_type": "EXHIBITOR",
                "confidence": 0.8,
            },
        ]
        task = {"companies": SAMPLE_COMPANIES, "signals": signals}
        await builder._build_graph(task)
        # Only one competitor node for "sap"
        competitor_nodes = [
            n
            for n in builder.nodes.values()
            if n.entity_type == EntityType.COMPETITOR
        ]
        assert len(competitor_nodes) == 1
        # But two edges
        competitor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ENTITY_MENTIONED_COMPETITOR
        ]
        assert len(competitor_edges) == 2


# ============================================================================
# TestUpdateGraph
# ============================================================================


class TestUpdateGraph:
    @pytest.mark.asyncio
    async def test_loads_existing_then_builds(self, builder):
        # First build some data
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        assert len(builder.nodes) == 2

        # Update with more data
        result = await builder._update_graph({"companies": SAMPLE_COMPANIES})
        assert result["success"] is True
        # Should have association nodes from load + new company nodes
        assert len(builder.nodes) >= 2

    @pytest.mark.asyncio
    async def test_calls_load_and_build(self, builder):
        with (
            patch.object(builder, "_load_graph", new_callable=AsyncMock) as mock_load,
            patch.object(
                builder,
                "_build_graph",
                new_callable=AsyncMock,
                return_value={"success": True},
            ) as mock_build,
        ):
            task = {"companies": SAMPLE_COMPANIES}
            await builder._update_graph(task)
            mock_load.assert_awaited_once()
            mock_build.assert_awaited_once_with(task)


# ============================================================================
# TestQueryNeighbors
# ============================================================================


class TestQueryNeighbors:
    @pytest.mark.asyncio
    async def test_finds_direct_neighbors(self, builder):
        await builder._build_graph(
            {
                "associations": SAMPLE_ASSOCIATIONS,
                "companies": SAMPLE_COMPANIES,
            }
        )
        result = builder._query_neighbors({"node_id": "assoc_PMA", "depth": 1})
        assert result["success"] is True
        assert len(result["neighbors"]) > 0

    @pytest.mark.asyncio
    async def test_respects_depth(self, builder):
        await builder._build_graph(
            {
                "associations": SAMPLE_ASSOCIATIONS,
                "companies": SAMPLE_COMPANIES,
                "events": SAMPLE_EVENTS,
                "participants": SAMPLE_PARTICIPANTS,
            }
        )
        result_d1 = builder._query_neighbors({"node_id": "assoc_PMA", "depth": 1})
        result_d2 = builder._query_neighbors({"node_id": "assoc_PMA", "depth": 2})
        assert len(result_d2["neighbors"]) >= len(result_d1["neighbors"])

    @pytest.mark.asyncio
    async def test_filters_by_relationship_types(self, builder):
        await builder._build_graph(
            {
                "associations": SAMPLE_ASSOCIATIONS,
                "companies": SAMPLE_COMPANIES,
                "events": SAMPLE_EVENTS,
            }
        )
        result = builder._query_neighbors(
            {
                "node_id": "assoc_PMA",
                "depth": 1,
                "relationship_types": ["ASSOCIATION_HOSTS_EVENT"],
            }
        )
        for neighbor in result["neighbors"]:
            assert (
                neighbor["edge"]["relationship_type"] == "ASSOCIATION_HOSTS_EVENT"
            )

    def test_missing_node_id(self, builder):
        result = builder._query_neighbors({})
        assert result["success"] is False
        assert "node_id required" in result["error"]

    @pytest.mark.asyncio
    async def test_node_with_no_neighbors(self, builder):
        # Create an isolated node
        builder.nodes["isolated"] = GraphNode(
            id="isolated", entity_type=EntityType.COMPANY, name="Isolated Co"
        )
        builder._build_adjacency()
        result = builder._query_neighbors({"node_id": "isolated", "depth": 1})
        assert result["success"] is True
        assert len(result["neighbors"]) == 0


# ============================================================================
# TestQueryByType
# ============================================================================


class TestQueryByType:
    @pytest.mark.asyncio
    async def test_returns_matching_nodes(self, builder):
        await builder._build_graph(
            {
                "companies": SAMPLE_COMPANIES,
                "associations": SAMPLE_ASSOCIATIONS,
            }
        )
        result = builder._query_by_type({"entity_type": "Company"})
        assert result["success"] is True
        assert result["count"] == 2
        for n in result["nodes"]:
            assert n["entity_type"] == "Company"

    @pytest.mark.asyncio
    async def test_no_matching_nodes(self, builder):
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        result = builder._query_by_type({"entity_type": "Company"})
        assert result["success"] is True
        assert result["count"] == 0
        assert result["nodes"] == []


# ============================================================================
# TestQueryRelatedCompanies
# ============================================================================


class TestQueryRelatedCompanies:
    @pytest.mark.asyncio
    async def test_finds_companies_sharing_events(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [
                {
                    "event_id": "evt-1",
                    "company_name": "Acme Manufacturing",
                    "participant_type": "SPONSOR",
                },
                {
                    "event_id": "evt-1",
                    "company_name": "Beta Industries",
                    "participant_type": "EXHIBITOR",
                },
            ],
        }
        await builder._build_graph(task)
        result = builder._query_related_companies({"company_id": "comp-1"})
        assert result["success"] is True
        assert len(result["related_companies"]) >= 1

    @pytest.mark.asyncio
    async def test_tracks_shared_event_ids(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [
                {
                    "event_id": "evt-1",
                    "company_name": "Acme Manufacturing",
                    "participant_type": "SPONSOR",
                },
                {
                    "event_id": "evt-1",
                    "company_name": "Beta Industries",
                    "participant_type": "EXHIBITOR",
                },
            ],
        }
        await builder._build_graph(task)
        result = builder._query_related_companies({"company_id": "comp-1"})
        for rc in result["related_companies"]:
            assert len(rc["shared_events"]) > 0

    def test_missing_company_id(self, builder):
        result = builder._query_related_companies({})
        assert result["success"] is False
        assert "company_id required" in result["error"]

    @pytest.mark.asyncio
    async def test_no_related_companies(self, builder):
        await builder._build_graph({"companies": SAMPLE_COMPANIES})
        result = builder._query_related_companies({"company_id": "comp-1"})
        assert result["success"] is True
        assert len(result["related_companies"]) == 0


# ============================================================================
# TestQueryPath
# ============================================================================


class TestQueryPath:
    @pytest.mark.asyncio
    async def test_query_path_not_implemented(self, builder):
        """_query_path is referenced in routing but NOT defined on the class."""
        assert not hasattr(builder, "_query_path")

    @pytest.mark.asyncio
    async def test_query_path_raises_attribute_error(self, builder):
        # Save empty graph first so _load_graph succeeds
        await builder._save_graph()
        with pytest.raises(AttributeError):
            await builder.run({"action": "query", "query": {"type": "path"}})


# ============================================================================
# TestCalculateMetrics
# ============================================================================


class TestCalculateMetrics:
    @pytest.mark.asyncio
    async def test_node_and_edge_count(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert metrics["node_count"] == len(builder.nodes)
        assert metrics["edge_count"] == len(builder.edges)

    @pytest.mark.asyncio
    async def test_node_type_distribution(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert metrics["node_types"]["Company"] == 2
        assert metrics["node_types"]["Association"] == 2

    @pytest.mark.asyncio
    async def test_relationship_type_distribution(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert "ASSOCIATION_HAS_MEMBER" in metrics["relationship_types"]

    @pytest.mark.asyncio
    async def test_degree_metrics(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert metrics["avg_degree"] > 0
        assert "max_degree" in metrics
        assert "min_degree" in metrics
        assert metrics["max_degree"] >= metrics["min_degree"]

    @pytest.mark.asyncio
    async def test_density_calculation(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert 0 <= metrics["density"] <= 1

    @pytest.mark.asyncio
    async def test_top_connected_nodes(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert "top_connected" in metrics
        assert len(metrics["top_connected"]) > 0
        for entry in metrics["top_connected"]:
            assert "id" in entry
            assert "name" in entry
            assert "degree" in entry

    @pytest.mark.asyncio
    async def test_empty_graph_metrics(self, builder):
        # Empty graph - save first so load works
        await builder._save_graph()
        result = await builder._calculate_metrics({})
        metrics = result["metrics"]
        assert metrics["node_count"] == 0
        assert metrics["edge_count"] == 0
        assert metrics["avg_degree"] == 0
        assert metrics["density"] == 0


# ============================================================================
# TestExportGraph
# ============================================================================


class TestExportGraph:
    @pytest.mark.asyncio
    async def test_json_format(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._export_graph({"format": "json"})
        assert result["success"] is True
        data = result["data"]
        assert "nodes" in data
        assert "edges" in data
        assert len(data["nodes"]) == len(builder.nodes)
        assert len(data["edges"]) == len(builder.edges)

    @pytest.mark.asyncio
    async def test_cytoscape_format(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._export_graph({"format": "cytoscape"})
        assert result["success"] is True
        data = result["data"]
        assert "elements" in data

    @pytest.mark.asyncio
    async def test_gephi_format(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._export_graph({"format": "gephi"})
        assert result["success"] is True
        data = result["data"]
        assert "nodes" in data
        assert "edges" in data

    @pytest.mark.asyncio
    async def test_unknown_format(self, builder):
        await builder._build_graph({"companies": SAMPLE_COMPANIES})
        result = await builder._export_graph({"format": "xml"})
        assert result["success"] is False
        assert "Unknown format" in result["error"]

    @pytest.mark.asyncio
    async def test_output_to_file(self, builder, tmp_path):
        await builder._build_graph({"companies": SAMPLE_COMPANIES})
        output = tmp_path / "export.json"
        result = await builder._export_graph(
            {"format": "json", "output_path": str(output)}
        )
        assert result["success"] is True
        assert output.exists()
        with open(output) as f:
            exported = json.load(f)
        assert "nodes" in exported
        assert "edges" in exported


# ============================================================================
# TestSaveAndLoadGraph
# ============================================================================


class TestSaveAndLoadGraph:
    @pytest.mark.asyncio
    async def test_save_creates_files(self, builder):
        builder.nodes["n1"] = GraphNode(
            id="n1", entity_type=EntityType.COMPANY, name="Test"
        )
        await builder._save_graph()
        assert (builder.graph_dir / "nodes.json").exists()
        assert (builder.graph_dir / "edges.json").exists()

    @pytest.mark.asyncio
    async def test_load_from_empty_dir(self, builder):
        await builder._load_graph()
        assert builder.nodes == {}
        assert builder.edges == []

    @pytest.mark.asyncio
    async def test_round_trip_preserves_nodes(self, builder):
        builder.nodes["n1"] = GraphNode(
            id="n1",
            entity_type=EntityType.COMPANY,
            name="Test Co",
            properties={"city": "Detroit"},
        )
        await builder._save_graph()

        # Clear and reload
        builder.nodes = {}
        await builder._load_graph()
        assert "n1" in builder.nodes
        assert builder.nodes["n1"].name == "Test Co"
        assert builder.nodes["n1"].properties["city"] == "Detroit"

    @pytest.mark.asyncio
    async def test_round_trip_preserves_edges(self, builder):
        edge = builder._create_edge(
            "a",
            "b",
            RelationshipType.ASSOCIATION_HAS_MEMBER,
            properties={"code": "PMA"},
        )
        builder.edges = [edge]
        await builder._save_graph()

        builder.edges = []
        await builder._load_graph()
        assert len(builder.edges) == 1
        assert builder.edges[0].source_id == "a"
        assert builder.edges[0].target_id == "b"
        assert builder.edges[0].relationship_type == RelationshipType.ASSOCIATION_HAS_MEMBER
        assert builder.edges[0].properties["code"] == "PMA"

    @pytest.mark.asyncio
    async def test_load_rebuilds_adjacency(self, builder):
        edge = builder._create_edge(
            "a", "b", RelationshipType.ASSOCIATION_HAS_MEMBER
        )
        builder.nodes["a"] = GraphNode(
            id="a", entity_type=EntityType.ASSOCIATION, name="A"
        )
        builder.nodes["b"] = GraphNode(
            id="b", entity_type=EntityType.COMPANY, name="B"
        )
        builder.edges = [edge]
        await builder._save_graph()

        builder.adjacency = defaultdict(list)
        await builder._load_graph()
        assert len(builder.adjacency["a"]) == 1
        assert len(builder.adjacency["b"]) == 1


# ============================================================================
# TestCytoscapeFormat
# ============================================================================


class TestCytoscapeFormat:
    @pytest.mark.asyncio
    async def test_nodes_have_required_fields(self, builder):
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        data = builder._to_cytoscape()
        node_elements = [
            e for e in data["elements"] if "source" not in e["data"]
        ]
        for ne in node_elements:
            assert "id" in ne["data"]
            assert "label" in ne["data"]
            assert "type" in ne["data"]

    @pytest.mark.asyncio
    async def test_edges_have_required_fields(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        data = builder._to_cytoscape()
        edge_elements = [
            e for e in data["elements"] if "source" in e["data"]
        ]
        for ee in edge_elements:
            assert "source" in ee["data"]
            assert "target" in ee["data"]
            assert "label" in ee["data"]

    @pytest.mark.asyncio
    async def test_properties_spread_into_data(self, builder):
        await builder._build_graph(
            {
                "associations": [
                    {"code": "PMA", "name": "PMA", "url": "https://pma.org"}
                ]
            }
        )
        data = builder._to_cytoscape()
        node_data = data["elements"][0]["data"]
        assert node_data["code"] == "PMA"
        assert node_data["url"] == "https://pma.org"


# ============================================================================
# TestGephiFormat
# ============================================================================


class TestGephiFormat:
    @pytest.mark.asyncio
    async def test_nodes_have_required_fields(self, builder):
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        data = builder._to_gephi()
        for node in data["nodes"]:
            assert "id" in node
            assert "label" in node
            assert "type" in node
            assert "attributes" in node

    @pytest.mark.asyncio
    async def test_edges_have_required_fields(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        data = builder._to_gephi()
        for edge in data["edges"]:
            assert "id" in edge
            assert "source" in edge
            assert "target" in edge
            assert "type" in edge
            assert "weight" in edge
            assert "attributes" in edge


# ============================================================================
# TestRunRouting
# ============================================================================


class TestRunRouting:
    @pytest.mark.asyncio
    async def test_build_action(self, builder):
        with patch.object(
            builder,
            "_build_graph",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({"action": "build"})
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_action(self, builder):
        with patch.object(
            builder,
            "_update_graph",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({"action": "update"})
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_query_action(self, builder):
        with patch.object(
            builder,
            "_query_graph",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({"action": "query", "query": {"type": "by_type"}})
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_metrics_action(self, builder):
        with patch.object(
            builder,
            "_calculate_metrics",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({"action": "metrics"})
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_export_action(self, builder):
        with patch.object(
            builder,
            "_export_graph",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({"action": "export"})
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unknown_action(self, builder):
        result = await builder.run({"action": "invalid_action"})
        assert result["success"] is False
        assert "Unknown action" in result["error"]

    @pytest.mark.asyncio
    async def test_default_action_is_build(self, builder):
        with patch.object(
            builder,
            "_build_graph",
            new_callable=AsyncMock,
            return_value={"success": True},
        ) as mock:
            await builder.run({})
            mock.assert_awaited_once()


# ============================================================================
# TestQueryGraphRouting
# ============================================================================


class TestQueryGraphRouting:
    @pytest.mark.asyncio
    async def test_unknown_query_type(self, builder):
        # Save empty graph so load works
        await builder._save_graph()
        result = await builder._query_graph(
            {"query": {"type": "unknown_query"}}
        )
        assert result["success"] is False
        assert "Unknown query type" in result["error"]

    @pytest.mark.asyncio
    async def test_query_neighbors_routing(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._query_graph(
            {"query": {"type": "neighbors", "node_id": "assoc_PMA", "depth": 1}}
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_query_by_type_routing(self, builder):
        await builder._build_graph({"associations": SAMPLE_ASSOCIATIONS})
        result = await builder._query_graph(
            {"query": {"type": "by_type", "entity_type": "Association"}}
        )
        assert result["success"] is True
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_query_related_companies_routing(self, builder):
        task = {
            "companies": SAMPLE_COMPANIES,
            "events": SAMPLE_EVENTS,
            "participants": [
                {
                    "event_id": "evt-1",
                    "company_name": "Acme Manufacturing",
                    "participant_type": "SPONSOR",
                },
                {
                    "event_id": "evt-1",
                    "company_name": "Beta Industries",
                    "participant_type": "EXHIBITOR",
                },
            ],
        }
        await builder._build_graph(task)
        result = await builder._query_graph(
            {"query": {"type": "related_companies", "company_id": "comp-1"}}
        )
        assert result["success"] is True


# ============================================================================
# TestEdgeCases
# ============================================================================


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_signal_without_company_id_skipped(self, builder):
        signals = [
            {
                "competitor_name": "SAP",
                "competitor_normalized": "sap",
                "signal_type": "MEMBER_USAGE",
                "confidence": 0.9,
            }
        ]
        await builder._build_graph({"signals": signals})
        # No edge created because source_company_id is missing
        competitor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ENTITY_MENTIONED_COMPETITOR
        ]
        assert len(competitor_edges) == 0

    @pytest.mark.asyncio
    async def test_signal_without_competitor_normalized_skipped(self, builder):
        signals = [
            {
                "source_company_id": "comp-1",
                "competitor_name": "SAP",
                "signal_type": "MEMBER_USAGE",
                "confidence": 0.9,
            }
        ]
        await builder._build_graph({"signals": signals})
        competitor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ENTITY_MENTIONED_COMPETITOR
        ]
        assert len(competitor_edges) == 0

    @pytest.mark.asyncio
    async def test_participant_without_event_id(self, builder):
        participants = [
            {
                "company_name": "Acme Manufacturing",
                "participant_type": "SPONSOR",
            }
        ]
        task = {"companies": SAMPLE_COMPANIES, "participants": participants}
        await builder._build_graph(task)
        sponsor_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.EVENT_HAS_SPONSOR
        ]
        # No event_node_id match, so no edge
        assert len(sponsor_edges) == 0

    @pytest.mark.asyncio
    async def test_event_without_organizer_association(self, builder):
        events = [{"title": "Independent Event", "id": "evt-ind"}]
        await builder._build_graph({"events": events})
        assert "evt-ind" in builder.nodes
        # No ASSOCIATION_HOSTS_EVENT edge created
        host_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ASSOCIATION_HOSTS_EVENT
        ]
        assert len(host_edges) == 0

    @pytest.mark.asyncio
    async def test_company_without_associations(self, builder):
        companies = [
            {"company_name": "Solo Corp", "id": "solo-1"}
        ]
        await builder._build_graph({"companies": companies})
        assert "solo-1" in builder.nodes
        member_edges = [
            e
            for e in builder.edges
            if e.relationship_type == RelationshipType.ASSOCIATION_HAS_MEMBER
        ]
        assert len(member_edges) == 0

    @pytest.mark.asyncio
    async def test_density_with_single_node(self, builder):
        builder.nodes["only"] = GraphNode(
            id="only", entity_type=EntityType.COMPANY, name="Only"
        )
        await builder._save_graph()
        result = await builder._calculate_metrics({})
        # n=1, density should be 0
        assert result["metrics"]["density"] == 0

    @pytest.mark.asyncio
    async def test_export_includes_counts(self, builder):
        await builder._build_graph(
            {"companies": SAMPLE_COMPANIES, "associations": SAMPLE_ASSOCIATIONS}
        )
        result = await builder._export_graph({"format": "json"})
        assert result["node_count"] == len(builder.nodes)
        assert result["edge_count"] == len(builder.edges)
        assert result["records_processed"] == 1
