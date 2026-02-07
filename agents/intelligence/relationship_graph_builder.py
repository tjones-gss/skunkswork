"""
Relationship Graph Builder Agent
NAM Intelligence Pipeline

Constructs and maintains a knowledge graph linking associations,
companies, events, participants, and competitor mentions.
"""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from agents.base import BaseAgent
from models.ontology import (
    EntityType,
    RelationshipType,
    GraphNode,
    GraphEdge,
    Provenance,
)
from middleware.policy import validate_json_output


class RelationshipGraphBuilderAgent(BaseAgent):
    """
    Relationship Graph Builder Agent - constructs knowledge graph.

    Responsibilities:
    - Build nodes for companies, associations, events, etc.
    - Create edges for relationships
    - Calculate network metrics
    - Support querying the graph
    """

    def _setup(self, **kwargs):
        """Initialize graph builder settings."""
        self.graph_dir = Path(self.agent_config.get("graph_dir", "data/graph"))
        self.graph_dir.mkdir(parents=True, exist_ok=True)

        # Initialize graph storage
        self.nodes = {}  # id -> GraphNode
        self.edges = []  # list of GraphEdge
        self.adjacency = defaultdict(list)  # node_id -> [(edge_id, target_id), ...]

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Build or update the relationship graph.

        Args:
            task: {
                "action": "build" | "update" | "query" | "metrics",
                "companies": [{...}, ...],  # For build/update
                "events": [{...}, ...],  # For build/update
                "participants": [{...}, ...],  # For build/update
                "signals": [{...}, ...],  # For build/update
                "query": {...},  # For query action
            }

        Returns:
            {
                "success": True,
                "nodes_created": 100,
                "edges_created": 250,
                "metrics": {...}
            }
        """
        action = task.get("action", "build")

        if action == "build":
            return await self._build_graph(task)
        elif action == "update":
            return await self._update_graph(task)
        elif action == "query":
            return await self._query_graph(task)
        elif action == "metrics":
            return await self._calculate_metrics(task)
        elif action == "export":
            return await self._export_graph(task)
        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}",
                "records_processed": 0
            }

    async def _build_graph(self, task: dict) -> dict:
        """Build graph from extracted data."""
        companies = task.get("companies", [])
        events = task.get("events", [])
        participants = task.get("participants", [])
        signals = task.get("signals", [])
        associations = task.get("associations", [])

        self.log.info(
            f"Building graph",
            companies=len(companies),
            events=len(events),
            participants=len(participants),
            signals=len(signals)
        )

        nodes_created = 0
        edges_created = 0

        # Create association nodes
        for assoc in associations:
            node = self._create_association_node(assoc)
            if node:
                self.nodes[node.id] = node
                nodes_created += 1

        # Create company nodes
        company_id_map = {}  # name -> node_id
        for company in companies:
            node = self._create_company_node(company)
            if node:
                self.nodes[node.id] = node
                company_id_map[company.get("company_name", "").lower()] = node.id
                nodes_created += 1

                # Create ASSOCIATION_HAS_MEMBER edges
                for assoc_code in company.get("associations", []):
                    assoc_node_id = f"assoc_{assoc_code}"
                    if assoc_node_id in self.nodes or True:  # Create edge anyway
                        edge = self._create_edge(
                            source_id=assoc_node_id,
                            target_id=node.id,
                            relationship_type=RelationshipType.ASSOCIATION_HAS_MEMBER,
                            properties={"association_code": assoc_code}
                        )
                        self.edges.append(edge)
                        edges_created += 1

        # Create event nodes
        event_id_map = {}  # event_id -> node_id
        for event in events:
            node = self._create_event_node(event)
            if node:
                self.nodes[node.id] = node
                event_id_map[event.get("id", "")] = node.id
                nodes_created += 1

                # Create ASSOCIATION_HOSTS_EVENT edge
                assoc_code = event.get("organizer_association")
                if assoc_code:
                    edge = self._create_edge(
                        source_id=f"assoc_{assoc_code}",
                        target_id=node.id,
                        relationship_type=RelationshipType.ASSOCIATION_HOSTS_EVENT
                    )
                    self.edges.append(edge)
                    edges_created += 1

        # Create participant relationships
        for participant in participants:
            event_id = participant.get("event_id")
            company_name = participant.get("company_name", "").lower()
            participant_type = participant.get("participant_type")

            event_node_id = event_id_map.get(event_id)
            company_node_id = company_id_map.get(company_name)

            # Create company node if doesn't exist
            if not company_node_id:
                node = self._create_company_node({
                    "company_name": participant.get("company_name"),
                    "website": participant.get("company_website")
                })
                if node:
                    self.nodes[node.id] = node
                    company_id_map[company_name] = node.id
                    company_node_id = node.id
                    nodes_created += 1

            if event_node_id and company_node_id:
                # Determine relationship type
                if participant_type == "SPONSOR":
                    rel_type = RelationshipType.EVENT_HAS_SPONSOR
                elif participant_type == "EXHIBITOR":
                    rel_type = RelationshipType.EVENT_HAS_EXHIBITOR
                else:
                    rel_type = RelationshipType.EVENT_HAS_PARTICIPANT

                edge = self._create_edge(
                    source_id=event_node_id,
                    target_id=company_node_id,
                    relationship_type=rel_type,
                    properties={
                        "participant_type": participant_type,
                        "sponsor_tier": participant.get("sponsor_tier"),
                        "booth_number": participant.get("booth_number"),
                    }
                )
                self.edges.append(edge)
                edges_created += 1

        # Create competitor signal relationships
        for signal in signals:
            company_id = signal.get("source_company_id")
            competitor_name = signal.get("competitor_normalized")

            if company_id and competitor_name:
                # Create competitor node if needed
                competitor_node_id = f"competitor_{competitor_name}"
                if competitor_node_id not in self.nodes:
                    node = GraphNode(
                        id=competitor_node_id,
                        entity_type=EntityType.COMPETITOR,
                        name=signal.get("competitor_name", competitor_name),
                        properties={"normalized_name": competitor_name}
                    )
                    self.nodes[competitor_node_id] = node
                    nodes_created += 1

                edge = self._create_edge(
                    source_id=company_id,
                    target_id=competitor_node_id,
                    relationship_type=RelationshipType.ENTITY_MENTIONED_COMPETITOR,
                    properties={
                        "signal_type": signal.get("signal_type"),
                        "confidence": signal.get("confidence"),
                    }
                )
                self.edges.append(edge)
                edges_created += 1

        # Build adjacency index
        self._build_adjacency()

        # Save graph
        await self._save_graph()

        self.log.info(
            f"Graph built",
            nodes_created=nodes_created,
            edges_created=edges_created
        )

        return {
            "success": True,
            "nodes_created": nodes_created,
            "edges_created": edges_created,
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "records_processed": nodes_created
        }

    async def _update_graph(self, task: dict) -> dict:
        """Update existing graph with new data."""
        # Load existing graph
        await self._load_graph()

        # Merge new data
        return await self._build_graph(task)

    async def _query_graph(self, task: dict) -> dict:
        """Query the relationship graph."""
        await self._load_graph()

        query = task.get("query", {})
        query_type = query.get("type")

        if query_type == "neighbors":
            return self._query_neighbors(query)
        elif query_type == "path":
            return self._query_path(query)
        elif query_type == "by_type":
            return self._query_by_type(query)
        elif query_type == "related_companies":
            return self._query_related_companies(query)
        else:
            return {
                "success": False,
                "error": f"Unknown query type: {query_type}",
                "records_processed": 0
            }

    def _query_neighbors(self, query: dict) -> dict:
        """Find neighbors of a node."""
        node_id = query.get("node_id")
        depth = query.get("depth", 1)
        relationship_types = query.get("relationship_types")

        if not node_id:
            return {"success": False, "error": "node_id required"}

        visited = set()
        neighbors = []

        def traverse(current_id, current_depth):
            if current_depth > depth or current_id in visited:
                return
            visited.add(current_id)

            for edge_id, target_id in self.adjacency.get(current_id, []):
                edge = next((e for e in self.edges if e.id == edge_id), None)
                if not edge:
                    continue

                if relationship_types and edge.relationship_type.value not in relationship_types:
                    continue

                target_node = self.nodes.get(target_id)
                if target_node and target_id not in visited:
                    neighbors.append({
                        "node": target_node.model_dump(),
                        "edge": edge.model_dump(),
                        "depth": current_depth
                    })
                    traverse(target_id, current_depth + 1)

        traverse(node_id, 1)

        return {
            "success": True,
            "neighbors": neighbors,
            "records_processed": len(neighbors)
        }

    def _query_by_type(self, query: dict) -> dict:
        """Query nodes by entity type."""
        entity_type = query.get("entity_type")

        nodes = [
            n.model_dump() for n in self.nodes.values()
            if n.entity_type.value == entity_type
        ]

        return {
            "success": True,
            "nodes": nodes,
            "count": len(nodes),
            "records_processed": len(nodes)
        }

    def _query_related_companies(self, query: dict) -> dict:
        """Find companies related through common events or associations."""
        company_id = query.get("company_id")

        if not company_id:
            return {"success": False, "error": "company_id required"}

        # Find events this company participated in
        related_events = set()
        for edge in self.edges:
            if edge.target_id == company_id and edge.relationship_type in [
                RelationshipType.EVENT_HAS_PARTICIPANT,
                RelationshipType.EVENT_HAS_SPONSOR,
                RelationshipType.EVENT_HAS_EXHIBITOR,
            ]:
                related_events.add(edge.source_id)

        # Find other companies at same events
        related_companies = {}
        for edge in self.edges:
            if edge.source_id in related_events and edge.target_id != company_id:
                target_node = self.nodes.get(edge.target_id)
                if target_node and target_node.entity_type == EntityType.COMPANY:
                    if edge.target_id not in related_companies:
                        related_companies[edge.target_id] = {
                            "node": target_node.model_dump(),
                            "shared_events": []
                        }
                    related_companies[edge.target_id]["shared_events"].append(edge.source_id)

        return {
            "success": True,
            "related_companies": list(related_companies.values()),
            "records_processed": len(related_companies)
        }

    async def _calculate_metrics(self, task: dict) -> dict:
        """Calculate network metrics."""
        await self._load_graph()

        metrics = {
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "node_types": {},
            "relationship_types": {},
            "avg_degree": 0,
            "density": 0,
        }

        # Count node types
        for node in self.nodes.values():
            node_type = node.entity_type.value
            metrics["node_types"][node_type] = metrics["node_types"].get(node_type, 0) + 1

        # Count relationship types
        for edge in self.edges:
            rel_type = edge.relationship_type.value
            metrics["relationship_types"][rel_type] = \
                metrics["relationship_types"].get(rel_type, 0) + 1

        # Calculate degree metrics
        if self.nodes:
            degrees = [len(self.adjacency.get(node_id, [])) for node_id in self.nodes]
            metrics["avg_degree"] = sum(degrees) / len(degrees)
            metrics["max_degree"] = max(degrees)
            metrics["min_degree"] = min(degrees)

        # Calculate density
        n = len(self.nodes)
        if n > 1:
            max_edges = n * (n - 1) / 2
            metrics["density"] = len(self.edges) / max_edges if max_edges > 0 else 0

        # Find most connected nodes
        degree_list = [(node_id, len(self.adjacency.get(node_id, [])))
                       for node_id in self.nodes]
        degree_list.sort(key=lambda x: x[1], reverse=True)

        metrics["top_connected"] = [
            {"id": node_id, "name": self.nodes[node_id].name, "degree": degree}
            for node_id, degree in degree_list[:10]
        ]

        return {
            "success": True,
            "metrics": metrics,
            "records_processed": 1
        }

    async def _export_graph(self, task: dict) -> dict:
        """Export graph to various formats."""
        await self._load_graph()

        export_format = task.get("format", "json")
        output_path = task.get("output_path")

        if export_format == "json":
            data = {
                "nodes": [n.model_dump() for n in self.nodes.values()],
                "edges": [e.model_dump() for e in self.edges]
            }
        elif export_format == "cytoscape":
            data = self._to_cytoscape()
        elif export_format == "gephi":
            data = self._to_gephi()
        else:
            return {"success": False, "error": f"Unknown format: {export_format}"}

        if output_path:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2, default=str)

        return {
            "success": True,
            "data": data,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "records_processed": 1
        }

    def _create_association_node(self, assoc: dict) -> Optional[GraphNode]:
        """Create a node for an association."""
        code = assoc.get("code") or assoc.get("association_code")
        if not code:
            return None

        return GraphNode(
            id=f"assoc_{code}",
            entity_type=EntityType.ASSOCIATION,
            name=assoc.get("name", code),
            properties={
                "code": code,
                "url": assoc.get("url"),
            }
        )

    def _create_company_node(self, company: dict) -> Optional[GraphNode]:
        """Create a node for a company."""
        company_name = company.get("company_name")
        if not company_name:
            return None

        node_id = company.get("id") or f"company_{hash(company_name.lower())}"

        return GraphNode(
            id=node_id,
            entity_type=EntityType.COMPANY,
            name=company_name,
            properties={
                "domain": company.get("domain"),
                "city": company.get("city"),
                "state": company.get("state"),
                "industry": company.get("industry"),
                "employee_count_min": company.get("employee_count_min"),
                "erp_system": company.get("erp_system"),
            }
        )

    def _create_event_node(self, event: dict) -> Optional[GraphNode]:
        """Create a node for an event."""
        title = event.get("title")
        if not title:
            return None

        node_id = event.get("id") or f"event_{hash(title.lower())}"

        return GraphNode(
            id=node_id,
            entity_type=EntityType.EVENT,
            name=title,
            properties={
                "event_type": event.get("event_type"),
                "start_date": event.get("start_date"),
                "city": event.get("city"),
                "state": event.get("state"),
                "organizer": event.get("organizer_association"),
            }
        )

    def _create_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_type: RelationshipType,
        properties: dict = None
    ) -> GraphEdge:
        """Create an edge between nodes."""
        return GraphEdge(
            source_id=source_id,
            target_id=target_id,
            relationship_type=relationship_type,
            properties=properties or {}
        )

    def _build_adjacency(self):
        """Build adjacency index for efficient traversal."""
        self.adjacency = defaultdict(list)

        for edge in self.edges:
            self.adjacency[edge.source_id].append((edge.id, edge.target_id))
            # For undirected traversal, also add reverse
            self.adjacency[edge.target_id].append((edge.id, edge.source_id))

    async def _save_graph(self):
        """Save graph to disk."""
        nodes_path = self.graph_dir / "nodes.json"
        edges_path = self.graph_dir / "edges.json"

        with open(nodes_path, "w") as f:
            json.dump(
                {k: v.model_dump() for k, v in self.nodes.items()},
                f,
                indent=2,
                default=str
            )

        with open(edges_path, "w") as f:
            json.dump(
                [e.model_dump() for e in self.edges],
                f,
                indent=2,
                default=str
            )

        self.log.debug(f"Graph saved to {self.graph_dir}")

    async def _load_graph(self):
        """Load graph from disk."""
        nodes_path = self.graph_dir / "nodes.json"
        edges_path = self.graph_dir / "edges.json"

        if nodes_path.exists():
            with open(nodes_path) as f:
                data = json.load(f)
                self.nodes = {k: GraphNode(**v) for k, v in data.items()}

        if edges_path.exists():
            with open(edges_path) as f:
                data = json.load(f)
                self.edges = [GraphEdge(**e) for e in data]

        self._build_adjacency()

    def _to_cytoscape(self) -> dict:
        """Convert to Cytoscape.js format."""
        elements = []

        for node in self.nodes.values():
            elements.append({
                "data": {
                    "id": node.id,
                    "label": node.name,
                    "type": node.entity_type.value,
                    **node.properties
                }
            })

        for edge in self.edges:
            elements.append({
                "data": {
                    "id": edge.id,
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "label": edge.relationship_type.value,
                    **edge.properties
                }
            })

        return {"elements": elements}

    def _to_gephi(self) -> dict:
        """Convert to Gephi-compatible format."""
        return {
            "nodes": [
                {
                    "id": node.id,
                    "label": node.name,
                    "type": node.entity_type.value,
                    "attributes": node.properties
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {
                    "id": edge.id,
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "type": edge.relationship_type.value,
                    "weight": edge.confidence,
                    "attributes": edge.properties
                }
                for edge in self.edges
            ]
        }
