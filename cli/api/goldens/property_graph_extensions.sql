CREATE OR REPLACE PROPERTY GRAPH `project-id.dataset-id.ExtGraph` NODE TABLES (
  `project-id.dataset-id.a` AS A KEY (id) DEFAULT LABEL OPTIONS(extensions=[("owner", "team-a"), ("tier", "gold")]) PROPERTIES (id, balance OPTIONS(extensions=[("unit", "USD")]))
)
EDGE TABLES (
  `project-id.dataset-id.a_edge` AS AtoA SOURCE KEY (src) REFERENCES A (id) DESTINATION KEY (dst) REFERENCES A (id) DEFAULT LABEL OPTIONS(extensions=[("kind", "self_loop")])
)
