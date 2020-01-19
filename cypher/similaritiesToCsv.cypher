CALL apoc.export.csv.query(
  "MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`)-[:`http://id.nlm.nih.gov/mesh/vocab#treeNumber`]-(t)
  WHERE t.`http://www.w3.org/2000/01/rdf-schema#label` STARTS WITH 'C'
  WITH collect(n) as topics
  UNWIND topics as n
  UNWIND topics as m
  WITH * WHERE n.`http://www.w3.org/2000/01/rdf-schema#label` < m.`http://www.w3.org/2000/01/rdf-schema#label`
  MATCH p = shortestPath((n)-[*]-(m))
  WHERE ALL(rel in relationships(p) WHERE type(rel) in [\"http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor\"])
  RETURN n.`http://www.w3.org/2000/01/rdf-schema#label`, m.`http://www.w3.org/2000/01/rdf-schema#label`, 1/length(p);",
  '/tmp/test.csv',
  null
);