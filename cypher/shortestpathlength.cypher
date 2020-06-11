MATCH (n:Disease {DESCRIPTOR: "Massive Hepatic Necrosis"})
WITH n MATCH (n2:Disease {DESCRIPTOR: "Hemoptysis"})
WITH n,n2 MATCH p = shortestPath((n)-[*]-(n2))
RETURN min(length(p))
