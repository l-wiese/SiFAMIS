USING PERIODIC COMMIT
LOAD CSV FROM "file:///ctree2019MeSH.csv" AS line
CREATE (d:Disease {TREE_NUMBER:toString(line[0]),
DESCRIPTOR:toString(line[1])});
