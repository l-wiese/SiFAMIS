package neo4j;

import clusteringbasedfragmentation.Similarity;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Simple class that contains a method to query from the Neo4J-MeSH-DB pairwise shortest path lengths
 * for a certain number of terms (ordered lexicographically) and export the results as csv-File ("|"-separated),
 * e.g.
 * term1|term2|pathlength
 * term1|term3|pathlength
 * ...
 * and contains a method that reads the output path length csv-File, transforms the path lengths to similarities
 * and returns it in a HashMap (keys=term1+term2 lexicographically ordered, Values=similarity values).
 */
public class PathLengthCSV {

    /**
     * Query from the Neo4J-MeSH-DB pairwise shortest path lengths and export the results as csv-File ("|"-separated).
     *
     * @param ipAddress Database ip of the form www.xxx.yyy.zzz, optional with port number
     * @param user      Neo4J user name
     * @param password  Neo4J user password
     * @param numTerms  Number of Terms to be used for pairwise calculation
     * @param output    Path to output csv-File, e.g. "/tmp/pathlengths.csv"
     * @throws IOException If the output csv-File could not be created or written.
     */
    public static void makeCSV(String ipAddress, String user, String password, int numTerms, String output)
            throws IOException {

        System.out.println("Initializing connection to Neo4J-DB (IP=" + ipAddress + ", User=" + user + ", PW=" +
                password + ") ...");
        try (Driver driver = GraphDatabase.driver("bolt://" + ipAddress, AuthTokens.basic(user, password))) {

            System.out.println("Initialized connection!");

            // Get a session
            Session session = driver.session(AccessMode.READ);


            // Cypher query (returns pairwise path lengths of numTerms disease terms)
            StringBuilder cypher = new StringBuilder();
            cypher.append("MATCH (n:`http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor`)-")
                    .append("[:`http://id.nlm.nih.gov/mesh/vocab#treeNumber`]-(t) ")
                    .append("WHERE t.`http://www.w3.org/2000/01/rdf-schema#label` STARTS WITH 'C'\n")
                    .append("WITH DISTINCT n LIMIT ").append(numTerms).append("\n")
                    .append("WITH collect(n) as topics\n")
                    .append("UNWIND topics as n\nUNWIND topics as m\n")
                    .append("WITH * WHERE n.`http://www.w3.org/2000/01/rdf-schema#label` < ")
                    .append("m.`http://www.w3.org/2000/01/rdf-schema#label`\n")
                    .append("MATCH p = shortestPath((n)-[*]-(m)) WHERE ALL(rel in relationships(p) WHERE type(rel) in ")
                    .append("[\"http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor\"])\n")
                    .append("RETURN n.`http://www.w3.org/2000/01/rdf-schema#label`, ")
                    .append("m.`http://www.w3.org/2000/01/rdf-schema#label`, length(p);");

            // Execute and get the result stream
            Stream<Record> stream = session.run(cypher.toString()).stream();
            System.out.println("Processing the result stream ... ");

            // Write all results to CSV-File
            FileWriter csvWriter = new FileWriter(output);
            for (Iterator<Record> it = stream.iterator(); it.hasNext(); ) {
                Record r = it.next();
                csvWriter.write(r.get(0).asString() + "|" + r.get(1).asString() + "|" + r.get(2).asInt() + "\n");
                csvWriter.flush();
            }
            System.out.println("Finished processing the result stream!");
            System.out.println("Wrote " + (numTerms) * (numTerms - 1) / 2 + " path lengths to csv-File '" + output + "'");
        }
        System.out.println("Closed connection successfully");
    }


    /**
     * Read output pathlength csv-File, transform the path lengths to similarities and return them in a {@link HashMap}
     *
     * @param pathLengthCSV Path to path length csv-file
     * @return HashMap mapping pairs of disease terms to similarity values (lexicographically ordered)
     * @throws IOException
     */
    public static HashMap<String, Double> readCSVToSimilarityMap(String pathLengthCSV) throws IOException {
        HashMap<String, Double> similarities = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(pathLengthCSV));
        String line;
        while ((line = reader.readLine()) != null) {
            String parts[] = line.split("\\|");
            similarities.put(parts[0] + "+" + parts[1], 1.0 / Double.valueOf(parts[2]));
        }
        return similarities;
    }


    /**
     * Read output pathlength csv-File, transform the path lengths to {@link Similarity} objects and return them
     * as a {@link Stream}
     *
     * @param pathLengthCSV Path to path length csv-file
     * @return Stream of similarity objects
     * @throws IOException
     */
    public static Stream<Similarity> readCSVToSimilarityObjectStream(String pathLengthCSV) throws IOException {
        // Read the file linewise as a stream of strings and map each line to a similarity object
        return Files.lines(Paths.get(pathLengthCSV))
                .filter(StringUtils::isNotBlank)
                .map(line -> new Similarity(
                        line.split("\\|")[0], line.split("\\|")[1],
                        1.0 / Double.valueOf(line.split("\\|")[2])
                ));
    }


    /**
     * Read output pathlength csv-File and return the path lengths in a HashMap.
     *
     * @param pathLengthCSV Path to path length csv-file
     * @return HashMap mapping pairs of disease terms to path length values (lexicographically ordered)
     * @throws IOException
     */
    public static HashMap<String, Integer> readCSV(String pathLengthCSV) throws IOException {
        HashMap<String, Integer> pathlengths = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(pathLengthCSV));
        String line;
        while ((line = reader.readLine()) != null) {
            String parts[] = line.split("\\|");
            pathlengths.put(parts[0] + "+" + parts[1], new Integer(parts[2]));
        }
        return pathlengths;
    }


    public static HashSet<String> readTermSet(String pathLengthCSV) throws IOException {
        HashSet<String> terms = new HashSet<>();
        Files.lines(Paths.get(pathLengthCSV))
                .filter(StringUtils::isNotBlank)
                .forEach(line -> {
                    terms.add(line.split("\\|")[0]);
                    terms.add(line.split("\\|")[1]);
                });

        return terms;
    }


    /**
     * Test unit
     *
     * @param args IP, user, pw, numTerms, csv-File
     */
    public static void main(String[] args) throws IOException {
        makeCSV(args[0], args[1], args[2], Integer.valueOf(args[3]), args[4]);
    }

}
