package mesh;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * The main method of this class can be used to query the MeSH SPARQL interface for the
 * MeSH terms (diseases) and their descriptors.
 * The output is saved to 'csv/mesh.csc'
 */
public class MeSHSPARQL {

    /**
     * Queries MeSH SPARQL interface for descriptors and disease names (terms), saves output to 'csv/mesh.csv'
     *
     * @param args Not used here
     */
    public static void main(String[] args) {

        // Setup filtered OutputStream to CSV-File
        FileOutputStream fileos = null;
        try {
            String separ = File.separator;
            String outputFile = "csv" + separ + "mesh.csv";
            fileos = new FileOutputStream(outputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        MeSHFilterOutputStream filteros = new MeSHFilterOutputStream(fileos);

        // Query
        String s = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#> " +
                "PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#> " +
                "PREFIX mesh: <http://id.nlm.nih.gov/mesh/> " +
                "PREFIX mesh2015: <http://id.nlm.nih.gov/mesh/2015/> " +
                "PREFIX mesh2016: <http://id.nlm.nih.gov/mesh/2016/> " +
                "PREFIX mesh2017: <http://id.nlm.nih.gov/mesh/2017/> " +
                "SELECT DISTINCT ?d ?name " +
                "FROM <http://id.nlm.nih.gov/mesh> " +
                "WHERE { " +
                "?d a meshv:Descriptor . " +
                "?d rdfs:label ?name . " +
                "?d meshv:treeNumber ?tn . " +
                "FILTER(REGEX(?tn,'C')) " +
                "} " +
                "ORDER BY ?d ";

        // Execute the query multiple times with an offset (max. limit is only 1000) and write result to csv
        Query query = QueryFactory.create(s);
        for (int offset = 0; ; offset += 1000) {

            // Setup query engine
            QueryEngineHTTP qe = new QueryEngineHTTP("http://id.nlm.nih.gov/mesh/sparql", query);
            qe.addParam("inference", "true");
            qe.addParam("year", "current");
            //qe.addParam("limit", "1000");       // Limit of 1000 is max value and default --> use offset instead
            qe.addParam("offset", offset + "");

            // Execute the query with the current offset & process the results
            ResultSet results = qe.execSelect();
            if (!results.hasNext()) {   // Empty?
                break;
            }
            while (results.hasNext()) {
                QuerySolution nextSol = results.next();
                try {
                    filteros.writeString(nextSol.get("d") + "|" + nextSol.get("name") + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // Flush
            try {
                filteros.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Done!");
    }

}
