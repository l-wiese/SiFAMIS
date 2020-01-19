package clusteringbasedfragmentation.similarityfunctions;

import clusteringbasedfragmentation.SimilarityException;
import neo4j.PathLengthCSV;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * <p>
 * This class implements a csv similarity loader functionality. The MeSH terms and the pairwise
 * similarity values of the MeSH terms are read from (a) file(s) and stored in a {@link HashMap}. </p>
 *
 * <p>
 * Note using this class as similarity function can cause out of memory errors (OOME) in the Apache Ignite
 * cluster, as the {@link HashMap} size becomes critical for increasing term set sizes.
 * </p>
 */
public class CSVSimilarityLoader implements MeSHSimilarityFunction, Serializable {


    private static final long serialVersionUID = -2112249481955189800L;


    /**
     * HashMap for the pairwise similarities of predefined MeSH terms (active domain here), stored with key
     * "term1+term2" and value as double
     */
    private HashMap<String, Double> similarities;

    /**
     * HashMap of all the terms (keys) occuring in the active domain of the relaxation attribute and their
     * Concept Unique Identifiers (CUIs)
     */
    private TreeSet<String> terms;

    /**
     * MeSH disease terms with their CUIs.
     */
    private HashMap<String, String> termsWithCUIs;


    /**
     * Constructor for CSVSimilarityLoader that reads terms and pairwise similarities from files
     *
     * @param termsFile        Path to file with terms (form: term1\nterm2\n...)
     * @param similaritiesFile Path to file with pairwise similarities (form: similarity<>term1(CUI1)<>term2(CUI2)\n...)
     */
    public CSVSimilarityLoader(String termsFile, String similaritiesFile) {

        // Init terms from file
        String line;
        this.terms = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(termsFile))) {
            while ((line = reader.readLine()) != null) {
                terms.add(line);    // CUI added later
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // init similarities HashMap from file (form: similarity<>term1(CUI1)<>term2(CUI2)\n...)
        this.similarities = new HashMap<>();
        this.termsWithCUIs = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(similaritiesFile))) {
            while ((line = reader.readLine()) != null) {

                // slice line by '<>' & get CUIs
                String[] things = line.split("<>");
                String CUI1 = things[1].substring(things[1].indexOf("(") + 1, things[1].length() - 1);
                String CUI2 = things[2].substring(things[2].indexOf("(") + 1, things[2].length() - 1);


                // Generate key for pairwise similarity
                String term1 = things[1].replaceAll("\\(.*\\)", "");
                String term2 = things[2].replaceAll("\\(.*\\)", "");
                this.similarities.put(term1 + "+" + term2, Double.parseDouble(things[0]));

                // Put CUI into terms HashMap
                this.termsWithCUIs.put(term1, CUI1);
                this.termsWithCUIs.put(term2, CUI2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    /**
     * Load similarities as path lengths from csv-File. Note: CUIs are not supported here!
     *
     * @param pathLengthFile Path length csv-File ("|"-separated, e.g. term1|term2|pathLength ...)
     * @throws IOException Error on file read/write
     */
    public CSVSimilarityLoader(String pathLengthFile) throws IOException {

        // Read path lengths from csv and store them as similarities in hashmap
        this.similarities = PathLengthCSV.readCSVToSimilarityMap(pathLengthFile);

        // init terms from the map
        this.terms = new TreeSet<String>();
        for (String key : similarities.keySet()) {
            String[] keySplit = key.split("\\+");
            terms.add(keySplit[0]);
            terms.add(keySplit[1]);
        }

        // init empty CUI map
        this.termsWithCUIs = new HashMap<>();
    }


    /**
     * Calculate the similarity of two MeSH terms by lookup in {@link CSVSimilarityLoader#terms}.
     *
     * @param term1 MeSH term
     * @param term2 MeSH term
     * @return Similarity value
     */
    @Override
    public double similarity(String term1, String term2) throws SimilarityException {
        Double sim;
        if ((sim = similarities.get(term1 + "+" + term2)) != null)
            return sim;
        if ((sim = similarities.get(term2 + "+" + term1)) != null)
            return sim;
        throw new SimilarityException("No similarity found for '" + term1 + "' and '" + term2 + "'!");
    }

    /**
     * Get all terms and their CUIs.
     * Note that this may be null as CUIs are not in every implementation supported!
     *
     * @return HashMap with keys=terms, values=CUIs
     */
    @Override
    public Map<String, String> getTermsWithCUIs() {
        return this.termsWithCUIs;
    }

    /**
     * Get a set of all terms.
     *
     * @return Term set
     */
    @Override
    public Set<String> getTerms() {
        return this.terms;
    }
}
