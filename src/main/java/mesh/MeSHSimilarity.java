package mesh;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * This class creates the csv files containing the term pairs for 10, 30 and 100 terms (may be changed).
 */
public class MeSHSimilarity {

    /**
     * Term set
     */
    private static HashSet<String> terms;

    /**
     * Maximal number of terms to be used for generation of the csv files
     */
    private final static int MAX_TERMS = 100;

    /**
     * Generates csv file with pairs of terms for similarity calculation, uses csv/mesh.csv as input
     *
     * @param args not used
     */
    public static void main(String[] args) {
        String separ = File.separator;
        String inputFile = "csv" + separ + "mesh.csv";
        String line = "";

        // Save csv content to HashMap and
        HashMap<String, String> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            while ((line = reader.readLine()) != null) {
                String[] s = line.split("\\|");
                map.put(s[0], s[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int numTerms = map.size();
        String[] t = new String[numTerms];
        t = map.values().toArray(t);


        // Collect 100 randomly chosen terms from array t
        terms = new HashSet<String>(MAX_TERMS);
        Random random = new Random();
        int i;
        while (terms.size() < 100) {
            i = random.nextInt(numTerms);
            terms.add(t[i]);
        }


        // Save termpairs to csv for 10, 30 and 100 (=MAX_TERMS) terms
        int[] counts = {10, 30, 100};
        String outFilePrefix = "csv" + separ + "termpairs";
        String outFileSuffix = ".csv";
        for (int c : counts)
            writeTerms(outFilePrefix, outFileSuffix, c);
        System.out.println("Done!");
    }


    /**
     * Write the first count terms of the static Set 'terms' to a csv named
     * '[outFilePrefix]count[outFileSuffix].csv]
     *
     * @param outFilePrefix Prefix of the output file
     * @param outFileSuffix Suffix of the output file
     * @param count         Number of terms
     */
    private static void writeTerms(String outFilePrefix, String outFileSuffix, int count) {
        String[] t = terms.toArray(new String[MAX_TERMS]);
        try (FileOutputStream fileout = new FileOutputStream(outFilePrefix + count + outFileSuffix)) {
            for (int i = 0; i < count; i++) {
                for (int j = i + 1; j < count; j++)
                    fileout.write((t[i] + "<>" + t[j] + "\n").getBytes(StandardCharsets.UTF_8));
                fileout.flush();
            }
            fileout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
