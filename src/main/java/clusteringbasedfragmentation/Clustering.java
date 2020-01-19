package clusteringbasedfragmentation;

import clusteringbasedfragmentation.similarityfunctions.CSVSimilarityLoader;
import clusteringbasedfragmentation.similarityfunctions.MeSHSimilarityFunction;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * This class represents a clustering of the MeSH terms.
 */
public class Clustering implements Serializable, Iterable<Cluster<String>> {

    private static final long serialVersionUID = 3726253629878428967L;

    /**
     * The clusters obtained from the clustering algorithm (will be initialized in constructor). Also provides a
     * mapping of each cluster to a partition with utilizing the index of the cluster in this ArrayList.
     */
    private ArrayList<Cluster<String>> clusters;

    /**
     * Similarity threshold for clustering algorithm and affinity key mapping.
     */
    private double alpha;

    /**
     * Similarity function
     */
    private transient MeSHSimilarityFunction similarityFunction;


//######################################### Constructor ######################################################

    /**
     * Constructor for empty clustering, no clustering is constructed.
     */
    public Clustering() {
        this.clusters = null;
        this.alpha = 0.0;
        this.similarityFunction = null;
    }

    /**
     * <p>
     * This constructs the clustering of the active domain of the relaxation
     * attribute in a table. All the values of the active domain of the relaxation
     * attribute (column) are assigned to a cluster.
     * (Note: do not mix up with the cluster of nodes storing data! Here clusters
     * are the partitions). The resulting assignment is a list of clusters. </p>
     * <p>
     * Also the provided similarity function must be able to deliver the active domain
     * correctly in order to perform the clustering process. </p>
     * <p>
     * For details see {@link Cluster} </p>
     */
    public Clustering(double alpha, MeSHSimilarityFunction similarityFunction) throws SimilarityException {

        this.alpha = alpha;
        this.similarityFunction = similarityFunction;


        System.out.println("Starting the clustering procedure ...");
        long time = System.nanoTime();

        ArrayList<String> activeDomain = new ArrayList<>(similarityFunction.getTerms());

        if (activeDomain.isEmpty())
            throw new IllegalArgumentException("The active domain is empty.");

        // Initialize first cluster with all values from the active domain (except head)
        clusters = new ArrayList<>();
        String headElement = activeDomain.remove(0);
        Cluster<String> c = new Cluster<>(headElement, new HashSet<>(activeDomain));
        clusters.add(0, c);

        // Initial minimal similarity for next head
        double sim_min = 1.0;
        for (String term : c.getAdom()) {       // NOTE: adom does not contain head!
            double sim = similarityFunction.similarity(term, headElement);
            if (sim < sim_min)
                sim_min = sim;
        }

        // Clustering procedure
        int i = 0;
        while (sim_min < alpha) {

            // Get element with smallest similarity to any cluster head (argmin)
            String nextHead = null;
            for (int j = 0; j <= i; j++) {
                c = clusters.get(j);
                String head_j = c.getHead();
                Set<String> adom = c.getAdom();
                for (String term : adom) {
                    if (similarityFunction.similarity(term, head_j) == sim_min) {
                        nextHead = term;
                        adom.remove(term);      // remove new found head
                        break;
                    }
                }
                // Already found next head?
                if (nextHead != null)
                    break;
            }

            // Create next cluster
            HashSet<String> nextAdom = new HashSet<>();
            for (int j = 0; j <= i; j++) {

                // Get head and adom
                c = clusters.get(j);
                String head_j = c.getHead();
                HashSet<String> adom_j = new HashSet<>(c.getAdom());

                // Move terms from old cluster's adom to
                // new cluster's adom if they are more similar to nextHead
                Iterator<String> it = adom_j.iterator();
                while (it.hasNext()) {
                    String term = it.next();
                    if (similarityFunction.similarity(term, head_j) <= similarityFunction.similarity(term, nextHead)) {
                        // remove from current cluster's adom & add to new one
                        it.remove();
                        nextAdom.add(term);
                    }
                }

                // Set (possibly modified) adom_j for old cluster
                c.setAdom(adom_j);
            }

            // Add the new cluster to the clustering
            i = i + 1;
            c = new Cluster<>(nextHead, nextAdom);
            clusters.add(i, c);

            // Update minimal similarity
            double min = 1;
            for (int j = 0; j <= i; j++) {

                c = clusters.get(j);
                String head_j = c.getHead();
                for (String term : c.getAdom()) {
                    double s = similarityFunction.similarity(term, head_j);
                    if (min >= s)
                        min = s;
                }
            }
            sim_min = min;
        }

        time = System.nanoTime() - time;
        System.out.println("Finished clustering in " + time / 1000000000.0 + " seconds!");
    }


    //####################################### Getter & Setter ##################################################

    /**
     * Get the clusters
     *
     * @return List of clusters
     */
    public ArrayList<Cluster<String>> getClusters() {
        return clusters;
    }

    /**
     * Overwrite list of clusters with given list of clusters.
     * (Should be set in addition to setting a new similarity threshold (alpha))
     *
     * @param clusters List of clusters
     * @return {@code This} for chaining
     */
    public Clustering setClusters(ArrayList<Cluster<String>> clusters) {
        this.clusters = clusters;
        return this;
    }

    /**
     * Get similarity threshold
     *
     * @return similarity threshold
     */
    public double getAlpha() {
        return alpha;
    }

    /**
     * Set similarity threshold (should be set in addition to setting new clusters)
     *
     * @param alpha Similarity threshold
     * @return {@code This} for chaining
     */
    public Clustering setAlpha(double alpha) {
        this.alpha = alpha;
        return this;
    }

    /**
     * Get the similarity function
     *
     * @return Similarity function
     */
    public MeSHSimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    /**
     * Set a new similarity function.
     *
     * @param similarityFunction Similarity function
     * @return {@code This} for chaining
     */
    public Clustering setSimilarityFunction(MeSHSimilarityFunction similarityFunction) {
        this.similarityFunction = similarityFunction;
        return this;
    }

    /**
     * Get i-th cluster
     *
     * @param i Index of the cluster
     * @return Cluster
     */
    public Cluster<String> getCluster(int i) {
        return clusters.get(i);
    }

    /**
     * Get i-th cluster's head term
     *
     * @param i Index of cluster
     * @return Head of cluster i
     */
    public String getHead(int i) {
        return getCluster(i).getHead();
    }

//############################################# Utils #######################################################

    /**
     * Size of the Clustering
     *
     * @return Number of clusters
     */
    public int size() {
        return this.getClusters().size();
    }

    /**
     * Print statistics of the clustering like avg. terms per cluster and maximal/minimal cluster size.
     */
    public void printClusteringStatistics() {
        System.out.println("##### Statistics for the clustering:");

        // Alpha, number of terms, number of clusters, avg. terms/cluster
        System.out.println(" - Similarity-threshold alpha: " + this.alpha);
        int numTerms = this.similarityFunction.getTerms().size();
        System.out.println(" - Number of terms: " + numTerms);
        System.out.println(" - Clustering size: " + this.clusters.size());
        System.out.println(" - Avg. Terms/Cluster: " + numTerms / clusters.size());

        // Minimal/maximal cluster size
        int min = numTerms;
        int max = 1;
        for (Cluster c : this) {
            int size = c.getAdom().size() + 1;      // + 1 for head element
            if (size < min)
                min = size;
            if (size > max)
                max = size;
        }
        System.out.println(" - Minimal cluster size: " + min);
        System.out.println(" - Maximal cluster size: " + max);

        System.out.println("#####\n");
    }

    /**
     * Get string representation of the clustering
     *
     * @return String representation
     */
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Clustering: Alpha = " + alpha + ", Size = " + this.size() + "\n");
        clusters.forEach(s::append);
        return s.toString();
    }

//######################################### Serialization ####################################################

    /**
     * Serialize clustering to a file.
     *
     * @param file Path to output file
     */
    public void serializeToFile(String file) {
        try (ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(file))) {
            objectOut.writeObject(this);
            System.out.println("Wrote clustering to file '" + file + "'!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Deserialize a clustering from a file
     *
     * @param file Path to input file
     * @return Clustering that was read from file
     */
    public static Clustering deserializeFromFile(String file) throws IOException, ClassNotFoundException {
        Clustering clustering;
        try (ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(file))) {
            clustering = (Clustering) objectIn.readObject();
        }
        System.out.println("Read clustering from file '" + file + "'!");
        return clustering;
    }


// ######################################## Iterable #######################################################

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @NotNull
    @Override
    public Iterator<Cluster<String>> iterator() {
        return getClusters().iterator();
    }

    /**
     * Performs the given action for each element of the {@code Iterable}
     * until all elements have been processed or the action throws an
     * exception.  Actions are performed in the order of iteration, if that
     * order is specified.  Exceptions thrown by the action are relayed to the
     * caller.
     * <p>
     * The behavior of this method is unspecified if the action performs
     * side-effects that modify the underlying source of elements, unless an
     * overriding class has specified a concurrent modification policy.
     *
     * @param action The action to be performed for each element
     * @throws NullPointerException if the specified action is null
     * @implSpec <p>The default implementation behaves as if:
     * <pre>{@code
     *     for (T t : this)
     *         action.accept(t);
     * }</pre>
     * @since 1.8
     */
    @Override
    public void forEach(Consumer<? super Cluster<String>> action) {
        for (Cluster<String> s : this) {
            action.accept(s);
        }
    }

    /**
     * Creates a {@link Spliterator} over the elements described by this
     * {@code Iterable}.
     *
     * @return a {@code Spliterator} over the elements described by this
     * {@code Iterable}.
     * @implSpec The default implementation creates an
     * <em><a href="../util/Spliterator.html#binding">early-binding</a></em>
     * spliterator from the iterable's {@code Iterator}.  The spliterator
     * inherits the <em>fail-fast</em> properties of the iterable's iterator.
     * @implNote The default implementation should usually be overridden.  The
     * spliterator returned by the default implementation has poor splitting
     * capabilities, is unsized, and does not report any spliterator
     * characteristics. Implementing classes can nearly always provide a
     * better implementation.
     * @since 1.8
     */
    @Override
    public Spliterator<Cluster<String>> spliterator() {
        return getClusters().spliterator();
    }


// ######################################## Main ##################################################

    /**
     * Initialize all the clustering serializations (500, 1000, 2500 and All terms).
     *
     * @param args Not used
     * @throws IOException         Error on file read/write
     * @throws SimilarityException Error on similarity calculation
     */
    public static void main(String[] args) throws IOException, SimilarityException {

        String separ = File.separator;
        String[] fileNames = {"500", "1000", "2500", "All"};

        // Serialize the clusterings
        for (String fileName : fileNames) {
            System.out.println("Loading " + fileName + " terms ...");
            long start = System.nanoTime();
            CSVSimilarityLoader csvLoader =
                    new CSVSimilarityLoader("csv" + separ + "pathlengths" + fileName + ".csv");
            long diff = System.nanoTime() - start;
            System.out.println("Loaded " + fileName + " terms in " + diff / 1000000000.0 + "s!");
            Clustering clustering = new Clustering(0.12, csvLoader);
            clustering.printClusteringStatistics();
            clustering.serializeToFile("clustering" + separ + "clustering" + fileName);

            // Test deserialization
//            clustering = deserializeFromFile("clustering" + separ + "clustering" + fileName);
//            System.out.println(clustering);

        }
    }



}
