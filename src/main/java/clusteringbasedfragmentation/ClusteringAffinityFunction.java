package clusteringbasedfragmentation;

import clusteringbasedfragmentation.similarityfunctions.*;
import materializedfragments.FragIDKey;
import materializedfragments.IllKey;
import neo4j.Neo4JSimilarity;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * <p>
 * This class provides the affinity collocation functionality as it implements the Interface {@link AffinityFunction}.
 * It uses a clustering-based fragmentation on a relaxation attribute (which is of type {@link String}) to obtain a
 * mapping of the horizontal fragmentation of a table to partitions and to a {@link ClusterNode} of the
 * {@link org.apache.ignite.IgniteCluster}.
 * </p>
 * Furthermore, it is also used to derive a fragmentation of the database tables (INFO and TREAT) based on the
 * fragmentation of the primary table (ILL).
 */
public class ClusteringAffinityFunction implements AffinityFunction, Serializable {


    private static final long serialVersionUID = 7436136269367842660L;


    /**
     * Number of partitions
     */
    private int parts;

    /**
     * Default value for clustering algorithm threshold.
     */
    private static final double DFLT_ALPHA = 0.2;

    /**
     * Clustering for the clustering-based fragmentation.
     */
    private Clustering clustering;


    /**
     * Similarity function providing the required similarity of two terms as well
     * as methods to obtain the term set
     */
    private final MeSHSimilarityFunction similarityFunction;


    /**
     * Stores the partition assignment of the cluster
     */
    private List<List<ClusterNode>> partitionAssignment;


//##################### Constructors ######################

    /**
     * Constructor using default value for alpha and testing constructor.
     * For details see {@link ClusteringAffinityFunction#ClusteringAffinityFunction(double)}.
     *
     * @throws SimilarityException Error on similarity value calculation
     */
    public ClusteringAffinityFunction() throws SimilarityException {
        this(DFLT_ALPHA);
    }

    /**
     * Constructor for a affinity function based on clustering-based fragmentation with similarity calculation.
     * Obtains the clusters from the clustering algorithm for later usage in the affinity collocation and partition
     * assignment.
     * NOTE: This constructor should only be used for code testing purposes, it only has the active domain {Cough,
     * Asthma, Ulna Fracture, Tibial Fracture, Influenza} with predefined similarities (obtained from UMLS::Similarity).
     *
     * @param alpha Threshold for clustering algorithm
     * @throws SimilarityException Error on similarity value calculation
     */
    public ClusteringAffinityFunction(double alpha) throws SimilarityException {

        this.similarityFunction = new DefaultSimilarity();

        // Calculate clustering (index of cluster implies mapping of cluster to partition)
        this.clustering = new Clustering(alpha, this.similarityFunction);

        // each cluster is assigned to a partition and for each cluster the same partition is used also for the
        // derived fragmentation
        this.parts = this.clustering.size();
    }

    /**
     * Constructor for an affinity function that loads the terms and similarity values from files, e.g.
     * .txt or .csv files, and then stores them in a set and map respectively.
     *
     * @param alpha            Similarity Threshold
     * @param termsFile        Path to terms file (form: term1\nterm2\n...)
     * @param similaritiesFile Path to similarity file (form: term1(CUI1)<>term2(CUI2)<>simValue\n...)
     * @throws SimilarityException Error on similarity value calculation
     */
    public ClusteringAffinityFunction(double alpha, String termsFile, String similaritiesFile) throws SimilarityException {

        this.similarityFunction = new CSVSimilarityLoader(termsFile, similaritiesFile);

        // calculate clustering (index of clusters implies mapping of cluster to partition)
        this.clustering = new Clustering(alpha, this.similarityFunction);
        this.parts = this.clustering.size();
    }

    /**
     * Affinity function that is provided with a custom {@link MeSHSimilarityFunction},
     * e.g. the {@link CSVSimilarityLoader} or the {@link Neo4JSimilarity}.
     *
     * @param alpha              Similarity Threshold
     * @param similarityFunction A custom {@link MeSHSimilarityFunction}
     * @throws SimilarityException Error on similarity value calculation
     */
    public ClusteringAffinityFunction(double alpha, MeSHSimilarityFunction similarityFunction) throws SimilarityException {

        this.similarityFunction = similarityFunction;

        // Calculate clustering
        this.clustering = new Clustering(alpha, this.similarityFunction);
        this.parts = this.clustering.size();
    }


    /**
     * Affinity function that loads the (serialized) {@link Clustering} from a file and
     * is provided with a custom {@link SimilarityFunction} (which was used to obtain the clustering!).
     *
     * @param similarityFunction A custom {@link MeSHSimilarityFunction}
     * @param clusteringFile     Path to the serialized clustering file
     * @throws IOException            Thrown if the file with the serialized clustering could not be found/read.
     * @throws ClassNotFoundException Thrown if a class could not be found in classpath
     */
    public ClusteringAffinityFunction(MeSHSimilarityFunction similarityFunction, String clusteringFile)
            throws IOException, ClassNotFoundException {

        this.similarityFunction = similarityFunction;

        // Calculate clustering
        this.clustering = Clustering.deserializeFromFile(clusteringFile).setSimilarityFunction(this.similarityFunction);
        this.parts = this.clustering.size();
    }

    /**
     * Affinity function that initializes the clustering from a pathlength csv file (e.g. term1|term2|pathlength...).
     * Uses a {@link CSVSimilarityLoader} as {@link MeSHSimilarityFunction}
     *
     * @param alpha         Similarity threshold
     * @param pathLengthCSV Path to pathlength csv
     * @throws SimilarityException Error on similarity value calculation
     * @throws IOException         Error on file read/write.
     */
    public ClusteringAffinityFunction(double alpha, String pathLengthCSV) throws SimilarityException, IOException {
        this(alpha, new CSVSimilarityLoader(pathLengthCSV));
    }


//##################### Overwritten Methods ######################

    @Override
    public void reset() {
        // No-op.
    }

    @Override
    public void removeNode(UUID nodeId) {
        // No-op
    }


    /**
     * Gets the total number of partitions. There should be at least as much partitions as nodes,
     * so that each node can host at least one partition.
     *
     * @return Number of Partitions
     */
    @Override
    public int partitions() {
        return parts;
    }


    /**
     * Returns the partition mapping for the given key.
     * See {@link AffinityFunction#partition(Object)} for more details.
     *
     * @param key Key
     * @return Partition number
     */
    @Override
    public int partition(Object key) {

        if (key == null)
            throw new IllegalArgumentException("The key passed to the AffinityFunction's method " +
                    "partition(Object key) was null.");

        // If the key is of type BinaryObject, then deserialize it first
        if (key instanceof BinaryObject) {
            BinaryObject binary = (BinaryObject) key;
            key = binary.deserialize();
        }


        // If the key is of type IllKey, find the partition based on the clustering (i-th cluster = i-th partition)
        // If the key is of type FragIDKey, find the partition based on the derived fragmentation (contained in key)
        if (key instanceof IllKey) {
            IllKey illKey = (IllKey) key;
            try {
                return this.identifyCluster(illKey.getDisease());
            } catch (SimilarityException e) {
                e.printStackTrace();
                return -1;
            }
        }
        if (key instanceof FragIDKey) {
            return ((FragIDKey) key).getFragID();
        }

        throw new IllegalArgumentException("ERROR: The key object " + key + " is of some other type: "
                + key.getClass() + "!");

    }


    /**
     * This function maps all the partitions of this cache to nodes of the cluster. The outer list
     * of the returned nested lists is indexed by the partition number and stores the mapping of that
     * partition to a list of nodes of the cluster (inner lists).
     * The current functionality is limited to map each partition only to one primary node without backups.
     *
     * @param affCtx Context to be passed to the function automatically. For details see {@link AffinityFunction}.
     * @return Assignment of partitions to nodes
     */
    @Override
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        if (affCtx == null)
            throw new IllegalArgumentException("AffinityFunctionContext passed to the AffinityFunction's method " +
                    "assignPartitions(AffinityFunctionContext affCtx) was null.");

        // Resulting list maps each partition to a list of nodes
        this.partitionAssignment = new ArrayList<>(this.parts);

        // Get all the nodes in the cluster
        List<ClusterNode> allNodes = affCtx.currentTopologySnapshot();

        // Assign each partition to a node (no replication)
        for (int i = 0; i < this.parts; i++) {
            List<ClusterNode> assignedNodes = this.assignPartition(i, allNodes);
            this.partitionAssignment.add(i, assignedNodes);
        }

        return this.partitionAssignment;
    }


//##################### Clustering-based Fragmentation (Partition-Mappings, Cluster-Algorithm)  ######################

    /**
     * Assign a certain partition to a certain node in the cluster.
     * <p>
     * This functionality can also be optionally extended later to map partitions to several nodes,
     * e.g. primary and backup nodes according to the clustering-based fragmentation.
     *
     * @param partition The number of the partition to assign to a node
     * @param allNodes  A list of all nodes in the cluster, can be provided by e.g.
     *                  {@link AffinityFunctionContext#currentTopologySnapshot()}
     * @return List of cluster nodes (currently containing only one node) to which the partition is mapped
     */
    public List<ClusterNode> assignPartition(int partition, List<ClusterNode> allNodes) {
        /* (assuming i partitions and k nodes, k <= i)
           The i-th partition is assigned to the (i % k)-th node
           Example: 2 Nodes
                - partition 0 with respiratory diseases --> node 0, partition 1 with fractures --> node 1
        */
        List<ClusterNode> result = new ArrayList<>();
        result.add(allNodes.get(partition % allNodes.size()));
        return result;
    }


    /**
     * This method identifies the cluster id to a given term.
     *
     * @param term The term to match to a cluster
     * @return Number of the cluster
     * @throws SimilarityException Thrown if an error occurs during similarity calculation
     */
    public int identifyCluster(String term) throws SimilarityException {

        // If the similarity function is of type SimClusteringTableSimilarity, use provided method to identify cluster
        if (this.similarityFunction instanceof SimClusteringTableSimilarity) {
            SimClusteringTableSimilarity scts = (SimClusteringTableSimilarity) this.similarityFunction;
            if (scts.isIdentifyClusterEnabled()) {
                try {
                    return scts.identifyCluster(term);
                } catch (NoSuchMethodException e) {
                    throw new SimilarityException("Method identifyCluster(String term) of class " +
                            scts.getClass().getName() + " is not enabled but was invoked!", e);
                }
            }
        }

        // Identify the cluster to which this term belongs
        // If term is equal to the head of i-th cluster (store heads during check for further identification) return i
        int clustersize = clustering.size();
        String[] head = new String[clustersize];
        for (int i = 0; i < clustersize; i++) {
            head[i] = clustering.getHead(i);
            if (term.equals(head[i]))
                return i;

        }

        // No head matched -> calculate similarity of t to each of the heads and find maximum similarity
        double max = -1;
        int argMax = -1;
        for (int i = 0; i < head.length; i++) {
            double sim = similarityFunction.similarity(term, head[i]);
            if (max < sim) {
                max = sim;
                argMax = i;
            }
        }

        return argMax;
    }


    /**
     * Get the corresponding (primary) node for a given partition
     *
     * @param partition Partition number
     * @return Node storing that partition
     */
    public ClusterNode getNodeOfPartition(int partition) {
        return this.partitionAssignment.get(partition).get(0);
    }


//################################# Getter & Setter  ########################################

    /**
     * Get list of clusters of underlying clustering
     *
     * @return List of clusters
     */
    public ArrayList<Cluster<String>> getClusters() {
        return clustering.getClusters();
    }

    /**
     * Get underlying clustering.
     *
     * @return Clustering
     */
    public Clustering getClustering() {
        return clustering;
    }

    /**
     * Get all the used MeSH terms as list
     *
     * @return List of terms
     */
    public ArrayList<String> getTerms() {
        return new ArrayList<>(similarityFunction.getTerms());
    }

    /**
     * Get the concept unique identifier (CUI) of a given MeSH term if known.
     *
     * @param diseaseTerm MeSH disease term
     * @return CUI of given term or "" if unknown or input term was wrong
     */
    public String getCUI(String diseaseTerm) {
        Map<String, String> map = similarityFunction.getTermsWithCUIs();
        if (map == null)
            return "";
        return map.get(diseaseTerm);
    }

    /**
     * Get the similarity function
     *
     * @return Similarity function
     */
    public MeSHSimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    //################################### MAIN-Method  #########################################

    /**
     * Test unit for clusterings for different datasets and for finetuning the parameter alpha
     *
     * @param args First string is ip address of neo4j (opt. port)
     *             Second string is neo4j username
     *             Third string is neo4j password
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Must be 3 string arguments as input parameters! Read documentation " +
                    "of ClusteringAffinityFunction class for details.");
            System.exit(-1);
        }

        // Prepare clustering test for simple default adom
        ArrayList<String> terms10, terms30, terms100;
        ClusteringAffinityFunction maf = new ClusteringAffinityFunction(0.2);

        // Test clustering
        Clustering clustering = maf.clustering;
        ArrayList<Cluster<String>> clusters = clustering.getClusters();
        System.out.println("Clustering size: " + clusters.size() + "\n" + clustering);
        System.out.println("-----------------------------------------------------------------");

        // Now test for small dataset (alpha 0.12 is relatively good), 10 terms
        // alpha = 0.1 --> 1 cluster, alpha = 0.2 --> 9 clusters, alpha = 0.15 --> 7 clusters,
        // alpha = 0.13 --> 6 clusters, alpha = 0.12 --> 5 clusters, alpha = 0.1111 --> 1 cluster (minimal sim)
        String separ = File.separator;
        String termsFile = "csv" + separ + "terms10.txt";
        String simFile = "csv" + separ + "result10.csv";
        maf = new ClusteringAffinityFunction(0.12, termsFile, simFile);
        terms10 = maf.getTerms();
        maf.clustering.printClusteringStatistics();
        System.out.println("-----------------------------------------------------------------");

        // bigger data set (alpha > 0.1111 and <= 0.15), 30 terms
        // alpha = 0.2 --> many small clusters (often only head), alpha = 0.1 --> 1 cluster, alpha = 0.15 --> 12,
        // alpha = 0.12 --> 8 (not bad), alpha = 0.13 & 0.14 --> 9 (not bad), alpha = 0.11 & 0.1111--> 2 (bad, min sim),
        // alpha = 0.115 & 0.1125 & 0.1112 --> 8 (not bad)
        termsFile = "csv" + separ + "terms30.txt";
        simFile = "csv" + separ + "result30.csv";
        maf = new ClusteringAffinityFunction(0.17, termsFile, simFile);
        terms30 = maf.getTerms();

        // Test clustering
        maf.clustering.printClusteringStatistics();
        System.out.println("-----------------------------------------------------------------");


        // biggest data set, 100 terms
        // alpha    | clusters  | terms/cluster         (interesting values)
        // 0.2      |    51     |       1
        // 0.15     |    34     |       2
        // 0.14     |    22     |       4
        // 0.13     |    22     |       4
        // 0.125    |    15     |       6
        // 0.115    |    15     |       6
        // 0.11     |    6      |       16
        // < 0.10 there is 3 clusters and 1 cluster (btw 0.001 steps do not produce other values than in the table)
        termsFile = "csv" + separ + "terms100.txt";
        simFile = "csv" + separ + "result100.csv";
        // Test clustering (with statistics)
        for (double alpha = 0.1; alpha <= 0.17; alpha = alpha + 0.005) {
            maf = new ClusteringAffinityFunction(alpha, termsFile, simFile);
            maf.clustering.printClusteringStatistics();
        }
        terms100 = maf.getTerms();
        System.out.println("-----------------------------------------------------------------");


        // 500, 1000, 2500 and all MeSH Terms (from pathlength csvs)
        // Good alphas (500):
        for (double alp = 0.1; alp <= 0.18; alp = alp + 0.01) {
            maf = new ClusteringAffinityFunction(alp, "csv" + separ + "pathlengths500.csv");
            maf.clustering.printClusteringStatistics();
        }
        for (double alp = 0.1; alp <= 0.18; alp = alp + 0.01) {
            maf = new ClusteringAffinityFunction(alp, "csv" + separ + "pathlengths1000.csv");
            maf.clustering.printClusteringStatistics();
        }
        for (double alp = 0.1; alp <= 0.18; alp = alp + 0.01) {
            maf = new ClusteringAffinityFunction(alp, "csv" + separ + "pathlengths2500.csv");
            maf.clustering.printClusteringStatistics();
        }
        for (double alp = 0.1; alp <= 0.18; alp = alp + 0.01) {
            maf = new ClusteringAffinityFunction(alp, "csv" + separ + "pathlengthsAll.csv");
            maf.clustering.printClusteringStatistics();
        }

    }


}
