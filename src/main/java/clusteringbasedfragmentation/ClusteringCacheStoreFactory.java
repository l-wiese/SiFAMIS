package clusteringbasedfragmentation;

import javax.cache.configuration.Factory;
import java.io.IOException;
import java.io.Serializable;

public class ClusteringCacheStoreFactory implements Factory<ClusteringCacheStore>, Serializable {

    private static final long serialVersionUID = -2472568242147907296L;

    private String clusteringFile;


//################################### Constructor #########################################

    /**
     * Empty Constructor
     */
    public ClusteringCacheStoreFactory() {
        this.clusteringFile = "";
    }

//################################### Getter & Setter #########################################

    public String getClusteringFile() {
        return clusteringFile;
    }

    public void setClusteringFile(String clusteringFile) {
        this.clusteringFile = clusteringFile;
    }

//################################### Overwritten Methods #########################################

    /**
     * Constructs and returns a fully configured instance of ClusteringCacheStore.
     *
     * @return ClusteringCacheStore
     */
    @Override
    public ClusteringCacheStore create() {
        try {
            return new ClusteringCacheStore(this.clusteringFile);
        } catch (ClassNotFoundException | IOException e) {
            System.err.println("Error on accessing clustering file!");
            e.printStackTrace();
        }
        return null;
    }
}
