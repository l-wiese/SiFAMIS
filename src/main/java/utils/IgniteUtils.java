package utils;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import com.github.javafaker.Faker;
import org.apache.ignite.configuration.BasicAddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class encapsulate some useful functionality regarding the Ignite cluster.
 */
public class IgniteUtils {


    /**
     * Create the {@link IgniteConfiguration} for the a client or server node.
     * @param addresses IP addresses
     * @param isClient Whether the node is a client or server node
     * @return Ignite configuration
     */
    public static IgniteConfiguration createIgniteConfig(Collection<String> addresses, boolean isClient) {
        TcpDiscoverySpi discSpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(addresses);
        discSpi.setIpFinder(ipFinder)       //TODO
                .setJoinTimeout(0)// wait forever for connection establishment
                .setLocalPort(47500)
                .setLocalPortRange(10);

        // Communication SPI
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setLocalPort(47100)
                .setLocalPortRange(10)
                .setUsePairedConnections(true);

        // Address resolver - map local ip (NAT) to router's public address, map server public IPs to local IPs
        Map<String,String> addressMap = new HashMap<>();
        BasicAddressResolver resolver = null;
        try {
            // TODO
            if (isClient) {
                addressMap.put(InetAddress.getLocalHost().getHostAddress(), "77.185.23.239");
                addressMap.put("141.5.107.8", "10.254.1.5");
                addressMap.put("141.5.107.75", "10.254.1.6");
                addressMap.put("141.5.107.76", "10.254.1.7");
            } else {
                addressMap.put("10.254.1.5", "141.5.107.8");
                addressMap.put("10.254.1.6", "141.5.107.75");
                addressMap.put("10.254.1.7", "141.5.107.76");
            }
            resolver = new BasicAddressResolver(addressMap);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // Connect this client to the cluster
        // Client config
        IgniteConfiguration clientConfig = new IgniteConfiguration();
        clientConfig.setClientMode(isClient)
                .setDiscoverySpi(discSpi)
                .setCommunicationSpi(commSpi)
                .setAddressResolver(resolver)
                .setClientFailureDetectionTimeout(60000)   // 60s (maybe should be more)
                .setPeerClassLoadingEnabled(true);

        return clientConfig;
    }



    /**
     * Generate p randomly created persons.
     * @param p Number of persons
     * @return Array of all persons
     */
    public static materializedfragments.Info[] generatePersons(long p) {
        materializedfragments.Info[] persons = new materializedfragments.Info[(int) p];
        materializedfragments.InfoGenerator generator = new materializedfragments.InfoGenerator(new Faker());
        for (int id = 0; id < p; id++) {
            persons[id] = generator.generate(id);
            System.out.println(id * 100.0 / p + "% ");
        }
        return persons;
    }

    /**
     * Load affinity function for the given alpha and number of terms (uses standard {@link File#separator}).
     * @param alpha Similarity threshold
     * @param terms Number of terms (used to identify correct term and similarity files)
     * @return Affinity function or null if at least one of the parameters was null
     */
    public static ClusteringAffinityFunction loadAffinityFunction(Double alpha, Integer terms) throws SimilarityException, IOException {
        if (alpha == null || terms == null)
            return null;

        if (terms == 10 || terms == 30 || terms == 100) {
            String termsFile = "csv" + File.separator + "terms" + terms + ".txt";
            String simFile = "csv" + File.separator + "result" + terms + ".csv";
            return new ClusteringAffinityFunction(alpha, termsFile, simFile);
        } else if (terms == 500 || terms == 1000 || terms == 2500){
            String pathlengthCSV = "csv" + File.separator + "pathlengths" + terms + ".csv";
            return new ClusteringAffinityFunction(alpha, pathlengthCSV);
        }
        String pathlengthCSV = "csv" + File.separator + "pathlengthsAll.csv";
        return new ClusteringAffinityFunction(alpha, pathlengthCSV);
    }


}
