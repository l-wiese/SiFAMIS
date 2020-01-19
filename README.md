# SiFAMIS

Similarity-based flexible query answering for medical information systems with a clustering-based fragmentation for [MeSH](https://id.nlm.nih.gov/mesh/)
disease term similarities on an Apache Ignite distributed in-memory SQL database.

## Requirements

On each server, the following have to be installed:
1. Java ([JDK 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or another version that is supported by the Apache Ignite System)
2. [Maven](https://maven.apache.org/install.html)
3. [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/) (only required for the Web Interface)
4. Apache Ignite can be downloaded [here](https://ignite.apache.org/download.cgi). Installation instructions see 
[how to get started](https://apacheignite.readme.io/docs/getting-started)

Furthermore, the JAVA_HOME and IGNITE_HOME environment variables need to be set accordingly.

## Get Project

The project can either be cloned with `git clone https://gitlab.gwdg.de/jeromario.schaefer/sifamis.git` or downloaded as ZIP or TAR file. 

After cloning or downloading and unpacking, the project can be compiled and packaged with

`mvn clean compile package`

To initialize the files that contain the clustering, a folder named clustering must be created in the repository folder by

`mkdir clustering`

and then `Clustering` class (package `clusteringbasedfragmentation`) main method has to be called with

`java -cp "target/* clusteringbasedfragmentation.Clustering`


Additionally, the code and the server configuration files have to be adapted to the network topology of the used servers 
(i.e. ip addresses, ports, address resolver, etc. have to be changed according to the servers' properties). The positions where
these changes have to be made are indicated by `TODO` comments in the code as well as in the configuration files.

Finally, another `mvn package` execution is required to have the updated SiFAMIS.jar file (in folder [target](target/)), configuration files 
and other dependencies automatically copied to the Ignite `lib/` folder.


## Configure & Start Servers

A server can be started via the command line via Ignite's provided script, `bin/ignite.sh`, using one of the predefined XML configuration 
files in folder [config](config/), e.g. 

`bin/ignite.sh config/partitions10.xml`. 

Note: This configuration files have to be adapted to match the network topology of the used servers (i.e. ip addresses, address resolver, ...)
as already mentioned in section "Get Project". Then, the servers will find one another and form a cluster.

Additional JVM parameters may be required or used to optimize performance when starting an Ignite server node. Some of them can be found in folder [jvmparams](jvmparams/) or
alternatively in the documentation of Apache Ignite, e.g. in the section [performance tips](https://apacheignite.readme.io/docs/performance-tips).


## Generate Sample Data

To generate sample data in the DDB, the created SiFAMIS.jar application can be used. For usage information type `java -jar SiFAMIS.jar --help` or `java -jar SiFAMIS.jar -h`

It is important that the servers in the cluster should be started before the client is started to generate the sample data and even more important that the server configuration
matches the command line parameters given to the SiFAMIS.jar application, e.g. if the server is started in "materialized" mode with config file materialized1000.xml, then also the 
data generation application must be started with command line flag `-m` or `--materialized`, number of terms `-t 1000` or `--terms 1000` and a scaling factor determining the amount of
tuples generated with `-sf [integer]`. The similarity threshold alpha should also be changed to a value matching the threshold of the server configs - ideally between 0.1 and 0.2 -
otherwise if it is not set a default value will be used.

If the SIM and CLUSTERING cache shall be used, the server config should be one of the template configs which name end with "clustCache.xml" or a modified copy of it and the client
can then be started with --initSim and --initClu flags to enable both caches.

For example:

`java -cp "*" SiFAMIS.jar -ip localhost -m -t 1000 --alpha 0.12 -sf 100 --initSim`


## Web Interface (RESTful Web Service)

Note: This is currently not(!) working but might be extended.

docker network create -d bridge --subnet 192.168.0.0/24 --gateway 192.168.0.1 dockernet

The web interface is deployed as WAR to an Apache Tomcat server and can be enabled on one of the servers with the help of the [docker-compose](docker-compose.yml) file by

`docker run --rm -it -v $(pwd):/project -w /project maven mvn package && docker-compose up`

It can be accessed in the browser locally with 'localhost:8080/SiFAMIS' or remotely replaced by the actual IP address of the server.