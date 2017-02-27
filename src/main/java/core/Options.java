package core;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Created by siddharth on 16/11/16.
 */
public class Options {
    private final String contactPoint;
    private final Integer fetchSize;
    private final String username;
    private final String password;
    private final String localDc;
    private final String keyspace;
    private final String tableName;
    private final ConsistencyLevel scanConsistencyLevel;
    private final String queueSize;
    private final long sleepMilliseconds;
    private int consumerCount;


    /**
     * @param contactPoint          URL of one node on cassandra cluster
     *
     * @param fetchSize             Fetch Size implies number of distinct primary keys to get at once.
     *                              @ManagerLayer
     *                              Can be left null, then default value 5000 used
     *
     * @param username              Authentication of cluster.
     *                              @ManagerLayer
     *                              Can be left null, if no authentication on cassandra cluster
     *
     * @param password              Authentication of cluster,
     *                              @ManagerLayer
     *                              Can be left null, if no authentication on cassandra cluster
     *
     * @param localDc               Option to set coordinators(where all computation will take place in case of mismatch)
     *                              @ManagerLayer
     *                              If left null, the coordinator selection will be done in round robin manner
     *
     * @param tableIdentifier       i/p is only keyspace.table_name
     *                              @ManagerLayer
     *                              If you want to repair t1 table of ks1 keyspace, specify "ks1.t1"
     *                              If you want to repair all tables of ks1 keyspace, specify "ks1.*"
     *                              If you want to repair all tables of all keyspace, specify "*.*"
     *
     * @param scanConsistencyLevel  The consistency to get partition keys on
     *                              @ManagerLayer
     *                              Can be left null, then default value of LOCAL_QUORUM used
     *
     * @param queueSize             The internal resultSet queue size.
     *                              @ManagerLayer
     *                              Can be left null, then default value of 500 used
     *
     * @param sleepMilliseconds     Number of milliseconds to sleep before retying individual query on CL.ALL for read repair
     *                              @ManagerLayer
     *                              Can be left null, then default value of 500 used
     */
    public Options(String contactPoint, Integer fetchSize, String username, String password, String localDc, String tableIdentifier, ConsistencyLevel scanConsistencyLevel, String queueSize, long sleepMilliseconds) {
        this.contactPoint = contactPoint;
        this.fetchSize = fetchSize;
        this.username = username;
        this.password = password;
        this.localDc = localDc;
        String temp[] = tableIdentifier.split("\\.");
        this.keyspace = temp[0];
        this.tableName = temp[1];
        this.scanConsistencyLevel = scanConsistencyLevel;
        this.queueSize = queueSize;
        this.sleepMilliseconds = sleepMilliseconds;
    }
}
