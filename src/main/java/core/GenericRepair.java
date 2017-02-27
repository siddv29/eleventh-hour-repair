package core;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 16/11/16.
 */
public class GenericRepair {
    private final Cluster cluster;
    private final Session session;
    private final Map<String, String> properties;
    private final LinkedBlockingQueue<ResultSet> queue;
    private final ConsistencyLevel scanConsistencyLevel;
    private final ConsistencyLevel fetchConsistencyLevel;
    private final int sleepForFailedFetchStatement;
    private final BoundStatement boundStatement;
    private final String keyspace;
    private final String tableName;
    private final String partitionKey;
    private final String individualFetchStatement;
    private final DataType.Name partitionKeyColumnsToDataTypeName[];
    private int numberOfConsumers;
    final static Logger LOG = /*LoggerFactory.getLogger*/Logger.getLogger(GenericRepair.class);

    public GenericRepair(String inputFile) throws IOException {
        properties =  new HashMap<String, String>();
        loadProperties(inputFile);
        cluster = Cluster.builder()
                .addContactPoint(properties.get("contact_point"))
                .withQueryOptions(new QueryOptions().setFetchSize(Integer.parseInt(properties.get("fetch_size"))))
                .withCredentials(properties.get("username"), properties.get("password"))
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(1000000).setReadTimeoutMillis(1000000))
                .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(properties.get("local_dc"))
                        .build()).build();
        session = cluster.connect();
        scanConsistencyLevel = ConsistencyLevel.valueOf(properties.get("scan_consistency"));
        fetchConsistencyLevel = ConsistencyLevel.ALL;
        this.keyspace = properties.get("keyspace");
        this.tableName = properties.get("table_name");
        this.partitionKey = getPartitionKey();
        this.individualFetchStatement = initializeIndividualFetchStatement();
        this.partitionKeyColumnsToDataTypeName = new DataType.Name[partitionKey.split(",").length];
        queue = new LinkedBlockingQueue<>(Integer.parseInt(properties.get("queue_size")));
        sleepForFailedFetchStatement = Integer.parseInt(properties.get("sleep_millisconds"));
        boundStatement = new BoundStatement(session.prepare("select distinct "+partitionKey+" from "+keyspace+"."+tableName));;
    }

    private void loadProperties(String inputFile) throws IOException{
        BufferedReader fileIn = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputFile))));
        String line = fileIn.readLine();
        String[] str;
        while (line != null) {
            str = line.split(":");
            properties.put(str[0], str[1]);
            line = fileIn.readLine();
        }
    }


    private String getPartitionKey() {
        StringBuffer partitionKeyTemp = new StringBuffer();
        for(ColumnMetadata partitionKeyPart : cluster.getMetadata().getKeyspace(keyspace).getTable(tableName).getPartitionKey()){
            partitionKeyTemp.append(partitionKeyPart.getName()+",");
        }
        String partitionKey = partitionKeyTemp.substring(0,partitionKeyTemp.length()-1);
        return partitionKey;
    }

    public static void main(String... args) throws Exception {

        if(args.length == 0) {
            args = new String[1];
            args[0]="/home/siddharth/git/films/reverse-migration/newInputConf";
        }
        System.out.println("Program started at " + new Date());
        GenericRepair genericRepair= new GenericRepair(args[0]);
        genericRepair.runRepair();
        System.out.println("End of main");
        System.out.println("Program ended at " + new Date());

        System.exit(0);
    }

    private void runRepair() throws InterruptedException {
        numberOfConsumers = Integer.parseInt(properties.get("consumer_count"));
        initializeMappingOfPartitionKeyColumnsToDataType();
        Thread thread[] = new Thread[numberOfConsumers];
        for(int i=0;i<numberOfConsumers;i++) {
            thread[i] = new Consumer();
            thread[i].start();
        }

        dumpPartitionKeys(numberOfConsumers);
        for(int i=0;i<numberOfConsumers;i++) {
            thread[i].join();
        }
    }

    private void initializeMappingOfPartitionKeyColumnsToDataType() {
        List<ColumnDefinitions.Definition> definitions = null;
        for(Row row : session.execute("select "+partitionKey+" from "+keyspace+"."+tableName+" limit 1")){
            definitions = row.getColumnDefinitions().asList();
        }
        for(int i=0;i<definitions.size();i++){
            partitionKeyColumnsToDataTypeName[i]=definitions.get(i).getType().getName();
        }
        /*
        partitionKeyColumnsToDataTypeName[0]=BIGINT
        partitionKeyColumnsToDataTypeName[0]=VARCHAR
        and so on
         */
    }


    public void dumpPartitionKeys(int numberOfConsumers) throws InterruptedException {
        fetchLoop(boundStatement);
        System.out.println("dumpProductFilterMapping Finished");
        LOG.info("dumpProductFilterMapping Finished");
        for(int i=0;i<numberOfConsumers;i++) {
            queue.put(new ProducerEnd());
        }

    }

    private void fetchLoop(BoundStatement boundStatement) throws InterruptedException {

        boundStatement.setConsistencyLevel(scanConsistencyLevel);
        boundStatement.setFetchSize(Integer.parseInt(properties.get("fetch_size")));
        String currentPageInfo = null;
        do {
            try {
                LOG.debug("Hitting..." + currentPageInfo + "...");
                if (currentPageInfo != null) {
                    boundStatement.setPagingState(PagingState.fromString(currentPageInfo));
                }
                ResultSet rs = session.execute(boundStatement);
                LOG.debug("Pushed to queue");
                queue.put(rs);
                PagingState nextPage = rs.getExecutionInfo().getPagingState();
                String nextPageInfo = null;
                if (nextPage != null) {
                    nextPageInfo = nextPage.toString();
                }
                currentPageInfo = nextPageInfo;
            } catch (NoHostAvailableException e) {
                LOG.warn("No host available exception... going to sleep for 1 sec");
                try {
                    Thread.sleep(1000 * 1);
                } catch (Exception e2) {
                }
            }
            LOG.debug("Finished while loop");

        } while (currentPageInfo != null);

    }

    public String initializeIndividualFetchStatement() {
        String prefix = "select count(*) from "+keyspace+"."+tableName+" where ";
        StringBuilder partitionKeyFetchClause = new StringBuilder();
        String temp[]=partitionKey.split(",");
        partitionKeyFetchClause.append(temp[0]+" = ?");
        for(int i=1;i<temp.length;i++){
            partitionKeyFetchClause.append(" and "+temp[i]+" = ?");
        }
        return prefix+partitionKeyFetchClause.toString();
    }

    class Consumer extends Thread {
        private final BoundStatement fetchStatement;
        private Object partitionKeyValues[];

        Consumer(){
            this.fetchStatement = new BoundStatement(session.prepare(individualFetchStatement));
            this.fetchStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        }


        private void processRow(Row row) throws Exception {

            partitionKeyValues = new Object[partitionKey.split(",").length];
            for(int i=0;i<partitionKeyValues.length;i++){
                switch (partitionKeyColumnsToDataTypeName[i]){
                    case BIGINT:
                        partitionKeyValues[i]=row.getLong(i); break;
                    case TEXT:
                    case VARCHAR:
                    case BLOB:
                        partitionKeyValues[i]=row.getString(i); break;
                    case FLOAT:
                        partitionKeyValues[i]=row.getFloat(i); break;
                    case DOUBLE:
                        partitionKeyValues[i]=row.getDouble(i); break;
                    case DECIMAL:
                        partitionKeyValues[i]=row.getDecimal(i); break;
                    case DATE:
                        partitionKeyValues[i]=row.getDate(i); break;
                    case TIME:
                        partitionKeyValues[i]=row.getTime(i); break;
                    case TIMESTAMP:
                        partitionKeyValues[i]=row.getTimestamp(i); break;
                    case TIMEUUID:
                    case UUID:
                        partitionKeyValues[i]=row.getUUID(i); break;
                    case BOOLEAN:
                        partitionKeyValues[i]=row.getBool(i); break;
                    case INET:
                        partitionKeyValues[i]=row.getInet(i); break;
                    case INT:
                        partitionKeyValues[i]=row.getInt(i); break;
                }
            }

            fetchStatement.bind(partitionKeyValues);
            boolean success=false;
            while(!success) {
                try {
//                System.out.println("FETCHED !!");
                    session.execute(fetchStatement);
                    success = true;

                } catch (Exception e) {LOG.error("READ_REPAIR_TRYING_AGAIN");try{Thread.sleep(sleepForFailedFetchStatement);}catch (Exception e2){}}
            }
            LOG.debug("REPAIRED->"+row);
        }


        public void run() {
            LOG.info("Consumer : " + Thread.currentThread().getName()+" started");
            ResultSet rs;
            String statusOfSave;
            try {
                do {
                    rs = queue.take();
                    LOG.debug("Consuming");
                    if (rs instanceof ProducerEnd) {
                        break;
                    }
                    int remaining = rs.getAvailableWithoutFetching();
                    LOG.debug("Consuming " + remaining);
                    if (remaining != 0) {
                        for (Row row : rs) {
                            try{
                                processRow(row);
                            }catch (Exception e){
                                LOG.error("EXCEPTION_IN_ROW:"+row);
                            }
                            if (--remaining == 0) {
                                break;
                            }
                        }
                    }

                } while (true);

            } catch (InterruptedException e) {
                LOG.error("Interrupted exception in consumer at queue" + e);
                e.printStackTrace();
                System.exit(2);
            } catch (Exception e) {
                LOG.error("Exception in consumer" + e);
                e.printStackTrace();
                System.exit(2);
            }
            LOG.info("Consumer : " + Thread.currentThread().getName()+" finished");
        }

    }

    class ProducerEnd implements ResultSet {

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return null;
        }

        @Override
        public boolean isExhausted() {
            return false;
        }

        @Override
        public Row one() {
            return null;
        }

        @Override
        public List<Row> all() {
            return null;
        }

        @Override
        public Iterator<Row> iterator() {
            return null;
        }

        @Override
        public int getAvailableWithoutFetching() {
            return 0;
        }

        @Override
        public boolean isFullyFetched() {
            return false;
        }

        @Override
        public ListenableFuture<ResultSet> fetchMoreResults() {
            return null;
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return null;
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            return null;
        }

        @Override
        public boolean wasApplied() {
            return false;
        }
    }

}


