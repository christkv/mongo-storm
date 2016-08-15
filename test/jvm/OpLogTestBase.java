import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.mongodb.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

class Inserter implements Runnable {
    private String dbName;
    private String collectionName;
    private CountDownLatch latch;

    Inserter(String dbName, String collectionName, CountDownLatch latch) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.latch = latch;
    }

    @Override
    public void run() {
        // Initialize the mongo object
        Mongo mongo = null;
        try {
            // Open connection
            mongo = new Mongo("localhost", 27017);
            // Fetch the local db
            DB db = mongo.getDB(dbName);
            // Holds our collection for the oplog
            DBCollection collection = db.getCollection(collectionName);
            // Now insert a bunch of docs once we are ready
            while(latch.getCount() != 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Insert one hundred objects
            for(int i = 0; i < 100; i++) {
                // Create a basic object
                BasicDBObject object = new BasicDBObject();
                object.put("a", i);
                // Insert the object
                collection.insert(object, WriteConcern.SAFE);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}

// Bolt summarising numbers
class Summarizer implements IBasicBolt {
    private int sum = 0;
    private int numberOfRecords = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // Add to sum
        this.sum = this.sum + tuple.getIntegerByField("a");
        this.numberOfRecords = this.numberOfRecords + 1;
        // Create tuple list
        List<Object> tuples = new ArrayList<Object>();
        // Add sum as tuple
        tuples.add(this.sum);
        // Emit transformed tuple
        basicOutputCollector.emit(tuples);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}

// Bolt summarising numbers
class FullDocumentSummarizer implements IBasicBolt {
    private int sum = 0;
    private int numberOfRecords = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // Object
        DBObject document = (DBObject) tuple.getValueByField("document");

        if(document.get("op").toString().equals("i")) {
            // Count number of records
            this.numberOfRecords = numberOfRecords + 1;
            // Add to sum
            this.sum = this.sum + (Integer)((DBObject)document.get("o")).get("a");
            // Create tuple list
            List<Object> tuples = new ArrayList<Object>();
            // Add sum as tuple
            tuples.add(this.sum);
            // Emit transformed tuple
            basicOutputCollector.emit(tuples);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}

public class OpLogTestBase implements Serializable {

    protected DBObject findLastOpLogEntry(Mongo mongo) throws UnknownHostException {
        // Connect to the db and find the current last timestamp
        DB db = mongo.getDB("local");
        DBObject query = null;
        DBCursor cursor = db.getCollection("oplog.$main").find().sort(new BasicDBObject("$natural", -1)).limit(1);
        if(cursor.hasNext()) {
            // Get the next object
            DBObject object = cursor.next();
            // Build the query
            query = new BasicDBObject("ts", new BasicDBObject("$gt", object.get("ts")));
        }
        // Return the query to find the last op log entry
        return query;
    }
}
