import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;
import org.mongodb.bolt.MongoInsertBolt;
import org.mongodb.bolt.MongoUpdateBolt;
import org.mongodb.spout.MongoCappedCollectionSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

// Bolt summarising numbers
class FullDocumentCappedSummarizer implements IBasicBolt {
    private int sum = 0;
    private int numberOfRecords = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {}

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // Object
        DBObject document = (DBObject) tuple.getValueByField("document");
        // Count number of records
        this.numberOfRecords = numberOfRecords + 1;
        // Add to sum
        this.sum = this.sum + (Integer)document.get("a");
        // Create tuple list
        List<Object> tuples = new ArrayList<Object>();
        // Add sum as tuple
        tuples.add(this.sum);
        // Emit transformed tuple
        basicOutputCollector.emit(tuples);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}

public class FullDocumentCappedCollectionTest extends OpLogTestBase {

    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        mongo.dropDatabase("storm_mongospout_test");
    }

    @Test
    public void aggregateOpLogFieldAndUpdateDocumentInMongoDB() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        // Get db
        DB db = mongo.getDB("storm_mongospout_test");
        // Create a capped collection
        db.createCollection("aggregation", new BasicDBObject("capped", true).append("size", 100000));
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter("storm_mongospout_test", "aggregation", latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // The update query
        UpdateQueryCreator updateQuery = new UpdateQueryCreator() {
            @Override
            public DBObject createQuery(Tuple tuple) {
                return new BasicDBObject("aggregation_doc", "summary");
            }
        };

        // Field mapper
        StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
            @Override
            public DBObject map(DBObject object, Tuple tuple) {
                return BasicDBObjectBuilder.start().push( "$set" ).add( "sum" , tuple.getIntegerByField("sum")).get();
            }
        };

        // Query to filter
        DBObject query = new BasicDBObject("a", new BasicDBObject("$gt", 50));

        // Create a mongo bolt
        MongoUpdateBolt mongoSaveBolt = new MongoUpdateBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", updateQuery, mapper, WriteConcern.NONE);
        // Set the spout
        builder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://127.0.0.1:27017/storm_mongospout_test", "aggregation", query), 1);
        // Add a bolt
        builder.setBolt("sum", new FullDocumentCappedSummarizer(), 1).allGrouping("mongodb");
        builder.setBolt("mongo", mongoSaveBolt, 1).allGrouping("sum");

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);

        // Run on local cluster
        LocalCluster cluster = new LocalCluster();
        // Submit the topology
        cluster.submitTopology("test", conf, builder.createTopology());
        // Starts inserts
        latch.countDown();

        // Wait until we have the summation all done
        DBCollection collection = mongo.getDB("storm_mongospout_test").getCollection("stormoutputcollection");
        // Keep polling until it's done
        boolean done = false;

        // Keep checking until done
        while(!done) {
            DBObject result = collection.findOne(new BasicDBObject("aggregation_doc", "summary"));

            if(result != null && ((Integer)result.get("sum")).intValue() == 3675) {
                done = true;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {}
            }
        }

        // Sleep for a bit then kill the topology
        cluster.killTopology("test");
        cluster.shutdown();
    }

    @Test
    public void aggregateOpLogFieldAndInsertADocumentPrResultInMongoDB() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        // Get db
        DB db = mongo.getDB("storm_mongospout_test");
        // Create a capped collection
        db.createCollection("aggregation", new BasicDBObject("capped", true).append("size", 100000));
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter("storm_mongospout_test", "aggregation", latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // Field mapper
        StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
            @Override
            public DBObject map(DBObject object, Tuple tuple) {
                return BasicDBObjectBuilder.start()
                        .add( "sum" , tuple.getIntegerByField("sum"))
                        .add( "timestamp", new Date())
                        .get();
            }
        };

        // Query to filter
        DBObject query = new BasicDBObject("a", new BasicDBObject("$gt", 50));

        // Create a mongo bolt
        MongoInsertBolt mongoInserBolt = new MongoInsertBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", mapper, WriteConcern.NONE, false);
        // Set the spout
        builder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://127.0.0.1:27017/storm_mongospout_test", "aggregation", query), 1);
        // Add a bolt
        builder.setBolt("sum", new FullDocumentCappedSummarizer(), 1).allGrouping("mongodb");
        builder.setBolt("mongo", mongoInserBolt, 1).allGrouping("sum");

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);

        // Run on local cluster
        LocalCluster cluster = new LocalCluster();
        // Submit the topology
        cluster.submitTopology("test", conf, builder.createTopology());
        // Starts inserts
        latch.countDown();

        // Wait until we have the summation all done
        DBCollection collection = mongo.getDB("storm_mongospout_test").getCollection("stormoutputcollection");
        // Keep polling until it's done
        boolean done = false;

        // Keep checking until done
        while(!done) {
            if(collection.count() == 49) {
                System.out.println("========================================== count :: " + collection.count());
                done = true;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {}
            }
        }

        // Sleep for a bit then kill the topology
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
