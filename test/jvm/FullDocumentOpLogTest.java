import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import com.mongodb.*;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;
import org.mongodb.bolt.MongoInsertBolt;
import org.mongodb.bolt.MongoUpdateBolt;
import org.mongodb.spout.MongoOpLogSpout;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class FullDocumentOpLogTest extends OpLogTestBase {

    @Before
    public void setUp() throws UnknownHostException {
        Mongo mongo = new Mongo("localhost", 27017);
        mongo.dropDatabase("storm_mongospout_test");
    }

    @Test
    public void aggregateOpLogFieldAndUpdateDocumentInMongoDB() throws UnknownHostException {
        Mongo mongo = new Mongo("localhost", 27017);
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter("storm_mongospout_test", "aggregation", latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Query to filter
        DBObject query = findLastOpLogEntry(mongo);

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

        // Create a mongo bolt
        MongoUpdateBolt mongoSaveBolt = new MongoUpdateBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", updateQuery, mapper, WriteConcern.NONE, true);
        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSpout("mongodb://127.0.0.1:27017", query, "storm_mongospout_test.aggregation"), 1);
        // Add a bolt
        builder.setBolt("sum", new FullDocumentSummarizer(), 1).allGrouping("mongodb");
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
            if(result != null && ((Integer)result.get("sum")).intValue() == 4950) {
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
        Mongo mongo = new Mongo("localhost", 27017);
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter("storm_mongospout_test", "aggregation", latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Query to filter
        DBObject query = findLastOpLogEntry(mongo);

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

        // Create a mongo bolt
        MongoInsertBolt mongoInserBolt = new MongoInsertBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", mapper, WriteConcern.NONE);
        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSpout("mongodb://127.0.0.1:27017", query, "storm_mongospout_test.aggregation"), 1);
        // Add a bolt
        builder.setBolt("sum", new FullDocumentSummarizer(), 1).allGrouping("mongodb");
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
            if(collection.count() == 100) {
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
