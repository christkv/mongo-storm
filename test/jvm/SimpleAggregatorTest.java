import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.mongodb.*;
import org.bson.BSONObject;
import org.junit.Test;
import org.mongodb.MongoObjectGrabber;
import org.mongodb.spout.MongoOpLogSpout;
import backtype.storm.topology.IBasicBolt;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

// Inserts some docs
class Inserter implements Runnable {
    private CountDownLatch latch;

    Inserter(CountDownLatch latch) {
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
            DB db = mongo.getDB("storm_mongospout_test");
            // Drop the db for the test
            db.dropDatabase();
            // Holds our collection for the oplog
            DBCollection collection = db.getCollection("aggregation");
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
                collection.insert(object);
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

        System.out.println("================================ sum :: " + sum);

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
}

public class SimpleAggregatorTest implements Serializable {

    @Test
    public void simpleAggregator() {
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter(latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // A Mapper from an object to spout
        LinkedHashMap<String, MongoObjectGrabber> fields = new LinkedHashMap<String, MongoObjectGrabber>();

        // Use the mappers to grab the right value, here we are mapping the operation
        fields.put("o", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                return object.get("op").toString();
            }
        });

        // Here are map the _id, note difference when using the update vs insert/remove
        fields.put("_id", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                if(object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
                    return ((BSONObject)object.get("o")).get("_id").toString();
                } else {
                    return ((BSONObject)object.get("o2")).get("_id").toString();
                }

            }
        });

        // Here are map the _id, note difference when using the update vs insert/remove
        fields.put("a", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                return ((BSONObject)object.get("o")).get("a");
            }
        });

        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSpout("localhost", 27017, "storm_mongospout_test.aggregation", fields), 1);
        // Add a bolt
        builder.setBolt("sum", new Summarizer(), 1).allGrouping("mongodb");

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);

        // Run on local cluster
        LocalCluster cluster = new LocalCluster();

        // Submit the topology
        cluster.submitTopology("test", conf, builder.createTopology());
        // Starts inserts
        latch.countDown();

        // Sleep for a bit then kill the topology
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
