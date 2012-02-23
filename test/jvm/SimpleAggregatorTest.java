import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.bson.BSONObject;
import org.junit.Test;
import org.mongodb.MongoObjectGrabber;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;
import org.mongodb.bolt.MongoUpdateBolt;
import org.mongodb.spout.MongoOpLogSpout;
import backtype.storm.topology.IBasicBolt;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
                collection.insert(object, WriteConcern.SAFE);
//                System.out.println("--------------------------------------------------- inserted record");
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
}

public class SimpleAggregatorTest implements Serializable {

    @Test
    public void simpleAggregator() throws UnknownHostException {
        // Signals thread to fire messages
        CountDownLatch latch = new CountDownLatch(1);
        // Wraps the thread
        Inserter inserter = new Inserter(latch);
        // Runs inserts in a thread
        new Thread(inserter).start();

        // Query to filter
        DBObject query = null;

        // Connect to the db and find the current last timestamp
        Mongo mongo = new Mongo("localhost", 27017);
        DB db = mongo.getDB("local");
        DBCursor cursor = db.getCollection("oplog.$main").find().sort(new BasicDBObject("$natural", -1)).limit(1);
        if(cursor.hasNext()) {
            // Get the next object
            DBObject object = cursor.next();
            // Build the query
            query = new BasicDBObject("ts", new BasicDBObject("$gt", object.get("ts")));
            System.out.println(query.toString());
        }

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // Map the mongodb object to a tuple
        MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
            @Override
            public List<Object> map(DBObject object) {
                List<Object> tuple = new ArrayList<Object>();

                // Add the op
                tuple.add(object.get("op").toString());

                // Add the id
                if(object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
                    tuple.add(((BSONObject)object.get("o")).get("_id").toString());
                } else {
                    tuple.add(((BSONObject)object.get("o2")).get("_id").toString());
                }
                
                // Add the a variable
                tuple.add(((BSONObject)object.get("o")).get("a"));

                // Return the mapped object
                return tuple;
            }

            @Override
            public String[] fields() {
                return new String[]{"o", "_id", "a"};
            }
        };

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

//        // Field mapper
//        StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
//            @Override
//            public DBObject map(DBObject object, Tuple tuple) {
//                object.put("sum", tuple.getIntegerByField("sum"));
//                return object;
//            }
//        };

        // Create a mongo bolt
//        MongoBoltBase mongoBoltBase = new MongoInsertBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", mapper, WriteConcern.NONE);
        MongoUpdateBolt mongoSaveBolt = new MongoUpdateBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", updateQuery, mapper, WriteConcern.NONE);

        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSpout("mongodb://127.0.0.1:27017", query, "storm_mongospout_test.aggregation", mongoMapper), 1);
        // Add a bolt
        builder.setBolt("sum", new Summarizer(), 1).allGrouping("mongodb");
        builder.setBolt("mongo", mongoSaveBolt, 2).allGrouping("sum");

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
        db = mongo.getDB("storm_mongospout_test");
        DBCollection collection = db.getCollection("stormoutputcollection");
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
                } catch (InterruptedException e) {
                }
            }
        }

        // Sleep for a bit then kill the topology
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
