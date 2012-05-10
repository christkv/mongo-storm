package org.mongodb.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.mongodb.MongoObjectGrabber;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.bolt.MongoInsertBolt;
import org.mongodb.spout.MongoCappedCollectionSpout;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.lang.System.exit;
import static java.lang.System.setOut;

public class SimpleCappedCollectionStoringSummaryInNewDocuments {

  public static void main(String[] args) throws UnknownHostException {
    // Open a connection
    Mongo mongo = new Mongo("localhost", 27017);
    // Drop the database
    mongo.dropDatabase("storm_mongospout_test");
    // Get db
    DB db = mongo.getDB("storm_mongospout_test");
    // Create a capped collection
    DBCollection collection = db.createCollection("aggregation", new BasicDBObject("capped", true).append("size", 100000));

    // Signals thread to fire messages
    CountDownLatch latch = new CountDownLatch(1);
    // Wraps the thread
    InsertHelper inserter = new InsertHelper("storm_mongospout_test", "aggregation", latch);
    // Runs inserts in a thread
    new Thread(inserter).start();

    // Build a topology
    TopologyBuilder builder = new TopologyBuilder();

    // Map the mongodb object to a tuple
    MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
      @Override
      public List<Object> map(DBObject object) {
        List<Object> tuple = new ArrayList<Object>();
        // Add the a variable
        tuple.add(object.get("a"));
        // Return the mapped object
        return tuple;
      }

      @Override
      public String[] fields() {
        return new String[]{"a"};
      }
    };

    // Field mapper
    StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
      @Override
      public DBObject map(DBObject object, Tuple tuple) {
        return BasicDBObjectBuilder.start()
                .add("sum", tuple.getIntegerByField("sum"))
                .add("timestamp", new Date())
                .get();
      }
    };

    // Create a mongo bolt
    MongoInsertBolt mongoInsertBolt = new MongoInsertBolt("mongodb://127.0.0.1:27017/storm_mongospout_test", "stormoutputcollection", mapper, WriteConcern.NONE, true);
    // Set the spout
    builder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://127.0.0.1:27017/storm_mongospout_test", "aggregation", mongoMapper), 1);
    // Add a bolt
    builder.setBolt("sum", new SummarizerBolt(), 1).allGrouping("mongodb");
    builder.setBolt("mongo", mongoInsertBolt, 1).allGrouping("sum");

    // Set debug config
    Config conf = new Config();
    conf.setDebug(true);

    // Run on local cluster
    LocalCluster cluster = new LocalCluster();
    // Submit the topology
    cluster.submitTopology("test2", conf, builder.createTopology());
    // Starts inserts
    latch.countDown();

    // Wait until we have the summation all done
    collection = mongo.getDB("storm_mongospout_test").getCollection("stormoutputcollection");
    // Keep polling until it's done
    boolean done = false;

    // Keep checking until done
    while (!done) {
      if (collection.count() == 100) {
        done = true;
      } else {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
        }
      }
    }

    // Stop topology
    cluster.killTopology("test2");
    cluster.shutdown();
    // Close db
    mongo.close();
    // Ensure we die
    exit(0);
  }

  static class InsertHelper implements Runnable {
    private String dbName;
    private String collectionName;
    private CountDownLatch latch;

    InsertHelper(String dbName, String collectionName, CountDownLatch latch) {
      this.dbName = dbName;
      this.collectionName = collectionName;
      this.latch = latch;
    }

    @Override
    public void run() {
      // Initialize the mongo object
      Mongo mongo;
      try {
        // Open connection
        mongo = new Mongo("localhost", 27017);
        // Fetch the local db
        DB db = mongo.getDB(dbName);
        // Holds our collection for the oplog
        DBCollection collection = db.getCollection(collectionName);
        // Now insert a bunch of docs once we are ready
        while (latch.getCount() != 0) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // Insert one hundred objects
        for (int i = 0; i < 100; i++) {
          // Create a basic object
          BasicDBObject object = new BasicDBObject();
          object.put("a", i);
          // Insert the object
          collection.insert(object, WriteConcern.SAFE);
        }

        mongo.close();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
  }

  // Bolt summarising numbers
  static class SummarizerBolt implements IBasicBolt {
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
      return null;
    }
  }
}
