package org.mongodb.enron;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.mongodb.*;
import org.mongodb.MongoObjectGrabber;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;
import org.mongodb.bolt.MongoUpdateBolt;
import org.mongodb.spout.MongoOpLogSpout;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EnronEmailWordCounts {
  static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(EnronEmailWordCounts.class);

  public static void main(String[] args) throws UnknownHostException {
    // Create a mapper from documents emitted from the spout to the ones
    // we wish to process
    MongoObjectGrabber mapsOplogItemsToTuples = new MongoObjectGrabber() {
      @Override
      public List<Object> map(DBObject object) {
        // The tuple we are returning
        List<Object> tuple = new ArrayList<Object>();

        // If it's a new document let's rip out the body of the email for processing and the _id field
        if (object.get("op").toString().equals("i")) {
          // Rip out the body
          tuple.add(((DBObject) object.get("o")).get("_id"));
          tuple.add(((DBObject) object.get("o")).get("body"));
        } else {
          tuple.add(null);
          tuple.add(null);
        }

        // Return the mapped object
        return tuple;
      }

      @Override
      public String[] fields() {
        return new String[]{"_id", "body"};
      }
    };

    // Set up
    MongoOpLogSpout spout = new MongoOpLogSpout("mongodb://127.0.0.1:27017", "enron.email", mapsOplogItemsToTuples);

    // Create a word counting bold
    IBasicBolt wordCountingBolt = new IBasicBolt() {
      @Override
      public void prepare(Map map, TopologyContext topologyContext) {
      }

      @Override
      public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (tuple.get("_id") != null) {
          // Grab the _id field so we can pass it on for a save later
          Object _id = tuple.get("_id");
          // Grab the text body
          String body = tuple.get("body").toString();
          // Clean up the text split it and count
          String[] numberOfWords = body.replace("\n\n", " ").split(" ");
          // Create the tuple result
          List<Object> resultTuple = new ArrayList<Object>();
          resultTuple.add(_id);
          resultTuple.add(numberOfWords.length);
          // Emit the result
          basicOutputCollector.emit(resultTuple);
        }
      }

      @Override
      public void cleanup() {
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("_id", "body_word_count"));
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }
    };

    // Update Query for document
    UpdateQueryCreator updateQueryCreator = new UpdateQueryCreator() {
      @Override
      public DBObject createQuery(Tuple tuple) {
        // Pick the document based on the _id passed in
        return new BasicDBObject("_id", tuple.get("_id"));
      }
    };

    // Field mapper
    StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
      @Override
      public DBObject map(DBObject object, Tuple tuple) {
        return BasicDBObjectBuilder.start().push("$set").add("body_word_count", tuple.getIntegerByField("body_word_count")).get();
      }
    };

    // Create a mongodb update bolt that will update documents adding the wordcount variable
    MongoUpdateBolt mongoUpdateBolt = new MongoUpdateBolt("mongodb://127.0.0.1:27017/enron", "email", updateQueryCreator, mapper, WriteConcern.SAFE);

    // Set up Localcluster
    LocalCluster cluster = new LocalCluster();

    // Build a topology
    TopologyBuilder builder = new TopologyBuilder();
    // Set the spout
    builder.setSpout("mongodb", spout, 1);
    // Add the bolt to count the number of words in each email
    builder.setBolt("words", wordCountingBolt, 20).shuffleGrouping("mongodb");
    // Save the word count back to the db
    builder.setBolt("mongoout", mongoUpdateBolt, 10).shuffleGrouping("words");

    // Set debug config
    Config conf = new Config();
    conf.setDebug(false);
    conf.setOptimize(true);

    // Submit a job
    cluster.submitTopology("enron", conf, builder.createTopology());
  }
}
