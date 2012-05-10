package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class MongoInsertBolt extends MongoBoltBase {
  static Logger LOG = Logger.getLogger(MongoInsertBolt.class);
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private MongoBoltTask task;
  private Thread writeThread;
  private boolean inThread;

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
    super(url, collectionName, mapper, writeConcern);
  }

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread) {
    super(url, collectionName, mapper, writeConcern);
    // Run the insert in a seperate thread
    this.inThread = inThread;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);

    // If we want to run the inserts in a separate thread
    if (this.inThread) {
      // Create a task
      this.task = new MongoBoltTask(this.queue, this.mongo, this.db, this.collection, this.mapper, this.writeConcern) {
        @Override
        public void execute(Tuple tuple) {
          // Build a basic object
          DBObject object = new BasicDBObject();
          // Map and save the object
          this.collection.insert(this.mapper.map(object, tuple), this.writeConcern);
        }
      };

      // Run the writeThread
      this.writeThread = new Thread(this.task);
      this.writeThread.start();
    }
  }

  @Override
  public void execute(Tuple tuple) {
    if (this.inThread) {
      this.queue.add(tuple);
    } else {
      try {
        DBObject object = this.mapper.map(new BasicDBObject(), tuple);
        // Map and save the object
        this.collection.insert(object, this.writeConcern);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Execute after insert action
    this.afterExecuteTuple(tuple);
  }

  @Override
  public void afterExecuteTuple(Tuple tuple) {
  }

  @Override
  public void cleanup() {
    if (this.inThread) this.task.stopThread();
    this.mongo.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}
