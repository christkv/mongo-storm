package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class MongoUpdateBolt extends MongoBoltBase {
  private UpdateQueryCreator updateQueryCreator;
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private boolean inThread;
  private MongoBoltTask task;
  private Thread writeThread;

  public MongoUpdateBolt(String url, String collectionName, UpdateQueryCreator updateQueryCreator, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
    super(url, collectionName, mapper, writeConcern);
    this.updateQueryCreator = updateQueryCreator;
  }

  public MongoUpdateBolt(String url, String collectionName, UpdateQueryCreator updateQueryCreator, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread) {
    super(url, collectionName, mapper, writeConcern);
    this.updateQueryCreator = updateQueryCreator;
    this.inThread = inThread;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);

    // If we want to run the inserts in a separate thread
    if (this.inThread) {
      // Create a task
      this.task = new MongoBoltTask(this.queue, this.mongo, this.db, this.collection, this.updateQueryCreator, this.mapper, this.writeConcern) {
        @Override
        public void execute(Tuple tuple) {
          // Unpack the query
          DBObject updateQuery = this.updateQueryCreator.createQuery(tuple);
          // The clean update object
          DBObject mappedUpdateObject = new BasicDBObject();
          // Mapping of the object
          mappedUpdateObject = this.mapper.map(mappedUpdateObject, tuple);
          // Create the update statement
          this.collection.update(updateQuery, mappedUpdateObject, true, false, this.writeConcern);
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
      // Unpack the query
      DBObject updateQuery = this.updateQueryCreator.createQuery(tuple);
      // The clean update object
      DBObject mappedUpdateObject = new BasicDBObject();
      // Mapping of the object
      mappedUpdateObject = this.mapper.map(mappedUpdateObject, tuple);
      // Create the update statement
      this.collection.update(updateQuery, mappedUpdateObject, true, false, this.writeConcern);
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
