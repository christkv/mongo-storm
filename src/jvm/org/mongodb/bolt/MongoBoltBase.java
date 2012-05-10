package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;

import java.net.UnknownHostException;
import java.util.Map;

public abstract class MongoBoltBase extends BaseRichBolt {
  static Logger LOG = Logger.getLogger(MongoBoltBase.class);

  // Bolt runtime objects
  protected Map map;
  protected TopologyContext topologyContext;
  protected OutputCollector outputCollector;

  // Constructor arguments
  protected String url;
  protected String collectionName;
  protected StormMongoObjectGrabber mapper;
  protected WriteConcern writeConcern;

  // Mongo objects
  protected Mongo mongo;
  protected DB db;
  protected DBCollection collection;

  public MongoBoltBase(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
    this.url = url;
    this.collectionName = collectionName;
    this.mapper = mapper;
    this.writeConcern = writeConcern == null ? WriteConcern.NONE : writeConcern;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    // Save all the objects from storm
    this.map = map;
    this.topologyContext = topologyContext;
    this.outputCollector = outputCollector;

    // Attempt to open a db connection
    try {
      MongoURI uri = new MongoURI(this.url);
      // Open the db
      this.mongo = new Mongo(uri);
      // Grab the db
      this.db = this.mongo.getDB(uri.getDatabase());

      // If we need to authenticate do it
      if (uri.getUsername() != null) {
        this.db.authenticate(uri.getUsername(), uri.getPassword());
      }

      // Grab the collection from the uri
      this.collection = this.db.getCollection(this.collectionName);
    } catch (UnknownHostException e) {
      // Die fast
      throw new RuntimeException(e);
    }
  }

  @Override
  public abstract void execute(Tuple tuple);

  /**
   * Lets you handle any additional emission you wish to do
   *
   * @param tuple
   */
  public abstract void afterExecuteTuple(Tuple tuple);

  @Override
  public abstract void cleanup();

  @Override
  public abstract void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);
}
