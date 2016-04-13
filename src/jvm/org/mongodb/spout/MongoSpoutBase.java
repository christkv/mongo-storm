package org.mongodb.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.mongodb.MongoObjectGrabber;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class MongoSpoutBase extends BaseRichSpout {
  static Logger LOG = Logger.getLogger(MongoSpoutBase.class);

  protected static MongoObjectGrabber wholeDocumentMapper = null;

  // Hard coded static mapper for whole document map
  static {
    wholeDocumentMapper = new MongoObjectGrabber() {
      @Override
      public List<Object> map(DBObject object) {
        List<Object> tuple = new ArrayList<Object>();
        tuple.add(object);
        return tuple;
      }

      @Override
      public String[] fields() {
        return new String[]{"document"};
      }
    };
  }

  // Internal state
  private String dbName;
  private DBObject query;
  protected MongoObjectGrabber mapper;
  protected Map<String, MongoObjectGrabber> fields;

  // Storm variables
  protected Map conf;
  protected TopologyContext context;
  protected SpoutOutputCollector collector;

  // Handles the incoming messages
  protected LinkedBlockingQueue<DBObject> queue = new LinkedBlockingQueue<DBObject>(10000);
  private String url;
  private MongoSpoutTask spoutTask;
  private String[] collectionNames;

  public MongoSpoutBase(String url, String dbName, String[] collectionNames, DBObject query, MongoObjectGrabber mapper) {
    this.url = url;
    this.dbName = dbName;
    this.collectionNames = collectionNames;
    this.query = query;
    this.mapper = mapper == null ? wholeDocumentMapper : mapper;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // Set the declaration
    declarer.declare(new Fields(this.mapper.fields()));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    // Save parameters from storm
    this.conf = conf;
    this.context = context;
    this.collector = collector;

    // Set up an executor
    this.spoutTask = new MongoSpoutTask(this.queue, this.url, this.dbName, this.collectionNames, this.query);
    // Start thread
    Thread thread = new Thread(this.spoutTask);
    thread.start();
  }

  @Override
  public void close() {
    // Stop the thread
    this.spoutTask.stopThread();
  }

  protected abstract void processNextTuple();

  @Override
  public void nextTuple() {
    processNextTuple();
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }
}
