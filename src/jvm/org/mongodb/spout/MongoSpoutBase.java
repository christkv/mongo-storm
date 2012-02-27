package org.mongodb.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import org.apache.log4j.Logger;
import org.mongodb.MongoObjectGrabber;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class MongoSpoutBase implements IRichSpout {
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
    private ExecutorService threadExecutor;
    private String[] collectionNames;

    public MongoSpoutBase(String url, String dbName, String[] collectionNames, DBObject query, MongoObjectGrabber mapper) {
        this.url = url;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.query = query;
        this.mapper = mapper == null ? wholeDocumentMapper : mapper;
    }

    @Override
    public boolean isDistributed() {
        return false;
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

        Mongo mongo = null;
        DB db = null;

        // Open the db connection
        try {
            MongoURI uri = new MongoURI(this.url);
            // Create mongo instance
            mongo = new Mongo();
            // Get the db the user wants
            db = mongo.getDB(this.dbName == null ? uri.getDatabase() : this.dbName);
            // If we need to authenticate do it
            if(uri.getUsername() != null) {
                db.authenticate(uri.getUsername(), uri.getPassword());
            }
        } catch (UnknownHostException e) {
            // Log the error
            LOG.error("Unknown host for Mongo DB", e);
            // Die fast
            throw new RuntimeException(e);
        }

        // Set up an executor
        this.spoutTask = new MongoSpoutTask(this.queue, mongo, db, this.collectionNames, this.query);
        // Start thread
        Thread thread = new Thread(this.spoutTask);
        thread.start();
    }

    @Override
    public void close() {
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
