package org.mongodb.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.mongodb.MongoObjectGrabber;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public abstract class MongoSpoutBase implements IRichSpout {
    static final long serialVersionUID = 737015318148636460L;
    static Logger LOG = Logger.getLogger(MongoSpoutBase.class);
    protected static LinkedHashMap<String, MongoObjectGrabber> wholeDocumentFields = null;

    // Hard coded static for whole document grab
    static {
        wholeDocumentFields = new LinkedHashMap<String, MongoObjectGrabber>();
        wholeDocumentFields.put("document", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                return object;
            }
        });
    }

    // Internal state
    private String dbName;
    private DBObject query;
    protected Map<String, MongoObjectGrabber> fields;

    // Storm variables
    protected Map conf;
    protected TopologyContext context;
    protected SpoutOutputCollector collector;

    // Handles the incoming messages
    protected ConcurrentLinkedQueue<DBObject> queue = new ConcurrentLinkedQueue<DBObject>();
    private String url;
    private MongoTask task;
    private ExecutorService threadExecutor;
    private String[] collectionNames;

    public MongoSpoutBase(String url, String dbName, String[] collectionNames, DBObject query,  LinkedHashMap<String, MongoObjectGrabber> fields) {
        this.url = url;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.query = query;
        this.fields = fields == null ? wholeDocumentFields : fields;
    }

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Map out all the keys we allow
        List<String> keys = new ArrayList<String>();
        for (Iterator<String> iterator = this.fields.keySet().iterator(); iterator.hasNext(); ) {
            String key = iterator.next();
            keys.add(key);
        }

        // Set the declaration
        declarer.declare(new Fields(keys));
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
            // Create mongo instance
            mongo = new Mongo(new MongoURI(this.url));
            // Get the db the user wants
            db = mongo.getDB(this.dbName);
        } catch (UnknownHostException e) {
            LOG.error("Unknown host for Mongo DB", e);
            return;
        }

        // Set up an executor
        this.task = new MongoTask(this.queue, mongo, db, this.collectionNames, this.query);
        // Start thread
        Thread thread = new Thread(this.task);
        thread.start();
    }

    @Override
    public void close() {
        this.task.stopThread();
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

    protected List<Object> mapObjectToTuples(DBObject object) {
        // The final tuple list
        List<Object> tuples = new ArrayList<Object>();

        // Fetch all the entries and then perform the mapping
        Set<Map.Entry<String, MongoObjectGrabber>> entries = this.fields.entrySet();

        // Iterate over all the entries
        for (Iterator<Map.Entry<String, MongoObjectGrabber>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Map.Entry<String, MongoObjectGrabber> entry = (Map.Entry<String, MongoObjectGrabber>) iterator.next();

            // Attemp to map to the object
            // If nothing is available set to null value
            try {
                tuples.add(entry.getValue().grab(object));
            } catch(Exception e) {
                tuples.add(null);
            }
        }
        // Return all the tuples
        return tuples;
    }
}
