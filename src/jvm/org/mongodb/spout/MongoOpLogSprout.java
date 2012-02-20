package org.mongodb.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.types.*;
import org.mongodb.MongoObjectGrabber;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: christiankvalheim
 * Date: 2/20/12
 * Time: 12:54 PM
 */
public class MongoOpLogSprout implements IRichSpout {
    static final long serialVersionUID = 737015318148636460L;
    static Logger LOG = Logger.getLogger(MongoOpLogSprout.class);

    // Internal state
    private String host;
    private int port;
    private String namespace;
    private Map<String, MongoObjectGrabber> fields;
    private int fromTimestamp;

    // Db connection
    private Mongo mongo;
    private DB db;
    private DBCursor cursor;

    // Storm variables
    private Map conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    // Handles the incoming messages
    private LinkedBlockingQueue<DBObject> queue;

    public MongoOpLogSprout(String host, int port, String namespace, LinkedHashMap<String, MongoObjectGrabber> fields) {
        this.host = host;
        this.port = port;
        this.namespace = namespace;
        this.fields = fields;
        this.fromTimestamp = 0;
    }

    public MongoOpLogSprout(String host, int port, String namespace, LinkedHashMap<String, MongoObjectGrabber> fields, int fromTimestamp) {
        this.host = host;
        this.port = port;
        this.namespace = namespace;
        this.fields = fields;
        this.fromTimestamp = fromTimestamp;
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

        try {
            // Initialize the mongo object
            this.mongo = new Mongo(this.host, this.port);
            // Fetch the local db
            this.db = this.mongo.getDB("local");

            // Holds our collection for the oplog
            DBCollection oplogCollection = this.db.getCollection("oplog.$main");

            // Check if we have a master-slave or replicaset oplog
            DBCursor lastCursor = oplogCollection.find().sort( new BasicDBObject( "$natural" , -1 ) ).limit(1);
            if(!lastCursor.hasNext()) {
                // No master oplog, verify if we have a replicaset op log
                oplogCollection = this.db.getCollection("local.oplog.rs");
                lastCursor = oplogCollection.find().sort( new BasicDBObject( "$natural" , -1 ) ).limit(1);

                // If we don't have a cursor
                if(lastCursor.hasNext()) {
                    // Close the cursor
                    lastCursor.close();
                    // Set the cursor
                    this.cursor = oplogCollection.find();
                    cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
                    cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
                }
            } else {
                // Close the cursor
                lastCursor.close();
                // Set the cursor
                this.cursor = oplogCollection.find();
                cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
                cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        this.mongo.close();
    }

    @Override
    public void nextTuple() {
        if(this.cursor.hasNext()) {
            // Fetch the next oplog item
            DBObject object = this.cursor.next();

            String operation = object.get("op").toString();
            // Check if it's a i/d/u operation and push the data
            if(operation.equals("i") || operation.equals("d") || operation.equals("u")) {
                if(LOG.isInfoEnabled()) LOG.info(object.toString());

                // Verify if it's the correct namespace
                if(!namespace.equals(object.get("ns").toString())) {
                    return;
                }

                // If we have a fromTimestamp check if we are newer than that
                if(fromTimestamp > 0 && ((BSONTimestamp)object.get("ts")).getTime() < fromTimestamp) {
                    return;
                }
                
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

                // Contains the objectID
                String objectId = null;
                // Extract the ObjectID
                if(operation.equals("i") || operation.equals("d")) {
                  objectId = ((BSONObject)object.get("o")).get("_id").toString();
                } else if(operation.equals("u")) {
                  objectId = ((BSONObject)object.get("o2")).get("_id").toString();
                }

                // Emit the tuple collection
                this.collector.emit(tuples, objectId);
            }
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    private List<Object> mapDBObjectToTuples(DBObject object) {
        List<Object> tuples = new ArrayList<Object>();

        // Get the key set
        Set<String> set = object.keySet();
        // Iterate over all the keys
        for (Iterator<String> iterator = set.iterator(); iterator.hasNext(); ) {
            String key = iterator.next();

            if(object.get(key) instanceof DBObject) {
                List<Object> tuple = new ArrayList<Object>(2);
                tuple.add(key);
                tuple.add(mapDBObjectToTuples((DBObject) object.get(key)));
                tuples.add(tuple);
            } else if(object.get(key) instanceof Binary) {
            } else if(object.get(key) instanceof BSONTimestamp) {
            } else if(object.get(key) instanceof Code) {
            } else if(object.get(key) instanceof CodeWScope) {
            } else if(object.get(key) instanceof MaxKey) {
            } else if(object.get(key) instanceof MinKey) {
            } else if(object.get(key) instanceof ObjectId) {
            } else if(object.get(key) instanceof Symbol) {
            } else {
                List<Object> tuple = new ArrayList<Object>(2);
                tuple.add(key);
                tuple.add(object.get(key));
                tuples.add(tuple);
            }
        }

        return tuples;
    }
}
