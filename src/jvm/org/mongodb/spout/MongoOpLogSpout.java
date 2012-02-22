package org.mongodb.spout;

import com.mongodb.DBObject;
import org.bson.BSONObject;
import org.mongodb.MongoObjectGrabber;

import java.util.LinkedHashMap;
import java.util.List;

public class MongoOpLogSpout extends  MongoSpoutBase {    
    private static String[] collectionNames = {"oplog.$main", "oplog.rs"};
    private String filterByNamespace;

    public  MongoOpLogSpout(String url) {
        super(url, "local", collectionNames, null, null);
    }
    
    public MongoOpLogSpout(String url, DBObject query) {
        super(url, "local", collectionNames, query, null);
    }

    public MongoOpLogSpout(String url, String filterByNamespace) {
        super(url, "local", collectionNames, null, null);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, DBObject query, String filterByNamespace) {
        super(url, "local", collectionNames, query, null);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, LinkedHashMap<String, MongoObjectGrabber> fields) {
        super(url, "local", collectionNames, null, fields);
    }

    public MongoOpLogSpout(String url, DBObject query, LinkedHashMap<String, MongoObjectGrabber> fields) {
        super(url, "local", collectionNames, query, fields);
    }

    public MongoOpLogSpout(String url, String filterByNamespace, LinkedHashMap<String, MongoObjectGrabber> fields) {
        super(url, "local", collectionNames, null, fields);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, DBObject query, String filterByNamespace, LinkedHashMap<String, MongoObjectGrabber> fields) {
        super(url, "local", collectionNames, query, fields);
        this.filterByNamespace = filterByNamespace;
    }

    @Override
    protected void processNextTuple() {
        DBObject object = this.queue.poll();
        // If we have an object, let's process it, map and emit it
        if(object != null) {
            String operation = object.get("op").toString();
            // Check if it's a i/d/u operation and push the data
            if(operation.equals("i") || operation.equals("d") || operation.equals("u")) {
                if(LOG.isInfoEnabled()) LOG.info(object.toString());

                // Verify if it's the correct namespace
                if(this.filterByNamespace != null && !this.filterByNamespace.equals(object.get("ns").toString())) {
                    return;
                }

                // Map the object to the tuples
                List<Object> tuples = mapObjectToTuples(object);

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
}
