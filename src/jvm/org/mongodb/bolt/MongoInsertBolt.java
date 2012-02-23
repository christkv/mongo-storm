package org.mongodb.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;

public class MongoInsertBolt extends MongoBoltBase {
    static Logger LOG = Logger.getLogger(MongoInsertBolt.class);

    public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
        super(url, collectionName, mapper, writeConcern);
    }

    @Override
    public void execute(Tuple tuple) {
        // Build a basic object
        DBObject object = new BasicDBObject();
        // Map and save the object
        this.collection.insert(this.mapper.map(object, tuple), this.writeConcern);
        // Execute after insert action
        this.afterExecuteTuple(tuple);
    }

    @Override
    public void afterExecuteTuple(Tuple tuple) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
