package org.mongodb.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;

public class MongoUpdateBolt extends MongoBoltBase {
    private UpdateQueryCreator updateQueryCreator;

    public MongoUpdateBolt(String url, String collectionName, UpdateQueryCreator updateQueryCreator, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
        super(url, collectionName, mapper, writeConcern);
        this.updateQueryCreator = updateQueryCreator;
    }

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
