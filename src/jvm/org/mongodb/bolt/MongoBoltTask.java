package org.mongodb.bolt;

import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.UpdateQueryCreator;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class MongoBoltTask implements Runnable, Serializable {

    private static final long serialVersionUID = -6501658936124868951L;
    static Logger LOG = Logger.getLogger(MongoBoltTask.class);
    private AtomicBoolean running = new AtomicBoolean(true);

    // Internal variables
    protected LinkedBlockingQueue<Tuple> queue;
    protected Mongo mongo;
    protected DB db;
    protected UpdateQueryCreator updateQueryCreator;
    protected StormMongoObjectGrabber mapper;
    protected WriteConcern writeConcern;
    protected DBCollection collection;

    public void stopThread() {
        running.set(false);
    }

    public MongoBoltTask(LinkedBlockingQueue<Tuple> queue, Mongo mongo, DB db, DBCollection collection, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
        this.queue = queue;
        this.mongo = mongo;
        this.db = db;
        this.collection = collection;
        this.mapper = mapper;
        this.writeConcern = writeConcern;
    }

    public MongoBoltTask(LinkedBlockingQueue<Tuple> queue, Mongo mongo, DB db, DBCollection collection, UpdateQueryCreator updateQueryCreator, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
        this.queue = queue;
        this.mongo = mongo;
        this.db = db;
        this.collection = collection;
        this.updateQueryCreator = updateQueryCreator;
        this.mapper = mapper;
        this.writeConcern = writeConcern;
    }

    @Override
    public void run() {
        // While the thread is set to running
        while(running.get()) {
            try {
                Tuple tuple = queue.poll();

                // Check if we have a next item in the collection
                if(tuple != null) {
                    if(LOG.isInfoEnabled()) LOG.info("Insert document");
                    // Execute the tuple
                    execute(tuple);
                } else {
                    // Sleep for 50 ms and then wake up
                    Thread.sleep(50);
                }
            } catch (Exception e) {
                if(running.get()) throw new RuntimeException(e);
            }
        }
    }

    public abstract void execute(Tuple tuple);
}
