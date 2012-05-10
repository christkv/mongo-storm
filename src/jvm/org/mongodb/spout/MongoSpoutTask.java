package org.mongodb.spout;

import com.mongodb.*;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

// We need to handle the actual messages in an internal thread to ensure we never block, so we will be using a non blocking queue between the
// driver and the db
class MongoSpoutTask implements Callable<Boolean>, Runnable, Serializable {
  private static final long serialVersionUID = 4440209304544126477L;
  static Logger LOG = Logger.getLogger(MongoSpoutTask.class);

  private LinkedBlockingQueue<DBObject> queue;
  private Mongo mongo;
  private DB db;
  private DBCollection collection;
  private DBCursor cursor;


  // Keeps the running state
  private AtomicBoolean running = new AtomicBoolean(true);
  private String[] collectionNames;
  private DBObject query;

  public MongoSpoutTask(LinkedBlockingQueue<DBObject> queue, String url, String dbName, String[] collectionNames, DBObject query) {
    this.queue = queue;
    this.collectionNames = collectionNames;
    this.query = query;

    initializeMongo(url, dbName);
  }

  private void initializeMongo(String url, String dbName) {
    // Open the db connection
    try {
      MongoURI uri = new MongoURI(url);
      // Create mongo instance
      mongo = new Mongo();
      // Get the db the user wants
      db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
      // If we need to authenticate do it
      if (uri.getUsername() != null) {
        db.authenticate(uri.getUsername(), uri.getPassword());
      }
    } catch (UnknownHostException e) {
      // Log the error
      LOG.error("Unknown host for Mongo DB", e);
      // Die fast
      throw new RuntimeException(e);
    }
  }

  public void stopThread() {
    running.set(false);
  }

  @Override
  public Boolean call() throws Exception {
    String collectionName = locateValidOpCollection(collectionNames);
    if (collectionName == null)
      throw new Exception("Could not locate any of the collections provided or not capped collection");
    // Set up the collection
    this.collection = this.db.getCollection(collectionName);
    // provide the query object
    this.cursor = this.collection.find(query)
            .sort(new BasicDBObject("$natural", 1))
            .addOption(Bytes.QUERYOPTION_TAILABLE)
            .addOption(Bytes.QUERYOPTION_AWAITDATA)
            .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

    // While the thread is set to running
    while (running.get()) {
      try {
        // Check if we have a next item in the collection
        if (this.cursor.hasNext()) {
          if (LOG.isInfoEnabled()) LOG.info("Fetching a new item from MongoDB cursor");
          // Fetch the next object and push it on the queue
          this.queue.put(this.cursor.next());
        } else {
          // Sleep for 50 ms and then wake up
          Thread.sleep(50);
        }
      } catch (Exception e) {
        if (running.get()) throw new RuntimeException(e);
      }
    }

    // Dummy return
    return true;
  }

  private String locateValidOpCollection(String[] collectionNames) {
    // Find a valid collection (used for oplogs etc)
    String collectionName = null;
    for (int i = 0; i < collectionNames.length; i++) {
      String name = collectionNames[i];
      // Attempt to read from the collection
      DBCollection collection = this.db.getCollection(name);
      // Attempt to find the last item in the collection
      DBCursor lastCursor = collection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
      if (lastCursor.hasNext() && collection.isCapped()) {
        collectionName = name;
        break;
      }
    }
    // return the collection name
    return collectionName;
  }

  @Override
  public void run() {
    try {
      call();
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
