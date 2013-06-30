# The Mongo Storm library

The Mongo Storm library let's you leverage MongoDB in your Storm toplogies. It includes two main components.

* A spout for the MongoDB oplog and any MongoDB capped collections, that lets you filter down documents to specific touples in case you want to lower the amount of data transmitted from your spout. Alternatively you can emit the entire document. It's threaded and non-blocking.
* A insert/update bolt that lets you map a tuple either to  a new document or to an existing document in your MongoDB instance.

## Capped Collection Spout Examples
The capped collection signature is as follows, there are other variations as some options are optional such as the query and mapper.

```java
MongoCappedCollectionSpout(String url, String collectionName, DBObject query, MongoObjectGrabber mapper)
```
The options are
	
	* url : the Mongodb connection url
	* collectionName : the name of the capped colletion we wish to spout from
	* query[optional] : a filtering query if we wish to only extract specific documents
	* mapper[optional] : an optional mapper instance that maps the object to a tuple list

### A simple capped collection Spout an object mapped to a custom tuple
This simple example extracts the number of users from document in a capped collection as they arrive come in. Boilercode omited for brevities sake.

```java
...
// Parameters used in the cap
String url = "mongodb://127.0.0.1:27017/storm_mongospout_test";
String collectionName = "aggregation";
MongoCappedCollectionSpout spout = null;

// Map the mongodb object to a tuple
MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
  @Override
  public List<Object> map(DBObject object) {
    List<Object> tuple = new ArrayList<Object>();
    // Add the a variable
    tuple.add(object.get("number_of_users"));
    // Return the mapped object
    return tuple;
  }

  @Override
  public String[] fields() {
     return new String[]{"number_of_users"};
  }
};

// Create the spout
spout = new MongoCappedCollectionSpout(url, collectionName, mongoMapper);

// Add to the topology
builder.setSpout(spout, 1);
…
```

Two thing to notice in the code that are important. To map fields form an object to a tuple you need to create an instance implementing the **MongoObjectGrabber** and implement the two methods **map** and **fields**. The **map** function actually maps the **DBOject** to a tuple list and the **fields** method lets Storm know what the name of each tuple field is called.

### A simple capped collection Spout emitting entire documents
This simple example extracts the number of users from document in a capped collection as they arrive come in. Boilercode omited for brevities sake.

```java
...
// Parameters used in the cap
String url = "mongodb://127.0.0.1:27017/storm_mongospout_test";
String collectionName = "aggregation";
MongoCappedCollectionSpout spout = null;

// Create the spout
spout = new MongoCappedCollectionSpout(url, collectionName);

// Add to the topology
builder.setSpout(spout, 1);
…
```

If you don't provide a **MongoObjectGrabber** instance it will default to emitting the entire document. That means a tuple will be emitted under the name "document". The corresponding mapper would look like this.

```java
// Map the mongodb object to a tuple
MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
  @Override
  public List<Object> map(DBObject object) {
    List<Object> tuple = new ArrayList<Object>();
    // Add the a variable
    tuple.add(object);
    // Return the mapped object
    return tuple;
  }

  @Override
  public String[] fields() {
     return new String[]{"document"};
  }
};
```

## Oplog Spout Example
```java
MongoOpLogSpout(String url, DBObject query, String filterByNamespace, MongoObjectGrabber mapper)
```
The options are
	
	* url : the Mongodb connection url
	* query[optional] : a filtering query if we wish to only extract specific documents
	* filterByNamespace[optional] : the name space to filter the oplog by
	* mapper[optional] : an optional mapper instance that maps the object to a tuple list

### A simple oplog Spoutm, object mapped to a custom tuple from start of oplog
The oplog is a special capped collection that replicasets and master-slave configurations of MongoDB uses to synchronize. It's perfect when you wish to listen to global document changes happening on your server for such things as aggregating information or indexing documents.

It's a little bit different in usage than the Capped collection spout but not by much. The example below shows the usage of the spout starting at the start of the current oplog. It's picking out the operation, the id of the document and age of the user document.

```java
...
// Parameters used in the cap
String url = "mongodb://127.0.0.1:27017";
String filter = "storm_mongospout_test.users";
MongoCappedCollectionSpout spout = null;

// Map the mongodb object to a tuple
mongoMapper = new MongoObjectGrabber() {
  @Override
  public List<Object> map(DBObject object) {
    List<Object> tuple = new ArrayList<Object>();
    // Add the op
    tuple.add(object.get("op").toString());
    // Add the id
    if(object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
      tuple.add(((BSONObject)object.get("o")).get("_id").toString());
    } else {
      tuple.add(((BSONObject)object.get("o2")).get("_id").toString());
    }
    // Add the a variable
    tuple.add(((BSONObject)object.get("o")).get("age"));
    // Return the mapped object
    return tuple;
  }
    
  @Override
  public String[] fields() {
    return new String[]{"o", "_id", "a"};
  }
};

// Create the spout, only accepting document from storm_mongospout_test.users
spout = new MongoOpLogSpout(url, filter, mongoMapper);

// Add to the topology
builder.setSpout(spout, 1);
…
```

### A simple oplog Spout an object mapped to a custom tuple form the end of the oplog
The example below shows the usage of the spout starting at the end of the current oplog. It's picking out the operation, the id of the document and age of the user document.

```java
...
// Parameters used in the cap
String url = "mongodb://127.0.0.1:27017";
String filter = "storm_mongospout_test.users";
DBObject query = null;
MongoCappedCollectionSpout spout = null;

// Connect to the db and find the current last timestamp
DB db = mongo.getDB("local");
DBObject query = null;
DBCursor cursor = db.getCollection("oplog.$main").find().sort(new BasicDBObject("$natural", -1)).limit(1);
if(cursor.hasNext()) {
  // Get the next object
  DBObject object = cursor.next();
  // Build the query
  query = new BasicDBObject("ts", new BasicDBObject("$gt", object.get("ts")));
}

// Map the mongodb object to a tuple
mongoMapper = new MongoObjectGrabber() {
  @Override
  public List<Object> map(DBObject object) {
    List<Object> tuple = new ArrayList<Object>();
    // Add the op
    tuple.add(object.get("op").toString());
    // Add the id
    if(object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
      tuple.add(((BSONObject)object.get("o")).get("_id").toString());
    } else {
      tuple.add(((BSONObject)object.get("o2")).get("_id").toString());
    }
    // Add the a variable
    tuple.add(((BSONObject)object.get("o")).get("age"));
    // Return the mapped object
    return tuple;
  }
    
  @Override
  public String[] fields() {
    return new String[]{"o", "_id", "a"};
  }
};

// Create the spout, only accepting document from storm_mongospout_test.users
spout = new MongoOpLogSpout(url, query, filter, mongoMapper);

// Add to the topology
builder.setSpout(spout, 1);
…
```

## MongoDB Insert/Update Bolt Examples
The bolt let's you save tuples to a mongodb database collection, by inserting a new document or updating an existing one. You can subclass them to make the passthrough bolts that just save the state of the document before passing it along to other topology processors.

The insert MongoDB bolt has the following signature for the constructor.

```java
MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread)
```

The options are
	
	* url : the Mongodb connection url
	* collectionName : the name of the capped colletion we wish to spout from
	* mapper[optional] : an optional mapper instance that maps the object to a tuple list
	* writeConcern : the write concern for the mongoDB inserts (safe, normal, w) see mongo docs
	* inThread[optional] :  run the inserts/updates in it's own internal thread
	
The update MongoDB bolt has the following signature for the constructor

```java
MongoUpdateBolt(String url, String collectionName, UpdateQueryCreator updateQueryCreator, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread)
```

The options are
	
	* url : the Mongodb connection url
	* collectionName : the name of the capped colletion we wish to spout from
	* updateQueryCreator : the instance that creates the update statement for MongoDB
	* mapper : the instance that maps the tuple to the update statement for MongoDB
	* writeConcern : the write concern for the mongoDB inserts (safe, normal, w) see mongo docs
	* inThread[optional] :  run the inserts/updates in it's own internal thread

### A simple insert Bolt example
This simple example maps a aggregated tuple with a field called sum and inserts a new document for each new sum. It uses **WriteConcern.NONE** doing no safe writes when writing to MongoDB.

```java
// Mongo insert bolt instance
String url = "mongodb://127.0.0.1:27017/storm_mongospout_test";
String collectionName = "stormoutputcollection";
MongoInsertBolt mongoInserBolt = null;

// Field mapper
StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
    @Override
    public DBObject map(DBObject object, Tuple tuple) {
        return BasicDBObjectBuilder.start()
                .add( "sum" , tuple.getIntegerByField("sum"))
                .add( "timestamp", new Date())
                .get();
    }
};

// Create a mongo bolt
mongoInserBolt = new MongoInsertBolt(url, collectionName, mapper, WriteConcern.NONE);
// Add it to the builder accepting content from the sum bolt/spout
builder.setBolt("mongo", mongoInserBolt, 1).allGrouping("sum");
```

### A simple update Bolt example
This simple example maps a aggregated tuple with a field called sum and updates an existing document for each new incoming sum. It uses **WriteConcern.NONE** doing no safe writes when writing to MongoDB.

```java
// Mongo update bolt instance
String url = "mongodb://127.0.0.1:27017/storm_mongospout_test";
String collectionName = "stormoutputcollection";
UpdateQueryCreator updateQuery = null;
StormMongoObjectGrabber mapper = null;
MongoUpdateBolt mongoSaveBolt = null;

// The update query
updateQuery = new UpdateQueryCreator() {
  @Override
  public DBObject createQuery(Tuple tuple) {
    return new BasicDBObject("aggregation_doc", "summary");
  }
};

// Field mapper
mapper = new StormMongoObjectGrabber() {
  @Override
  public DBObject map(DBObject object, Tuple tuple) {
    return BasicDBObjectBuilder.start().push( "$set" ).add( "sum" , tuple.getIntegerByField("sum")).get();
  }
};

// Create a mongo bolt
mongoSaveBolt = new MongoUpdateBolt(url, collectionName, updateQuery, mapper, WriteConcern.NONE);

```

### A simple passthrough insert Bolt example
This is a simple example showing how you can extend the insert class to pass the values through as well as save them. Useful if you are saving state in the middle of a topology.

```java
class ExtendInsertClass extends MongoInsertBolt {
  public ExtendInsertClass(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
    super(url, collectionName, mapper, writeConcern);
  }

  @Override
  public void afterExecuteTuple(Tuple tuple) {
    this.outputCollector.emit(tuple.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("sum"));
  }
}

// Mongo insert bolt instance
String url = "mongodb://127.0.0.1:27017/storm_mongospout_test";
String collectionName = "stormoutputcollection";
MongoInsertBolt mongoInserBolt = null;

// Field mapper
StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
    @Override
    public DBObject map(DBObject object, Tuple tuple) {
        return BasicDBObjectBuilder.start()
                .add( "sum" , tuple.getIntegerByField("sum"))
                .add( "timestamp", new Date())
                .get();
    }
};

// Create a mongo bolt
mongoInserBolt = new ExtendInsertClass(url, collectionName, mapper, WriteConcern.NONE);
// Add it to the builder accepting content from the sum bolt/spout
builder.setBolt("mongo", mongoInserBolt, 1).allGrouping("sum");
```

License
=======

 Copyright 2009 - 2012 Christian Amor Kvalheim.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.







