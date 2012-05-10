package org.mongodb;

import backtype.storm.tuple.Tuple;
import com.mongodb.DBObject;

import java.io.Serializable;

public abstract class UpdateQueryCreator implements Serializable {

  public abstract DBObject createQuery(Tuple tuple);

}
