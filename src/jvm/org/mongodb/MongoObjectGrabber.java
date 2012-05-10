package org.mongodb;

import com.mongodb.DBObject;

import java.io.Serializable;
import java.util.List;

public abstract class MongoObjectGrabber implements Serializable {

  private static final long serialVersionUID = 7265794696380763567L;

  public abstract List<Object> map(DBObject object);

  public abstract String[] fields();

}
