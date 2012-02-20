package org.mongodb;

import com.mongodb.DBObject;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: christiankvalheim
 * Date: 2/20/12
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class MongoObjectGrabber implements Serializable {
    
    public abstract Object grab(DBObject object);
}
