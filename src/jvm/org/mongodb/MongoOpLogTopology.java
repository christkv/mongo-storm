package org.mongodb;

import com.mongodb.DBObject;
import org.bson.BSONObject;
import org.mongodb.spout.MongoOpLogSprout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import java.util.LinkedHashMap;

public class MongoOpLogTopology {
	public static void main(String[] args) {
        String host = "localhost";
        int port = 27017;
        TopologyBuilder builder = new TopologyBuilder();

        // A Mapper from an object to spout
        LinkedHashMap<String, MongoObjectGrabber> fields = new LinkedHashMap<String, MongoObjectGrabber>();

        // Use the mappers to grab the right value, here we are mapping the operation
        fields.put("o", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                return object.get("op").toString();
            }
        });

        // Here are map the _id, note difference when using the update vs insert/remove
        fields.put("_id", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                if(object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
                    return ((BSONObject)object.get("o")).get("_id").toString();
                } else {
                    return ((BSONObject)object.get("o2")).get("_id").toString();
                }

            }
        });

        // Here are map the _id, note difference when using the update vs insert/remove
        fields.put("a", new MongoObjectGrabber() {
            @Override
            public Object grab(DBObject object) {
                return ((BSONObject)object.get("o")).get("a");
            }
        });

        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSprout(host, port, "", fields));

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);
        
        // Run on local cluster
        LocalCluster cluster = new LocalCluster();

        // Submit the topology
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
