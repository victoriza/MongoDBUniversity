package es.victoriza.mongo.university.connection;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: victor
 * Date: 20/10/13
 * Time: 17:17
 * To change this template use File | Settings | File Templates.
 */
public class MongoConnection {

    public static DB getDB(String db) throws UnknownHostException {
        MongoClient mc = new MongoClient();
        return mc.getDB(db);
    }
}
