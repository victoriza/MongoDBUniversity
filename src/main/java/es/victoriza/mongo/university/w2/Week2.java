package es.victoriza.mongo.university.w2;

import com.mongodb.*;
import es.victoriza.mongo.university.connection.MongoConnection;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: victor
 * Date: 20/10/13
 * Time: 17:26
 * To change this template use File | Settings | File Templates.
 */
public class Week2 {

    private static final Logger log = Logger.getLogger(Week2.class.getName());

    public static void main(String[] args) throws UnknownHostException {
        DB db = MongoConnection.getDB("test");
        DBCollection col = db.getCollection("findTest");

        col.drop();
        //insert 10 docs
        for (int i = 0; i < 10; i++) {
            col.insert(new BasicDBObject()
                    .append("_id", i)
                    .append("start",
                            new BasicDBObject().append("x", new Random().nextInt(2))
                                    .append("y", new Random().nextInt(100))
                                    .append("z", new Random().nextInt(1000)))
                    .append("end",
                            new BasicDBObject().append("x", new Random().nextInt(100))
                                    .append("y", new Random().nextInt(100))
                                    .append("z", new Random().nextInt(1000))));
        }
        //the same
        QueryBuilder builder = QueryBuilder.start("start.x").is(0).and("start.y").greaterThan(1).lessThan(90);

        DBObject query = new BasicDBObject("x",0).append("y", new BasicDBObject("$gt",10).append("$lt",90));


        long count = col.count();
        log.info("Count: "+count);

        //find one
        log.info("FindOne: "+col.findOne().toString());

        //find
        DBCursor cursor = col.find(builder.get()
                //the projection
                ,new BasicDBObject("_id", false).append("coordinates.x",false))
                //a simple descending sort
                .sort(new BasicDBObject("start.x",-1))
                //skip first 20
                .skip(20)
                //limit the to 10
                .limit(10);

        log.info("Creating find cursor: ");
        try {
            while(cursor.hasNext()) {
                DBObject cur = cursor.next();
                log.info(cur.toString());
            }
        } catch (Exception e) {
            log.info("Ups an error: "+e.getMessage());
            //we close the server side cursor
            cursor.close();
        }

        //we do an update of the whole document
        col.update(QueryBuilder.start("_id").is(0).get()
                , new BasicDBObject("age",23) );

        //we add a new property to de doc
        col.update(QueryBuilder.start("_id").is(1).get()
                , new BasicDBObject("$set" , new BasicDBObject("age",23)));

        //we remove a new property to de doc
        col.update(QueryBuilder.start("_id").is(1).get()
                , new BasicDBObject("$unset" , new BasicDBObject("start.x",1)));


        //we remove a new property to de doc
        col.update(QueryBuilder.start("_id").is("juan").get()
                , new BasicDBObject("age",23) ,false,true);


        //we add a new property to de ALL docs
        //The find is empty
        col.update(new BasicDBObject()
                , new BasicDBObject("$set" , new BasicDBObject("title","A")),false,true);

        DBCursor cursor1 = col.find().sort(new BasicDBObject("_id",-1));
        while(cursor1.hasNext()) {
            log.info(cursor1.next().toString());
        }

    }

}
