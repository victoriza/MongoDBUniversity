package es.victoriza.mongo.university.w2;

import com.mongodb.*;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: victor
 * Date: 20/10/13
 * Time: 18:01
 * To change this template use File | Settings | File Templates.
 */
public class HomeWork22 {

    private final static Logger log = Logger.getLogger(HomeWork22.class.getName());

    public static void main(String[] args) throws UnknownHostException {

        MongoClient mc = new MongoClient();
        DB db = mc.getDB("students");

        DBCollection collection = db.getCollection("grades");

        DBCursor cursor = collection.find(new BasicDBObject("type","homework"));
        cursor.sort(new BasicDBObject("score",1));

        HashMap students = new HashMap();
        while (cursor.hasNext()) {
            DBObject cur = cursor.next();
            //we add the student
            String studentId = cur.get("student_id").toString();
            String id = cur.get("_id").toString();
            //if not already deleted
            log.info("CHecking: " + studentId + " score:" + cur.get("score"));
            if (!students.containsKey(studentId)) {
                log.info("Adding the student stId: " + studentId);

                //we add the student
                students.put(studentId, id);

                collection.remove(new BasicDBObject("_id",new ObjectId(id)));

            } else {
                log.info("@@@@@@@@@@@@@ already deleted:"+studentId);
            }
        }


    }

}

