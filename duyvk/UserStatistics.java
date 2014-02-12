package unitn.dii.hive;


import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Date;

import org.apache.commons.lang.time.StopWatch;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class UserStatistics {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	/**
	* @param args
	* @throws SQLException
	**/
	public static final String EVENT_TABLE = "github_events_hive";
	public static final String USER_GROUPBY_QUERY = "select actor_login as user, %2$s, count(*) as num_of_events from %1$s group by actor_login , %2$s" ;
			
	public static void updateStatistics (Statement stmt, DBCollection coll, String table, String field) throws SQLException{
		// element in row
		String user, fieldValue;
		int value;
		
		// query in hive
		String query = (String.format(USER_GROUPBY_QUERY, table, field));
		
		long ls = new Date().getTime();
		ResultSet res = stmt.executeQuery(query);
		long le = new Date().getTime();
		System.out.println("\tTime: query for "+field+": "+ (le-ls)/1000 + " s");
		
		
		long ls2 = new Date().getTime();
		while (res.next()){
			user = res.getString("user");
			fieldValue = res.getString(field);
			value = res.getInt("num_of_events");
			// put data to mongodb
			updateToMongo(coll, user, field, fieldValue, value);
		}
		
		long le2 = new Date().getTime();
		System.out.println("\tTime: update mongo for "+field+": "+ (le2-ls2)/1000 + " s");
	}
	
	public static void updateToMongo(DBCollection coll, String user,String field, String fieldValue, int value){
		
		// find the user
		BasicDBObject query = new BasicDBObject();
		query.append("user", user);
		
		DBCursor cur =  coll.find(query);
		if (cur.count()==0){
			// insert
			BasicDBObject newDoc = new BasicDBObject();
			newDoc.put("user", user);
			
			BasicDBObject fieldOb = new BasicDBObject();
			fieldOb.put(fieldValue, value);
			newDoc.put(field, fieldOb);
			
			coll.insert(newDoc);
		}else{
			// update
			BasicDBObject newDoc  = new BasicDBObject();
			newDoc.append("$set", new BasicDBObject().append((field+"."+fieldValue), value));
			
			BasicDBObject searchQuery = new BasicDBObject().append("user", user);
			coll.update(searchQuery, newDoc);
		}
		
	}
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e){
			e.printStackTrace();
			System.exit(1);
		}
		long ls = new Date().getTime();
		
		// hive collection
		Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		// mongodb connection
		Mongo mongo = null;
		try {
			mongo = new Mongo("localhost", 27017);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// mongodb db
		DB db = mongo.getDB("github_users");
				
		// get mongodb collection
		DBCollection coll = db.getCollection("users");
		
		// set unique index for user.
		coll.ensureIndex(new BasicDBObject("user", 1), new BasicDBObject("unique", true));
		
		String[] fields = {"created_at_hour", "created_at_weekday", "repository_language", "repository_id", "event_type"};
		
		// put data to mongodb
		for (String field : fields){
			System.out.println("Start update : "+ field);
			updateStatistics(stmt, coll, EVENT_TABLE, field);
			System.out.println("Stop update: "+ field);
			System.out.println("-----------");
		}
		con.close();
		
		long le = new Date().getTime();
		System.out.println("time: "+ (le-ls)/1000 + " s");
	}
}