package spark.connector;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;

public class ConnectionExample {

	private static SparkConf conf;
	private static SparkSession spark;
	private static SparkContext context;

	static {
		conf = new SparkConf(true).setAppName("App_name").setMaster("local[2]").set("spark.executor.memory", "1g")
				.set("spark.cassandra.connection.host", "127.0.0.1,172.17.0.2")
				.set("spark.cassandra.connection.port", "9042")
				.set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra");
		
		//This context must be used by entire application 
		context = new SparkContext(conf);
		
		spark = SparkSession.builder()
				.appName("Spark SQL examples")
				.master("local")
				/* ONLY ONE CONTEXT PER JVM can be used!*/
				.sparkContext(context)
				.getOrCreate();
	}

	public static void readFromCassandra() {
		
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(context);
		
		// Mapping Row data to Java types
		JavaRDD<User> userRDD = functions.cassandraTable("space", "user", CassandraJavaUtil.mapRowTo(User.class));
		userRDD.cache();
		userRDD.collect().stream().forEach(u -> {
			System.out.println(u.toString());
		});
	}

	public static void importCSVAndPersist() {
		Dataset<Row> data = spark.read()
				.csv("input.csv")
				.withColumnRenamed("_c0", "id")
				.withColumnRenamed("_c1", "user_name")
				.withColumnRenamed("_c2", "unit");
		data.show();
		
		//Parse data as table to User data set
		Dataset<User> users = data.as(Encoders.bean(User.class));
		
		//Save in Cassandra RDD object
		CassandraJavaUtil.javaFunctions(users.rdd())
				.writerBuilder("space", "user", CassandraJavaUtil.mapToRow(User.class)).saveToCassandra();
	}

	public static void main(String[] args) {
		ConnectionExample.importCSVAndPersist();
		ConnectionExample.readFromCassandra();
		
	}

}
