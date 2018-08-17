package spark.connector;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;

public class ConnectionExample {

	public static void generateSparkContext() {
		SparkConf conf = new SparkConf(true).setAppName("App_name").setMaster("local[2]")
				.set("spark.executor.memory", "1g").set("spark.cassandra.connection.host", "127.0.0.1")
				.set("spark.cassandra.connection.port", "9042").set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra");

		SparkContext ctx = new SparkContext(conf);

		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ctx);

		// Mapping Row data to Java types
		JavaRDD<User> userRDD = functions.cassandraTable("space", "user", CassandraJavaUtil.mapRowTo(User.class));
		userRDD.cache();
		userRDD.collect().stream().forEach(u -> {
			System.out.println(u.getUsername());
		});
		;

	}

	public static void main(String[] args) {
		ConnectionExample.generateSparkContext();
	}

}
