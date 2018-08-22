package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ProjectPropertie {

	public static final String KEY_SPACE = "space";

	public static SparkConf conf;
	public static SparkSession spark;
	public static SparkContext context;
	public static JavaSparkContext javaSparkContext;

	static {
		conf = new SparkConf(true).setAppName("App_name").setMaster("local[2]").set("spark.executor.memory", "1g")
				.set("spark.cassandra.connection.host", "127.0.0.1,172.17.0.2")
				.set("spark.cassandra.connection.port", "9042")
				.set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra");
		
		//This context must be used by entire application 
		context = new SparkContext(conf);
		javaSparkContext = new JavaSparkContext(context);
		
		spark = SparkSession
	              .builder()
	              .config(conf)
	              .getOrCreate();
		}

}
