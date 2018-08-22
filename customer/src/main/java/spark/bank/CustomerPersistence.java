package spark.bank;

import org.apache.spark.sql.Dataset;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class CustomerPersistence {

	private static final String KEY_SPACE = "space";
	private static final String TABLE_NAME = "cc_customer";

	public static void save(Dataset<Customer> customers) {
		CassandraJavaUtil.javaFunctions(customers.rdd())
				.writerBuilder(KEY_SPACE, TABLE_NAME, CassandraJavaUtil.mapToRow(Customer.class))
				.saveToCassandra();
	}

}
