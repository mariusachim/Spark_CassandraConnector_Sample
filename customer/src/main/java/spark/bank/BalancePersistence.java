package spark.bank;

import org.apache.spark.rdd.RDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class BalancePersistence {

	private static final String KEY_SPACE = "space";
	private static final String TABLE_NAME = "cc_balance";

	public static void save(RDD<Balance> rdd) {
		CassandraJavaUtil.javaFunctions(rdd).writerBuilder(KEY_SPACE, TABLE_NAME, CassandraJavaUtil.mapToRow(Balance.class)).saveToCassandra();
	}
}
