package spark.bank;

import org.apache.spark.sql.Dataset;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class TransactionPersistence {
	
	private static final String KEY_SPACE = "space";
	private static final String TABLE_NAME = "cc_transactions";

	public static void save(Dataset<Transaction> transactions) {
		CassandraJavaUtil.javaFunctions(transactions.rdd())
				.writerBuilder(KEY_SPACE, TABLE_NAME, CassandraJavaUtil.mapToRow(Transaction.class))
				.saveToCassandra();
	}
}
