package spark;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.DataFrameFunctions;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;

import scala.Tuple2;
import spark.bank.Balance;
import spark.bank.BalancePersistence;
import spark.bank.Customer;
import spark.bank.CustomerPersistence;
import spark.bank.Transaction;
import spark.bank.TransactionPersistence;

/**
 * Hello world!
 *
 */
public class App {

	public static void readFromCSV() {
		Dataset<Customer> customerRecords = ProjectPropertie.spark.read().csv("customers.csv")
				.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "county").withColumnRenamed("_c2", "name")
				.as(Encoders.bean(Customer.class));
		customerRecords.show();
		CustomerPersistence.save(customerRecords);

		Dataset<Transaction> transactionRecords = ProjectPropertie.spark.read().csv("transactions.csv")
				.withColumnRenamed("_c0", "customerid").withColumnRenamed("_c1", "year")
				.withColumnRenamed("_c2", "month").withColumnRenamed("_c3", "id").withColumnRenamed("_c4", "amount")
				.withColumnRenamed("_c5", "card").withColumnRenamed("_c6", "status")
				.as(Encoders.bean(Transaction.class));
		TransactionPersistence.save(transactionRecords);
	}

	public static void generateBalanceByDate(Date date) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
		String reportDate = df.format(date);
		// Print what date is today!
		System.out.println("Report Date: " + reportDate);

		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ProjectPropertie.context);
		JavaRDD<Balance> balances = functions.cassandraTable(ProjectPropertie.KEY_SPACE, Transaction.TABLE_NAME)
				.select("customerid", "amount", "card", "status", "id").where("id < minTimeuuid(?)", date)
				.filter(row -> row.getString("status").equals("COMPLETED"))
				.keyBy(row -> new Tuple2<>(row.getString("customerid"), row.getString("card")))
				.mapToPair(row -> new Tuple2<>(row._1, row._2.getInt("amount")))
				.reduceByKey((i1, i2) -> i1.intValue() + i2.intValue())
				.flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Integer>, Balance>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Balance> call(Tuple2<Tuple2<String, String>, Integer> r) throws Exception {
						List<Balance> list = new ArrayList<Balance>();
						list.add(new Balance(r._1._1, r._1._2, r._2, reportDate));
						return list.iterator();
					}
				}).cache();

		balances.collect().stream().forEach(System.out::println);
		BalancePersistence.save(balances.rdd());
	}
	
	// how much customer spend in different counties per month
	public static void aggregationBasedOnCustomer(String county) {			
		Dataset<Row> customerTable = ProjectPropertie.spark.read().format("org.apache.spark.sql.cassandra").options(new HashMap<String,String>() {
			   {
			   put("keyspace", "space");
			   put("table", "cc_customer");
			   }}).load();
		Dataset<Row> transactionsTable =  ProjectPropertie.spark.read().format("org.apache.spark.sql.cassandra").options(new HashMap<String,String>() {
			   {
			   put("keyspace", "space");
			   put("table", "cc_transactions");
			   }}).load();
		
		Dataset<Row> innerResult = customerTable
				.join(transactionsTable, customerTable.col("id").equalTo(transactionsTable.col("customerid")))
				.select("county", "year", "month", "amount")
				.where("status = 'COMPLETED' AND county = '" + county + "'")
				.groupBy("county", "year", "month").sum("amount");
		innerResult.show();
		
	}
	
	public static void suspiciousTransactions() {
		Dataset<Row> transactionsTable =  ProjectPropertie.spark.read().format("org.apache.spark.sql.cassandra").options(new HashMap<String,String>() {
			   {
			   put("keyspace", "space");
			   put("table", "cc_transactions");
			   }}).load().withColumnRenamed("status", "transaction_status");
		
		Dataset<Row> lostCards = ProjectPropertie.spark.read()
				.csv("suspicious.csv")
				.withColumnRenamed("_c0", "card_id")
				.withColumnRenamed("_c1", "status_card")
				.withColumnRenamed("_c2", "reported_at");
		//lostCards.show();
		
		Dataset<Row> suspicious = transactionsTable
			.join(lostCards, transactionsTable.col("card").equalTo(lostCards.col("card_id")))
			.filter( row -> {
				// id transaction
				Long id = UUIDs.unixTimestamp(UUID.fromString(row.getAs("id")));
				//suspicious transaction
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
				Date d = dateFormat.parse(row.getAs("reported_at"));
				Long reportedAt = d.getTime();
				
				System.out.println(id);
				System.out.println(reportedAt);
				return id >= reportedAt?true:false;
			});		
		suspicious.show();	
	}

	public static void main(String[] args) {
		readFromCSV();
		Date date = Calendar.getInstance().getTime();
		generateBalanceByDate(date);
		aggregationBasedOnCustomer("Cluj");
		suspiciousTransactions();
	}
}
