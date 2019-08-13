package spark.customer;

import junit.framework.Assert;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import spark.bank.Customer;

public class SparkCassandraTest {

    SparkConf conf;

    SparkSession spark;

    @Before
    public void beforeTest() {
        conf = new SparkConf(true).setAppName("App_name").setMaster("local[2]").set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "127.0.0.1,172.17.0.2")
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");
        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
    }

    @Test
    public void testReadingProperties() {
        Dataset<Customer> customerRecords = spark.read().csv("customers.csv")
                .withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "county").withColumnRenamed("_c2", "name")
                .as(Encoders.bean(Customer.class));
        RDD<Customer> rdd = customerRecords.rdd();
        Assert.assertTrue(!rdd.isEmpty());
    }


}
