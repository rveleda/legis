package ca.yorku.ceras.sparkjobengine.job;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;

import scala.Tuple2;
import scala.Tuple3;

import com.cloudera.spark.hbase.JavaHBaseContext;

public class HBaseTest {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ca.yorku.ceras.sparkjobengine.job.HBaseTest");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(jsc);
		
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", "10.12.7.37");
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.set("hbase.master", "10.12.7.37:60000");

		JavaHBaseContext context = new JavaHBaseContext(jsc, hbaseConf);
		
		//RDD with records of type (RowKey, List[(columnFamily, columnQualifier, Value)]
		JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> legisRDD = context.hbaseRDD("legis_data_all", new Scan());
		
		legisRDD = legisRDD.filter(new Function<Tuple2<byte[],List<Tuple3<byte[],byte[],byte[]>>>, Boolean>() {

			@Override
			public Boolean call(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) throws Exception {
				String rowKey = Bytes.toString(record._1());
				return rowKey.contains("43.75165375_-79.52952512") && rowKey.contains("18d8042386b79e2c279fd162df0205c8");
			}
			
		});

		
		System.out.println("TOTAL: " + legisRDD.count());
	}

}
