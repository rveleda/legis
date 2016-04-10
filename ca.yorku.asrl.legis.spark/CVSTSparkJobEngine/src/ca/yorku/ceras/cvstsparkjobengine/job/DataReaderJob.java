package ca.yorku.ceras.cvstsparkjobengine.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple3;

import com.cloudera.spark.hbase.JavaHBaseContext;

/**
 * This class represents a Spark Job used for the Sipresk project
 * @author rveleda
 *
 */
public class DataReaderJob {
	
	public static final Logger log = Logger.getLogger(DataReaderJob.class);
	
	private static final String TARGET_TABLE_NAME = "legis_data_all";
	
	private static StructType generateSchema() {
		List<StructField> fields = new ArrayList<StructField>();
		
		fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("latitude", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("longitude", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("speed", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("occ", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("vol", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("laneNumber", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, true));

		return DataTypes.createStructType(fields);
	}
	
	private static String generateRow(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) {
		String rowKey = Bytes.toString(record._1());
		
		//TODO TURN THIS GENERIC
		String id = null;
		Double latitude = null;
		Double longitude = null;
		Double speed = null;
		Double occ = null;
		Double vol = null;
		Double laneNumber = null;
		Long timestamp = null;
		
		for (Tuple3<byte[], byte[], byte[]> tuple : record._2) {
			String columnName = Bytes.toString(tuple._2());
			
			byte[] columnValue = tuple._3();
			
			String tmp = Bytes.toString(columnValue);
			try {
				// Skipping NULL values
				if ((tmp != null && !tmp.equalsIgnoreCase(""))) {
					
					if (columnName.equalsIgnoreCase("id")) {
						id = tmp;
					} else if (columnName.equalsIgnoreCase("latitude")) {
						latitude = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("longitude")) {
						longitude = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("speed")) {
						speed = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("occ")) {
						occ = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("vol")) {
						vol = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("laneNumber")) {
						laneNumber = new Double(Double.parseDouble(tmp));
					} else if (columnName.equalsIgnoreCase("timestamp")) {
						timestamp = new Long(Long.parseLong(tmp));
					}
				}
			} catch (Exception e) {
				System.out.println("HERE: " +tmp);
				System.out.println(e.getMessage());
			}

		}
		
		Row row = RowFactory.create(rowKey, id, latitude, longitude, speed, occ, vol, laneNumber, timestamp);
		
		StringBuffer sb = new StringBuffer();
		
		if (row.length() < 9) {
			System.out.println("TROUBLE ROW: " + row.toString());
		} else {
			sb.append(rowKey).append(",");
			sb.append(id).append(",");
			sb.append(latitude).append(",");
			sb.append(longitude).append(",");
			sb.append(speed).append(",");
			sb.append(occ).append(",");
			sb.append(vol).append(",");
			sb.append(laneNumber).append(",");
			sb.append(timestamp);
		}
		
		return sb.toString();
	}
	
	public static void main(String[] args) {
		// Creating the Spark Context and providing the necessary settings
		SparkConf conf = new SparkConf().setAppName("ca.yorku.ceras.cvstsparkjobengine.job.DataReaderJob");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", "10.12.7.59");
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.set("hbase.master", "10.12.7.59:60000");
		
		JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, hbaseConf);

		//RDD with records of type (RowKey, List[(columnFamily, columnQualifier, Value)]
		JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> targetHbaseTable = hBaseContext.hbaseRDD(TARGET_TABLE_NAME, new Scan());
		//Generating local schema
		StructType schema = generateSchema();
		
		JavaRDD<String> resultRDD = targetHbaseTable.map(new Function<Tuple2<byte[],List<Tuple3<byte[],byte[],byte[]>>>, String>() {
			public String call(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) throws Exception {
				return generateRow(record);
			}
		});
		
		//System.out.println("TOTAL ROWS: " + resultRDD.count());

//		DataFrame cvstDataFrame = sqlContext.createDataFrame(rowDD, schema);
//		cvstDataFrame.registerTempTable(TARGET_TABLE_NAME);
//		
//		DataFrame totalLoopDetectors = sqlContext.sql("SELECT `rowKey` FROM " + TARGET_TABLE_NAME + " LIMIT 1000");
//		
//		JavaRDD<ResultRow> totalLoopDetectorResult = totalLoopDetectors.javaRDD().map(new Function<Row, ResultRow>() {
//			@Override
//			public ResultRow call(Row v1) throws Exception {
//				ResultRow row = new ResultRow();
//				row.setContent(v1.getString(0));
//				
//				return row;
//			}
//		});
//		
//		log.info("============= TOTAL LOOP DETECTORS ===============");
//		
		List<String> rows = resultRDD.collect();
		
		for (String r : rows) {
			//log.info(r.getContent());
			System.out.println(r);
		}
	
		log.info("done is done");
		System.out.println("done is done.");
		
		jsc.close();
	}

}
