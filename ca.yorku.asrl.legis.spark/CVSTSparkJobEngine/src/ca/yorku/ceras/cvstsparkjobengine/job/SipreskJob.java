package ca.yorku.ceras.cvstsparkjobengine.job;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple3;
import ca.yorku.ceras.cvstsparkjobengine.model.ResultRow;

import com.cloudera.spark.hbase.JavaHBaseContext;

/**
 * This class represents a Spark Job used for the Sipresk project
 * @author rveleda
 *
 */
public class SipreskJob {
	
	public static final Logger log = Logger.getLogger(SipreskJob.class);
	
	private static final String TARGET_TABLE_NAME = "cvst";
	
	private static StructType generateSchema() {
		List<StructField> fields = new ArrayList<StructField>();
		
		fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("ContractId", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("VdsId", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("LaneOcc", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("LaneSpd", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("LaneVol", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("LaneLength", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("Occ", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("Spd", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("Vol", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("Length", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("LaneNumber", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("Timestamp", DataTypes.LongType, true));
		fields.add(DataTypes.createStructField("ErrorCodeUpStream", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("ErrorCodeDownStream", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("MonthYear", DataTypes.StringType, true));
		
		return DataTypes.createStructType(fields);
	}
	
	private static ResultRow generateRow(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) {
		String rowKey = Bytes.toString(record._1());
		
		ResultRow result = new ResultRow();
		
		//TODO TURN THIS GENERIC
		String contractId = null;
		String vdsId = null;
		Double laneOcc = null;
		Double laneSpd = null;
		Double laneVol = null;
		Double laneLength = null;
		Double occ = null;
		Double spd = null;
		Double vol = null;
		Double length = null;
		Double laneNumber = null;
		Long timestamp = null;
		Double errorCodeUpStream = null;
		Double errorCodeDownStream = null;
		String monthYear = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("MMyyyy");
		
		if (rowKey.contains("MTO")) {
			String[] rowKeyData = rowKey.split("-");
			String rowKeyTimestamp = rowKeyData[2];
			
			if (rowKeyTimestamp != null) {
				Long timestampKey = Long.parseLong(rowKeyTimestamp);
				
				timestampKey = timestampKey * 1000;
				
				Date currentDate = new Date(timestampKey);
				
				monthYear = sdf.format(currentDate);
				
				// Must be between 07/07/2015 00:00am and 07/07/2015 23:59pm
				if (timestampKey.compareTo(new Long("1436241600000")) >= 0 && timestampKey.compareTo(new Long("1436327940000")) <= 0) {
					for (Tuple3<byte[], byte[], byte[]> tuple : record._2) {
						String columnName = Bytes.toString(tuple._2());
						
						byte[] columnValue = tuple._3();
						
						String tmp = Bytes.toString(columnValue);
						try {
							// Skipping NULL values
							if ((tmp != null && !tmp.equalsIgnoreCase(""))) {
								
								if (columnName.equalsIgnoreCase("ContractId")) {
									contractId = tmp;
								} else if (columnName.equalsIgnoreCase("VdsId")) {
									vdsId = tmp;
								} else if (columnName.equalsIgnoreCase("LaneOcc")) {
									laneOcc = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("LaneSpd")) {
									laneSpd = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("LaneVol")) {
									laneVol = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("LaneLength")) {
									laneLength = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("Occ")) {
									occ = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("Spd")) {
									spd = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("Vol")) {
									vol = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("Length")) {
									length = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("LaneNumber")) {
									laneNumber = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("timestamp")) {
									timestamp = new Long(Long.parseLong(tmp));
								} else if (columnName.equalsIgnoreCase("ErrorCodeUpStream")) {
									errorCodeUpStream = new Double(Double.parseDouble(tmp));
								} else if (columnName.equalsIgnoreCase("ErrorCodeDownStream")) {
									errorCodeDownStream = new Double(Double.parseDouble(tmp));
								}
							}
						} catch (Exception e) {
							System.out.println("HERE: " +tmp);
							System.out.println(e.getMessage());
						}

					}

					StringBuffer sb = new StringBuffer();
					
					sb.append(rowKey).append(",");
					sb.append(contractId).append(",");
					sb.append(vdsId).append(",");
					sb.append(laneOcc).append(",");
					sb.append(laneSpd).append(",");
					sb.append(laneVol).append(",");
					sb.append(laneLength).append(",");
					sb.append(occ).append(",");
					sb.append(spd).append(",");
					sb.append(vol).append(",");
					sb.append(length).append(",");
					sb.append(laneNumber).append(",");
					sb.append(timestamp).append(",");
					sb.append(errorCodeUpStream).append(",");
					sb.append(errorCodeDownStream).append(",");
					sb.append(monthYear);
					
					result.setContent(sb.toString());
				}
			}
		}

		return result;
	}
	
	public static void main(String[] args) {
		// Creating the Spark Context and providing the necessary settings
		SparkConf conf = new SparkConf().setAppName("ca.yorku.ceras.cvstsparkjobengine.job.SipreskJob");
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
		
		JavaRDD<ResultRow> resultRDD = targetHbaseTable.map(new Function<Tuple2<byte[],List<Tuple3<byte[],byte[],byte[]>>>, ResultRow>() {
			public ResultRow call(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) throws Exception {
				return generateRow(record);
			}
		});

		resultRDD = resultRDD.filter(new Function<ResultRow, Boolean>() {
			public Boolean call(ResultRow result) throws Exception {
				return !result.getContent().equalsIgnoreCase("");
			}
		});

		JavaRDD<Row> rowRDD = resultRDD.map(new Function<ResultRow, Row>() {
			@Override
			public Row call(ResultRow record) throws Exception {
				String[] values = record.getContent().split(",");
				
				return RowFactory.create(values[0], values[1], values[2], Double.parseDouble(values[3]), Double.parseDouble(values[4]), Double.parseDouble(values[5]), 
						Double.parseDouble(values[6]), Double.parseDouble(values[7]), Double.parseDouble(values[8]), Double.parseDouble(values[9]), 
						Double.parseDouble(values[10]), Double.parseDouble(values[11]), Long.parseLong(values[12]), Double.parseDouble(values[13]), 
						Double.parseDouble(values[14]), values[15]);
			}
		});
		
		DataFrame cvstDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		cvstDataFrame.registerTempTable(TARGET_TABLE_NAME);
		
		DataFrame totalLoopDetectors = sqlContext.sql("SELECT `ContractId`, `Timestamp`, `LaneSpd` FROM " + TARGET_TABLE_NAME + " WHERE `LaneNumber` = 1 "
				+ "AND `LaneSpd` >= 0 AND `LaneOcc` > 0 AND `LaneVol` > 0 "
				+ "GROUP BY `ContractId`, `Timestamp`, `LaneSpd` ORDER BY `ContractId`, `Timestamp`");
		
//		JavaRDD<ResultRow> totalLoopDetectorResult = totalLoopDetectors.javaRDD().map(new Function<Row, ResultRow>() {
//			@Override
//			public ResultRow call(Row v1) throws Exception {
//				ResultRow row = new ResultRow();
//				SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss");
//				row.setContent(v1.getString(0) + " - " + v1.getDouble(2) + " - " + sdf.format(new Date(v1.getLong(1) * 1000)));
//				
//				return row;
//			}
//		});
		
		JavaRDD<ResultRow> totalLoopDetectorResult = totalLoopDetectors.javaRDD().map(new Function<Row, ResultRow>() {
			// Speed1, Speed2 = (60,150)
			// Speed3, Speed4 = (0,20)
			double speed1, speed2, speed3, speed4 = -50.0;
			long timeStamp1, timeStamp2, timeStamp3, timeStamp4 = 0L;
			
			SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");

			String currentLoopDetector = "";
			
			@Override
			public ResultRow call(Row row) throws Exception {				
				ResultRow result = new ResultRow();
				
				if (!currentLoopDetector.equalsIgnoreCase(row.getString(0))) {
					currentLoopDetector = row.getString(0);
					
					speed1 = -50.0;
					speed2 = -50.0;
					speed3 = -50.0;
					speed4 = -50.0;
					
					timeStamp1 = 0L;
					timeStamp2 = 0L;
					timeStamp3 = 0L;
					timeStamp4 = 0L;
				}
				
				double targetSpeed = row.getDouble(2);
				long timeStamp = row.getLong(1);
				
				if (speed1 == -50.0) {
					speed1 = targetSpeed;
					timeStamp1 = timeStamp;
					
					return result;
				} else if (speed2 == -50.0) {
					speed2 = targetSpeed;
					timeStamp2 = timeStamp;
					
					return result;
				} else if (speed3 == -50.0) {
					speed3 = targetSpeed;
					timeStamp3 = timeStamp;
					
					return result;
				}
				
				speed4 = targetSpeed;
				timeStamp4 = timeStamp;
				
				if (speed4 >= 0 && speed4 <= 20 && row.getString(0).equalsIgnoreCase(currentLoopDetector)) {

					if (speed3 >= 0 && speed3 <= 20 && row.getString(0).equalsIgnoreCase(currentLoopDetector)) {
						
						if (speed2 >= 60 && speed2 <= 150 && row.getString(0).equalsIgnoreCase(currentLoopDetector)) {
							
							if (speed1 >= 60 && speed1 <= 150 && row.getString(0).equalsIgnoreCase(currentLoopDetector)) {
								StringBuffer sb = new StringBuffer();
								
								sb.append(currentLoopDetector).append(",");
								sb.append(sdf.format(new Date(timeStamp1 * 1000))).append("_").append(speed1).append(",");
								sb.append(sdf.format(new Date(timeStamp2 * 1000))).append("_").append(speed2).append(",");
								sb.append(sdf.format(new Date(timeStamp3 * 1000))).append("_").append(speed3).append(",");
								sb.append(sdf.format(new Date(timeStamp4 * 1000))).append("_").append(speed4);
								
								result.setContent(sb.toString());
								
								// RESET!!!!
								speed1 = -50.0;
								speed2 = -50.0;
								speed3 = -50.0;
								speed4 = -50.0;
								
								timeStamp1 = 0L;
								timeStamp2 = 0L;
								timeStamp3 = 0L;
								timeStamp4 = 0L;
							}
						}
					}
				}
				
				if (speed1 != -50.0 && speed2 != -50.0 && speed3 != -50.0) {
					speed1 = speed2;
					speed2 = speed3;
					speed3 = speed4;
					speed4 = -50.0;
					
					timeStamp1 = timeStamp2;
					timeStamp2 = timeStamp3;
					timeStamp3 = timeStamp4;
					timeStamp4 = 0L;
				}
				
				return result;
			}
			
		});
		
		totalLoopDetectorResult = totalLoopDetectorResult.filter(new Function<ResultRow, Boolean>() {
			public Boolean call(ResultRow result) throws Exception {
				return !result.getContent().equalsIgnoreCase("");
			}
		});
		
		log.info("============= TOTAL LOOP DETECTORS ===============");
		
		List<ResultRow> rows = totalLoopDetectorResult.collect();
		
		for (ResultRow r : rows) {
			log.info(r.getContent());
			//System.out.println(r.getContent());
		}
	
		log.info("done is done");
		System.out.println("done is done.");
		
		jsc.close();
	}

}
