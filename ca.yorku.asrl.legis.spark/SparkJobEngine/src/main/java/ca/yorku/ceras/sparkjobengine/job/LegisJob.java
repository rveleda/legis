package ca.yorku.ceras.sparkjobengine.job;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import scala.Tuple2;
import scala.Tuple3;
import ca.yorku.ceras.sparkjobengine.model.Coordinate;

import com.cloudera.spark.hbase.JavaHBaseContext;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LegisJob {
	
	public static final Logger log = Logger.getLogger(LegisJob.class);
	
	private static final String TARGET_TABLE_NAME = "legis_data_all";
	
	private static StructType generateSchema() {
		List<StructField> fields = new ArrayList<StructField>();
		
		fields.add(DataType.createStructField("rowKey", DataType.StringType, true));
		fields.add(DataType.createStructField("id", DataType.StringType, true));
		fields.add(DataType.createStructField("latitude", DataType.DoubleType, true));
		fields.add(DataType.createStructField("longitude", DataType.DoubleType, true));
		fields.add(DataType.createStructField("speed", DataType.DoubleType, true));
		fields.add(DataType.createStructField("occ", DataType.DoubleType, true));
		fields.add(DataType.createStructField("vol", DataType.DoubleType, true));
		fields.add(DataType.createStructField("laneNumber", DataType.DoubleType, true));
		fields.add(DataType.createStructField("timestamp", DataType.LongType, true));

		return DataType.createStructType(fields);
	}
	
	private static Row generateRow(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) {
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

		Row row = Row.create(rowKey, id, latitude, longitude, speed, occ, vol, laneNumber, timestamp);
		
		if (row.length() < 9) {
			System.out.println("TROUBLE ROW: " + row.toString());
		}
		
		//System.out.println("CREATING ROW: " + row.toString());
		
		return row;
	}
	
	public static String crypt(String str) throws Exception {
        if (str == null || str.length() == 0) {
            throw new IllegalArgumentException("String to encript cannot be null or zero length");
        }
        
        MessageDigest digester = MessageDigest.getInstance("MD5");

        digester.update(str.getBytes());
        byte[] hash = digester.digest();
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < hash.length; i++) {
            if ((0xff & hash[i]) < 0x10) {
                hexString.append("0" + Integer.toHexString((0xFF & hash[i])));
            }
            else {
                hexString.append(Integer.toHexString(0xFF & hash[i]));
            }
        }
        return hexString.toString();
    }
	
	private static Random rand = new Random();
	
	private static int getRandomInt(int min, int max) {
		return rand.nextInt((max-min) + 1) + min;
	}
	
	private static EuclideanDistance distance = new EuclideanDistance();
	
	private static double calculateDistance(String latitude, String longitude, Coordinate coord) {
		
		double[] targetCoord = {Double.parseDouble(coord.getLat()), Double.parseDouble(coord.getLng())};
		
		double[] neededCoord = {Double.parseDouble(latitude), Double.parseDouble(longitude)};
		
		return distance.compute(neededCoord, targetCoord);
	}
	
	private static Coordinate getTheClosestCoordinate(String latitude, String longitude, List<Coordinate> coords) {
		double shortestDistance = 1000000.0;
		Coordinate targetCoordinate = null;
		
		for (Coordinate c : coords) {
			double distance = calculateDistance(latitude, longitude, c);
			
			if (distance <= shortestDistance) {
				shortestDistance = distance;
				targetCoordinate = c;
			}
		}
		
		return targetCoordinate;
	}

	// args[0] = latitude+longitude_latitude+longitude
	// args[1] = request number
	public static void main(String[] args) {
		
		// Creating the Spark Context and providing the necessary settings
		SparkConf conf = new SparkConf().setAppName("ca.yorku.ceras.sparkjobengine.job.LegisJob");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(jsc);
		
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", "10.12.7.37");
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.set("hbase.master", "10.12.7.37:60000");

		JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, hbaseConf);
		
		List<Coordinate> coords = null;
		
		try {
			Type listType = new TypeToken<List<Coordinate>>() {}.getType();
			Gson gson = new Gson();
			
			// FIX THIS HARDCODED PATH
			BufferedReader br = new BufferedReader(new FileReader("/home/ubuntu/jar/real.json"));
			JsonObject jsonRaw = (JsonObject) new JsonParser().parse(br);
			
			coords = gson.fromJson(jsonRaw.get("coords"), listType);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		// handle multiple coords
		List<String> inputCoords = Arrays.asList(args[0].split("_"));
		List<Coordinate> targetCoordinates = new ArrayList<Coordinate>();
		
		for (String inputCoord : inputCoords) {
			String inputLatitude = inputCoord.split("\\+")[0];
			String inputLongitude = inputCoord.split("\\+")[1];
			
			Coordinate targetCoordinate = getTheClosestCoordinate(inputLatitude, inputLongitude, coords);
			
			targetCoordinates.add(targetCoordinate);
		}
		
		Long targetInitialTimestamp = 1427907600L; //1pm
		Long targetFinalTimestamp = 1427925540L; //5:59pm
		
		if (args[1].equals("1")) {
			targetInitialTimestamp = 1427907600L; //1pm
			targetFinalTimestamp = 1427911140L; //1:59pm
		} else if (args[1].equals("2")) {
			targetInitialTimestamp = 1427911200L; //2pm
			targetFinalTimestamp = 1427914740L; //2:59pm
		}	else if (args[1].equals("3")) {
			targetInitialTimestamp = 1427914800L; //3pm
			targetFinalTimestamp = 1427918340L; //3:59pm
		}

		StringBuffer sb = new StringBuffer();
		
		for (Coordinate targetCoordinate : targetCoordinates) {
			String rowKey = null;
			
			// SQL can be run over RDDs that have been registered as tables.
			try {
				rowKey = targetCoordinate.getLat() + "_" + targetCoordinate.getLng() + "_" + crypt(targetCoordinate.getStreetName()) + "_";
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("ROWKEY IS NULL!");
			}

			//RDD with records of type (RowKey, List[(columnFamily, columnQualifier, Value)]
			JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> targetHbaseTable = hBaseContext.hbaseRDD(TARGET_TABLE_NAME, 
					new Scan(Bytes.toBytes(rowKey + targetInitialTimestamp), Bytes.toBytes(rowKey + targetFinalTimestamp)));
			//Generating local schema
			StructType schema = generateSchema();
			
			JavaRDD<Row> rowDD = targetHbaseTable.map(new Function<Tuple2<byte[],List<Tuple3<byte[],byte[],byte[]>>>, Row>() {
				@Override
				public Row call(Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> record) throws Exception {
					return generateRow(record);
				}
			});
			
			JavaSchemaRDD schemaRDD = sqlContext.applySchema(rowDD, schema);
			// Register the SchemaRDD as a table.
			schemaRDD.registerTempTable(TARGET_TABLE_NAME);
			
			JavaSchemaRDD results = sqlContext.sql("SELECT id, AVG(speed)"+ " FROM " + TARGET_TABLE_NAME + " GROUP BY id");
			//JavaSchemaRDD results = sqlContext.sql("SELECT MAX(speed)"+ " FROM " + TARGET_TABLE_NAME);
			
			JavaRDD<ResultRow> queryResult = results.map(new Function<Row, ResultRow>() {
				@Override
				public ResultRow call(Row v1) throws Exception {
					//System.out.println(v1.getDouble(1));
					
					ResultRow result = new ResultRow();
					result.row = String.valueOf(v1.getDouble(1));

					return result;
				}
			});
			
			ResultRow result = queryResult.collect().get(0);
			
			sb.append("SCORE:").append(calculateScore(Double.parseDouble(result.row))).append("\n");
		}
		
		System.out.println(sb.toString());

//		log.info("done is done");
//		System.out.println("done is done.");
		
		jsc.close();
	}
	
	private static double calculateScore(double averageSpeed) {
		double maxSpeed = 175.0; //max speed hardcoded manually from the data
		
		double score = (averageSpeed / maxSpeed) * 100;
		
		return score;
	}

	private static class ResultRow implements Serializable {

		private static final long serialVersionUID = 7918623481050678178L;
		
		protected String row = "";
	}
	
	
}
