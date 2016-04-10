package ca.yorku.ceras.cvstsparkjobengine.job;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

import ca.yorku.ceras.cvstsparkjobengine.model.Coordinate;
import ca.yorku.ceras.cvstsparkjobengine.model.LegisData;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LegisJob {
	
	public static final Logger log = Logger.getLogger(LegisJob.class);
	
	private static final String TARGET_TABLE_NAME = "legis_data_all";
	
	private static final String TARGET_KEYSPACE_NAME = "legis";
	
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
		SparkConf conf = new SparkConf();
        conf.setAppName("ca.yorku.ceras.cvstsparkjobengine.job.LegisJob");
        
        JavaSparkContext jsc = new JavaSparkContext(conf);
        
        CassandraConnector connector = CassandraConnector.apply(jsc.getConf());
        
        // Opening session with Cassandra...
    	Session session = connector.openSession();
		
		List<Coordinate> coords = null;
		
		try {
			Type listType = new TypeToken<List<Coordinate>>() {}.getType();
			Gson gson = new Gson();
			
			// FIX THIS HARDCODED PATH
			BufferedReader br = new BufferedReader(new FileReader("/home/real.json"));
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
		
		final Long targetInitialTimestamp = 1427907600L; //1pm
		final Long targetFinalTimestamp = 1427925540L; //5:59pm
		
//		final Long targetInitialTimestamp;
//		final Long targetFinalTimestamp;
		
//		if (args[1].equals("1")) {
//			targetInitialTimestamp = 1427907600L; //1pm
//			targetFinalTimestamp = 1427911140L; //1:59pm
//		} else if (args[1].equals("2")) {
//			targetInitialTimestamp = 1427911200L; //2pm
//			targetFinalTimestamp = 1427914740L; //2:59pm
//		} else {
//			targetInitialTimestamp = 1427914800L; //3pm
//			targetFinalTimestamp = 1427918340L; //3:59pm
//		}

		StringBuffer sb = new StringBuffer();
		
		for (Coordinate targetCoordinate : targetCoordinates) {
			JavaRDD<LegisData> data = CassandraJavaUtil.javaFunctions(jsc).cassandraTable(TARGET_KEYSPACE_NAME, TARGET_TABLE_NAME, CassandraJavaUtil.mapRowTo(LegisData.class))
					.where("latitude=?", targetCoordinate.getLat()).where("longitude=?", targetCoordinate.getLng());
			
			data = data.filter(new Function<LegisData, Boolean>() {
				@Override
				public Boolean call(LegisData data) throws Exception {
					return (data.getTimestamp() >= targetInitialTimestamp && data.getTimestamp() <= targetFinalTimestamp);
				}
			});
			
			Double averageSpeed = data.mapToDouble(new DoubleFunction<LegisData>() {
				@Override
				public double call(LegisData data) throws Exception {
					return data.getSpeed();
				}
			}).mean();
			
			sb.append("SCORE:").append(calculateScore(averageSpeed)).append("\n");
		}
		
		System.out.println(sb.toString());

//		log.info("done is done");
//		System.out.println("done is done.");
		
		session.close();
		jsc.close();
	}
	
	private static double calculateScore(double averageSpeed) {
		double maxSpeed = 175.0; //max speed hardcoded manually from the data
		
		double score = (averageSpeed / maxSpeed) * 100;
		
		return score;
	}
	
}
