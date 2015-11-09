package controllers;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http.RequestBody;
import play.mvc.Result;
import play.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Application extends Controller {
	
	private static int requestNumber = 1;

    public static Result index() {
    	System.out.println("index");
    	
//    	String score;
//		try {
//			score = calculateScore("43.6681673");
//			
//			System.out.println("SCORE: " + score);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

    	return ok();
    }
    
    public static Result getScore() {
    	RequestBody body = request().body();
    	
    	String jsonFinal = "";
    	
    	System.out.println("REQUEST RECEIVED...");

    	ObjectMapper mapper = new ObjectMapper();
    	
    	JsonNode node = body.asJson();
    	
    	StringBuffer sb = new StringBuffer();
    	
    	if (node != null) {
			ArrayNode routesNode = (ArrayNode) node.get("routes");
	
			for (final JsonNode route : routesNode) {
		        JsonNode legsNode = route.get("legs");
		        
		        for (final JsonNode leg : legsNode) {
		        	JsonNode stepsNode = leg.get("steps");
		        	
		        	int cont = 0;
		        	for (final JsonNode step : stepsNode) {
		        		String latitude = step.get("end_location").get("lat").asText();
		        		String longitude = step.get("end_location").get("lng").asText();
		        		String score = step.get("score") == null ? null : step.get("score").asText();

		        		if (latitude != null && longitude != null && score != null) {
		        			if (score.equalsIgnoreCase("-1")) {
		        				sb.append(latitude).append("+").append(longitude).append("_");
		        				
				        		ObjectNode stepObj = (ObjectNode) step;
				        		stepObj.remove("score");
				        		stepObj.put("score", "%"+cont);
				        		cont++;
		        			}
		        		}
		        	}
		        }  
		    }
			
			//System.out.println(node.toString());
			
			String coords = sb.toString();
	    	
	    	if (coords.length() > 0) {
	    		try {
	    			String[] scores = calculateScore(coords);

	    			jsonFinal = node.toString();
	    			
	    			for (int i=0; i<scores.length; i++) {
	    				jsonFinal = StringUtils.replace(jsonFinal, "%"+i, scores[i]);
	    			}
	    			
				} catch (Exception e) {
					e.printStackTrace();
				}
	    	}
    	}
    	
    	if (requestNumber == 3) {
    		requestNumber = 1;
    	} else {
    		requestNumber++;
    	}
    	
    	ObjectNode finalObject = null;
		try {
			finalObject = (ObjectNode) mapper.readTree(jsonFinal);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return ok(finalObject);
    }
    
    private static String[] calculateScore(String coordsFull) throws Exception {
    	List<String> coords = Arrays.asList(coordsFull.split("_"));
    	
    	String[] scores = new String[coords.size()];
    	
    	String command = "/opt/spark/bin/spark-submit --driver-class-path /home/ubuntu/spark_libs/hbase-server-0.98.6-cdh5.3.0.jar:"
				+ "/home/ubuntu/spark_libs/hbase-protocol-0.98.6-cdh5.3.0.jar:/home/ubuntu/spark_libs/hbase-hadoop-compat-0.98.6-cdh5.3.0.jar:"
				+ "/home/ubuntu/spark_libs/hbase-client-0.98.6-cdh5.3.0.jar:/home/ubuntu/spark_libs/hbase-common-0.98.6-cdh5.3.0.jar:"
				+ "/home/ubuntu/spark_libs/htrace-core-2.04.jar:/home/ubuntu/spark_libs/spark-hbase-0.0.3-clabs-SNAPSHOT.jar:"
				+ "/home/ubuntu/spark_libs/protobuf-java-2.5.0.jar:/home/ubuntu/spark_libs/guava-12.0.1.jar:"
				+ "/home/ubuntu/spark_libs/high-scale-lib-1.1.1.jar:/home/ubuntu/spark_libs/gson-2.4.jar:"
				+ "/home/ubuntu/spark_libs/commons-math3-3.2.jar --class ca.yorku.ceras.sparkjobengine.job.LegisJob"
				+ " --master spark://legis-cluster-legis-master-tmpl-001:7077 /home/ubuntu/jar/SparkJobEngine-1.0-SNAPSHOT.jar %s %s";
		
		//command = String.format(command, "43.6681673+-79.3695427_43.6624229+-79.39476599999999_43.7746998+-79.5040829", "1");
    	command = String.format(command, coordsFull, requestNumber);
		
		System.out.println("Executing the command:\n" + command);
		
		ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
		builder.redirectErrorStream(true);
		
		Process process = builder.start();
		
    	InputStream is = process.getInputStream();
    	InputStreamReader isr = new InputStreamReader(is);
    	BufferedReader br = new BufferedReader(isr);
    	String line;

    	DecimalFormat df = new DecimalFormat("#.00"); 
    	
    	int cont = 0;
    	
    	while ((line = br.readLine()) != null) {
    		if (line.startsWith("SCORE:")) {
    			System.out.println(line);
    			double score = Double.parseDouble(line.substring(6));
    			scores[cont] = df.format(score);
    			cont++;
    		}
    	}

    	return scores;
    }
}
