package ca.yorku.asrl.legis.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import ca.yorku.asrl.legis.gateway.CloudNode;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;


@Path("/directions")
public class Directions {
	
	public static final String GOOGLE_API_KEY = "AIzaSyBdnn64NPPuaiBRbsb94jyVzEjLWC0anh8";
	public static final CloudNode myNode = CloudNode.EDGE_TR_1;
	public static final CloudNode west = CloudNode.EDGE_WT_1;
	public static final CloudNode north = CloudNode.EDGE_YK_1;
	public static final CloudNode south = null;
	public static final CloudNode east = CloudNode.EDGE_CT_1;
	public static final String filename = "/WEB-INF/jsonResponse_10.json";
	
	@Context
	private ServletContext context;
	
	private String jsonResponse;
	
	//public HashMap<ClientIP,>
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getDirections(@QueryParam("origin_lat") double origin_lat, 
			@QueryParam("origin_long") double origin_long, 
			@QueryParam("destination_lat") double destination_lat,
			@QueryParam("destination_long") double destination_long, @QueryParam("dynamic") boolean dynamic) {
		
		String origin = ""+origin_lat+","+origin_long;
		String destination = ""+destination_lat+","+destination_long;

		if (!dynamic) {
			try {
				//byte[] encoded = Files.readAllBytes(Paths.get("jsonResponse.json"));
				//jsonResponse = new String(encoded, StandardCharsets.UTF_8);
				
				jsonResponse = new Scanner(context.getResourceAsStream(filename), "UTF-8").useDelimiter("\\A").next();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			jsonResponse = "";
		}
		
		if (jsonResponse == null || jsonResponse.equals("")) {
			Response response = ClientBuilder
					.newClient()
					.target("https://maps.googleapis.com/maps/api/directions/json")
					.queryParam("origin", origin)
					.queryParam("destination", destination)
					.queryParam("key", GOOGLE_API_KEY)
					.queryParam("alternatives", "true").request()
					.get(Response.class);
			System.out.println("Google request sent.");
			jsonResponse = response.readEntity(String.class);
		}
		
		JsonParser parser = new JsonParser();
		JsonReader reader = new JsonReader(new StringReader(jsonResponse));
		reader.setLenient(true);
		JsonObject o = parser.parse(reader).getAsJsonObject();
		//JsonObject localJson = parser.parse(response.readEntity(String.class)).getAsJsonObject();
		
		
		//System.out.println(jsonResponse);
		JsonObject annotated = null;
		if(o.get("status").getAsString().equals("OK")) {
			annotated = parser.parse(annotate(o.toString())).getAsJsonObject();
		}
		
//		try {
//			BufferedWriter writer = new BufferedWriter(new FileWriter("jsonResponse"+origin+".json"));
//			writer.write(jsonResponse);
//			writer.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		if(annotated != null) {
			return "legis_directions("+annotated.toString()+");";
		}
		else {
			return "legis_directions("+o.toString()+");";
		}
		
		
	}

	/*private void annotateRoutes(JsonObject o) {
		for(JsonElement route : o.get("routes").getAsJsonArray()) {
			for(JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for(JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					JsonObject stepObject = step.getAsJsonObject();
					double score = getScoreForStep(stepObject.toString());
					stepObject.addProperty("score", score);
				}
			}
		}
		
	}*/
	
/*	private void annotateRoutes(JsonObject o) {
		CloudNode forwardServer = null;
		for(JsonElement route : o.get("routes").getAsJsonArray()) {
			for(JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for(JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					JsonObject stepObject = step.getAsJsonObject();
					String direction = myNode.getZone().outOfBoundsDirection(stepObject.get("end_location").getAsJsonObject().get("lat").getAsDouble(), 
							stepObject.get("end_location").getAsJsonObject().get("lng").getAsDouble());
					if (direction.equals("inbounds")) {
						double score = getScoreForStep(stepObject.toString());
						stepObject.addProperty("score", score);
					}
					else if(direction.equals("west")) {
						
					}
				}
			}
		}
	}*/
	
	@Path("/annotate")
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String annotate(String json) {
		JsonParser parser = new JsonParser();
		JsonObject o = parser.parse(json).getAsJsonObject();
		HashSet<CloudNode> forwardNeighbors = new HashSet<CloudNode>();
		for(JsonElement route : o.get("routes").getAsJsonArray()) {
			for(JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for(JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					JsonObject stepObject = step.getAsJsonObject();
					String direction = myNode.getZone().outOfBoundsDirection(stepObject.get("end_location").getAsJsonObject().get("lat").getAsDouble(), 
							stepObject.get("end_location").getAsJsonObject().get("lng").getAsDouble());
//					double score = getScoreForStep(stepObject.toString());
//					stepObject.addProperty("score", score);
					stepObject.addProperty("score", "%s");
					
					/*if (direction.equals("inbounds")) {
						//double score = getScoreForStep(stepObject.toString());
						stepObject.addProperty("score", -1);
					}
					else if(direction.equals("west") && stepObject.get("score") == null) {
						Response response = ClientBuilder.newClient().target("http://"+west.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
								.request()
								.put(Entity.json(json),Response.class);
						o = parser.parse(response.readEntity(String.class)).getAsJsonObject();
						forwardNeighbors.add(west);
						break;
					}
					else if(direction.equals("east") && stepObject.get("score") == null) {
						Response response = ClientBuilder.newClient().target("http://"+east.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
								.request()
								.put(Entity.json(json),Response.class);
						o = parser.parse(response.readEntity(String.class)).getAsJsonObject();
						forwardNeighbors.add(east);
						break;
					}
					else if(direction.equals("north") && stepObject.get("score") == null) {
						Response response = ClientBuilder.newClient().target("http://"+north.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
								.request()
								.put(Entity.json(json),Response.class);
						o = parser.parse(response.readEntity(String.class)).getAsJsonObject();
						forwardNeighbors.add(north);
						break;
					}
					else if(direction.equals("south") && stepObject.get("score") == null) {
						Response response = ClientBuilder.newClient().target("http://"+south.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
								.request()
								.put(Entity.json(json),Response.class);
						o = parser.parse(response.readEntity(String.class)).getAsJsonObject();
						forwardNeighbors.add(south);
						break;
					}*/
				}
			}
		}
		jsonResponse = callSpark(o.toString());
		/*for(CloudNode neighbor : forwardNeighbors) {
			forwardResponse(neighbor);
		}*/
		//jsonResponse = o.toString();
		return jsonResponse;
	}

	private String callSpark(String json) {
//		Response response = ClientBuilder.newClient().target("http://10.12.7.25:9000/spark/getScore")
//				.request()
//				.put(Entity.json(json),Response.class);
//		return response.readEntity(String.class);
		
		return this.calculateScore(json);
	}

	private void forwardResponse(CloudNode neighbor) {
		Response response = ClientBuilder.newClient().target("http://"+neighbor.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
		.request()
		.put(Entity.json(jsonResponse),Response.class);
		jsonResponse = response.readEntity(String.class);
		
	}

	private double getScoreForStep(String string) {
		Random gen = new Random();
		return gen.nextDouble()*100;
	}
	
	private String calculateScore(String jsonResponse) {
		JsonParser parser = new JsonParser();
		JsonObject o = parser.parse(jsonResponse).getAsJsonObject();
		
		StringBuffer sb = new StringBuffer();
		
		for (JsonElement route : o.get("routes").getAsJsonArray()) {
			for (JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for (JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					String latitude = step.getAsJsonObject().get("end_location").getAsJsonObject().get("lat").getAsString();
					String longitude = step.getAsJsonObject().get("end_location").getAsJsonObject().get("lng").getAsString();
					
					sb.append(latitude).append("+").append(longitude).append("_");
				}
			}
		}
		
		String coords = sb.toString();
		
		System.out.println("COORDS: " + coords);

	    if (coords.length() > 0) {
	    	try {
	    		Object[] scores = (Object[]) getScoreFromSpark(coords);

	    		jsonResponse = String.format(jsonResponse, scores);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
	    
	    return jsonResponse;
	}
	
    private String[] getScoreFromSpark(String coordsFull) throws Exception {
    	List<String> coords = Arrays.asList(coordsFull.split("_"));
    	
    	String[] scores = new String[coords.size()];
    	
//    	String command = "/usr/local/spark/bin/spark-submit --master spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT} --conf spark.driver.host=${SPARK_LOCAL_IP} "
//    			+ "--properties-file /spark-defaults.conf --conf spark.cassandra.connection.host=${CASSANDRA_IP} "
//    			+ "--jars $(echo /home/dependency/*.jar | tr ' ' ',') --class ca.yorku.ceras.cvstsparkjobengine.job.LegisJob "
//    			+ "/home/CVSTSparkJobEngine-0.0.1-SNAPSHOT.jar %s %s";
		
		//command = String.format(command, "43.6681673+-79.3695427_43.6624229+-79.39476599999999_43.7746998+-79.5040829", "1");
    	//command = String.format(command, coordsFull, "1");
    	
    	String command = "/home/spark-remote-submit.sh " + coordsFull + " 1";
		
		System.out.println("Executing the command:\n" + command);
		
		ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
		builder.redirectErrorStream(true);
		
		Process process = builder.start();
		
//    	InputStream is = process.getInputStream();
//    	InputStreamReader isr = new InputStreamReader(is);
//    	BufferedReader br = new BufferedReader(isr);
//    	String line;
//
//    	DecimalFormat df = new DecimalFormat("#.00"); 
//    	
//    	int cont = 0;
//    	
//    	while ((line = br.readLine()) != null) {
//    		if (line.startsWith("SCORE:")) {
//    			System.out.println(line);
//    			double score = Double.parseDouble(line.substring(6));
//    			scores[cont] = df.format(score);
//    			cont++;
//    		}
//    		
//    		if (cont == coords.size())
//    			break;
//    	}
//    	
//    	br.close();
//    	process.destroy();
//    	
//    	System.out.println("TOTAL SCORES COLLECTED: " + cont);
		
		SparkInputStreamReaderRunnable inputStreamReaderRunnable = new SparkInputStreamReaderRunnable(process.getInputStream(), "input", scores);
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();

		SparkInputStreamReaderRunnable errorStreamReaderRunnable = new SparkInputStreamReaderRunnable(process.getErrorStream(), "error", scores);
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();

		System.out.println("Waiting for finish...");
		int exitCode = process.waitFor();
		System.out.println("Finished! Exit code:" + exitCode);
		
		if (exitCode == 0) {
			System.out.println("TOTAL SCORES COLLECTED: " + inputStreamReaderRunnable.getTotalScoresCollected());
			
			return inputStreamReaderRunnable.getScores();
		} else {
			System.out.println("Something went wrong. Returning null scores...");
			return scores;
		}
    }

	public static void main(String[] args) {
		Directions directions = new Directions();
		
		String result = directions.getDirections(43.6623616,-79.395533, 43.7746998,-79.5040829, false);
		System.out.println(result);
	}
}
