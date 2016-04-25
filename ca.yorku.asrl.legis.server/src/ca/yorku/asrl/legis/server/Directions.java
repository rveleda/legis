package ca.yorku.asrl.legis.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.annotation.PostConstruct;
import javax.jws.WebResult;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientConfig;

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
	
	private static final String[] SCORES = {"20.23", "20.23", "20.23", "20.23", "19.53", "19.53", "18.80", "20.50", "15.55", "15.70"};
	
	@Context
	private ServletContext context;
	
	//public HashMap<ClientIP,>
	
	private Random rand = new Random();
	
	private int getRandomInt(int min, int max) {
		return rand.nextInt((max-min) + 1) + min;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/pi")
	public String getDirectionsPi() {
		long startTime = System.currentTimeMillis();
		
		BigDecimal pi = this.calculateBigPi(getRandomInt(3000, 4000), getRandomInt(3000, 4000));
		//System.out.println("PI: " + pi);
		
		String result = getDirections(0.0, 0.0, 0.0, 0.0, false);
		
		long endTime = System.currentTimeMillis();

		ServletContextInitializer.logger.info("TOTAL TIME SPENT: " + (endTime-startTime) + " milliseconds");
		
		return result;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getDirections(@QueryParam("origin_lat") double origin_lat, 
			@QueryParam("origin_long") double origin_long, 
			@QueryParam("destination_lat") double destination_lat,
			@QueryParam("destination_long") double destination_long, @QueryParam("dynamic") boolean dynamic) {
		
		long startTime = System.currentTimeMillis();
		
		String origin = ""+origin_lat+","+origin_long;
		String destination = ""+destination_lat+","+destination_long;

		InputStream is = null;
		
		String jsonResponse = "";
		
		if (!dynamic) {
			try {
				//byte[] encoded = Files.readAllBytes(Paths.get("jsonResponse.json"));
				//jsonResponse = new String(encoded, StandardCharsets.UTF_8);
				
				is = context.getResourceAsStream(filename);
				
				jsonResponse = IOUtils.toString(is, "UTF-8");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
//		if (jsonResponse == null || jsonResponse.equals("")) {
//			Response response = ClientBuilder
//					.newClient()
//					.target("https://maps.googleapis.com/maps/api/directions/json")
//					.queryParam("origin", origin)
//					.queryParam("destination", destination)
//					.queryParam("key", GOOGLE_API_KEY)
//					.queryParam("alternatives", "true").request()
//					.get(Response.class);
//			System.out.println("Google request sent.");
//			jsonResponse = response.readEntity(String.class);
//		}
		
		JsonParser parser = new JsonParser();
		JsonReader reader = new JsonReader(new StringReader(jsonResponse));
		reader.setLenient(true);
		JsonObject o = parser.parse(reader).getAsJsonObject();
		//JsonObject localJson = parser.parse(response.readEntity(String.class)).getAsJsonObject();
		
		//System.out.println(jsonResponse);
		JsonObject annotated = null;
		if(o.get("status").getAsString().equals("OK")) {
			annotated = annotate(o);
		}
		
//		try {
//			BufferedWriter writer = new BufferedWriter(new FileWriter("jsonResponse"+origin+".json"));
//			writer.write(jsonResponse);
//			writer.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		try {
			parser = null;
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long endTime = System.currentTimeMillis();
		
		ServletContextInitializer.logger.info("TOTAL TIME SPENT WITHOUT PI: " + (endTime-startTime) + " milliseconds");
		
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
	public JsonObject annotate(JsonObject o) {
		//HashSet<CloudNode> forwardNeighbors = new HashSet<CloudNode>();
		
		StringBuffer sb = new StringBuffer();
		
		for(JsonElement route : o.get("routes").getAsJsonArray()) {
			for(JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for(JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					JsonObject stepObject = step.getAsJsonObject();
					String direction = myNode.getZone().outOfBoundsDirection(stepObject.get("end_location").getAsJsonObject().get("lat").getAsDouble(), 
							stepObject.get("end_location").getAsJsonObject().get("lng").getAsDouble());
//					double score = getScoreForStep(stepObject.toString());
//					stepObject.addProperty("score", score);
					
					String latitude = step.getAsJsonObject().get("end_location").getAsJsonObject().get("lat").getAsString();
					String longitude = step.getAsJsonObject().get("end_location").getAsJsonObject().get("lng").getAsString();
					
					sb.append(latitude).append("+").append(longitude).append("_");
					
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
		
		JsonObject jsonOResponse = callSpark(o, sb.toString());
		/*for(CloudNode neighbor : forwardNeighbors) {
			forwardResponse(neighbor);
		}*/
		//jsonResponse = o.toString();
		return jsonOResponse;
	}

	private JsonObject callSpark(JsonObject o, String coords) {
//		Response response = ClientBuilder.newClient().target("http://10.12.7.25:9000/spark/getScore")
//				.request()
//				.put(Entity.json(json),Response.class);
//		return response.readEntity(String.class);
		
//		return this.calculateScore(o, coords);
		
		long startTime = System.currentTimeMillis();
		
		String sparkMasterIp = context.getInitParameter("sparkMasterIp");
		String cassandraIp = context.getInitParameter("cassandraIp");
		
		String url = "http://"+sparkMasterIp+":6066/v1/submissions/create";

		String payload = "{"+"\"action\" : \"CreateSubmissionRequest\","+
  "\"appArgs\" : [ \""+coords+"\", \"1\" ],"+
  "\"appResource\" : \"file:/job-dependencies/CVSTSparkJobEngine-0.0.1-SNAPSHOT.jar\","+
  "\"clientSparkVersion\" : \"1.3.0\","+
  "\"environmentVariables\" : {"+
    "\"SPARK_ENV_LOADED\" : \"1\""+
  "},"+
  "\"mainClass\" : \"ca.yorku.ceras.cvstsparkjobengine.job.LegisJob\","+
  "\"sparkProperties\" : {"+
	"\"spark.jars\" : \"file:/job-dependencies/CVSTSparkJobEngine-0.0.1-SNAPSHOT.jar\","+
    "\"spark.driver.extraClassPath\" : \"file:/job-dependencies/RoaringBitmap-0.4.5.jar:file:/job-dependencies/activation-1.1.jar:file:/job-dependencies/akka-actor_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/akka-remote_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/akka-slf4j_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/apacheds-i18n-2.0.0-M15.jar:file:/job-dependencies/apacheds-kerberos-codec-2.0.0-M15.jar:file:/job-dependencies/api-asn1-api-1.0.0-M20.jar:file:/job-dependencies/api-util-1.0.0-M20.jar:file:/job-dependencies/asm-3.1.jar:file:/job-dependencies/asm-5.0.3.jar:file:/job-dependencies/asm-analysis-5.0.3.jar:file:/job-dependencies/asm-commons-5.0.3.jar:file:/job-dependencies/asm-tree-5.0.3.jar:file:/job-dependencies/asm-util-5.0.3.jar:file:/job-dependencies/avro-1.7.6-cdh5.3.0.jar:file:/job-dependencies/aws-java-sdk-1.7.14.jar:file:/job-dependencies/cassandra-clientutil-2.1.5.jar:file:/job-dependencies/cassandra-driver-core-2.1.10.jar:file:/job-dependencies/chill-java-0.5.0.jar:file:/job-dependencies/chill_2.10-0.5.0.jar:file:/job-dependencies/commons-beanutils-1.7.0.jar:file:/job-dependencies/commons-beanutils-core-1.8.0.jar:file:/job-dependencies/commons-cli-1.2.jar:file:/job-dependencies/commons-codec-1.7.jar:file:/job-dependencies/commons-collections-3.2.1.jar:file:/job-dependencies/commons-compress-1.4.1.jar:file:/job-dependencies/commons-configuration-1.6.jar:file:/job-dependencies/commons-daemon-1.0.13.jar:file:/job-dependencies/commons-digester-1.8.jar:file:/job-dependencies/commons-el-1.0.jar:file:/job-dependencies/commons-httpclient-3.1.jar:file:/job-dependencies/commons-io-2.4.jar:file:/job-dependencies/commons-lang-2.6.jar:file:/job-dependencies/commons-lang3-3.3.2.jar:file:/job-dependencies/commons-logging-1.1.1.jar:file:/job-dependencies/commons-math-2.1.jar:file:/job-dependencies/commons-math3-3.2.jar:file:/job-dependencies/commons-net-2.2.jar:file:/job-dependencies/compress-lzf-1.0.0.jar:file:/job-dependencies/config-1.0.2.jar:file:/job-dependencies/core-3.1.1.jar:file:/job-dependencies/curator-client-2.6.0.jar:file:/job-dependencies/curator-framework-2.4.0.jar:file:/job-dependencies/curator-recipes-2.4.0.jar:file:/job-dependencies/findbugs-annotations-1.3.9-1.jar:file:/job-dependencies/gson-2.4.jar:file:/job-dependencies/guava-16.0.1.jar:file:/job-dependencies/hadoop-annotations-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-auth-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-aws-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-client-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-common-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-core-2.5.0-mr1-cdh5.3.0.jar:file:/job-dependencies/hadoop-hdfs-2.5.0-cdh5.3.0-tests.jar:file:/job-dependencies/hadoop-hdfs-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-mapreduce-client-app-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-core-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-shuffle-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-api-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-client-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-server-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hamcrest-core-1.3.jar:file:/job-dependencies/hbase-client-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-common-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-common-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-hadoop-compat-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-hadoop-compat-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-hadoop2-compat-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-hadoop2-compat-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-prefix-tree-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-protocol-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-server-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-server-0.98.6-cdh5.3.0.jar:file:/job-dependencies/high-scale-lib-1.1.1.jar:file:/job-dependencies/hsqldb-1.8.0.10.jar:file:/job-dependencies/htrace-core-2.04.jar:file:/job-dependencies/httpclient-4.1.2.jar:file:/job-dependencies/httpcore-4.1.2.jar:file:/job-dependencies/ivy-2.4.0.jar:file:/job-dependencies/jackson-annotations-2.2.3.jar:file:/job-dependencies/jackson-core-2.2.3.jar:file:/job-dependencies/jackson-core-asl-1.8.8.jar:file:/job-dependencies/jackson-databind-2.2.3.jar:file:/job-dependencies/jackson-jaxrs-1.8.8.jar:file:/job-dependencies/jackson-mapper-asl-1.8.8.jar:file:/job-dependencies/jackson-module-scala_2.10-2.2.3.jar:file:/job-dependencies/jackson-xc-1.7.1.jar:file:/job-dependencies/jamon-runtime-2.3.1.jar:file:/job-dependencies/jasper-compiler-5.5.23.jar:file:/job-dependencies/jasper-runtime-5.5.23.jar:file:/job-dependencies/java-xmlbuilder-0.4.jar:file:/job-dependencies/javax.servlet-3.0.0.v201112011016.jar:file:/job-dependencies/jaxb-api-2.1.jar:file:/job-dependencies/jaxb-impl-2.2.3-1.jar:file:/job-dependencies/jcl-over-slf4j-1.7.5.jar:file:/job-dependencies/jersey-client-1.9.jar:file:/job-dependencies/jersey-core-1.8.jar:file:/job-dependencies/jersey-json-1.8.jar:file:/job-dependencies/jersey-server-1.8.jar:file:/job-dependencies/jets3t-0.9.0.jar:file:/job-dependencies/jettison-1.1.jar:file:/job-dependencies/jetty-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-continuation-8.1.14.v20131031.jar:file:/job-dependencies/jetty-http-8.1.14.v20131031.jar:file:/job-dependencies/jetty-io-8.1.14.v20131031.jar:file:/job-dependencies/jetty-server-8.1.14.v20131031.jar:file:/job-dependencies/jetty-sslengine-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-util-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-util-8.1.14.v20131031.jar:file:/job-dependencies/jffi-1.2.10-native.jar:file:/job-dependencies/jffi-1.2.10.jar:file:/job-dependencies/jnr-constants-0.9.0.jar:file:/job-dependencies/jnr-ffi-2.0.7.jar:file:/job-dependencies/jnr-posix-3.0.27.jar:file:/job-dependencies/jnr-x86asm-1.0.2.jar:file:/job-dependencies/joda-convert-1.2.jar:file:/job-dependencies/joda-time-2.3.jar:file:/job-dependencies/jodd-core-3.6.3.jar:file:/job-dependencies/jsch-0.1.42.jar:file:/job-dependencies/json4s-ast_2.10-3.2.10.jar:file:/job-dependencies/json4s-core_2.10-3.2.10.jar:file:/job-dependencies/json4s-jackson_2.10-3.2.10.jar:file:/job-dependencies/jsp-2.1-6.1.14.jar:file:/job-dependencies/jsp-api-2.1-6.1.14.jar:file:/job-dependencies/jsp-api-2.1.jar:file:/job-dependencies/jsr166e-1.1.0.jar:file:/job-dependencies/jsr305-1.3.9.jar:file:/job-dependencies/jul-to-slf4j-1.7.5.jar:file:/job-dependencies/junit-4.11.jar:file:/job-dependencies/kryo-2.21.jar:file:/job-dependencies/leveldbjni-all-1.8.jar:file:/job-dependencies/log4j-1.2.17.jar:file:/job-dependencies/lz4-1.2.0.jar:file:/job-dependencies/mesos-0.21.0-shaded-protobuf.jar:file:/job-dependencies/metrics-core-2.2.0.jar:file:/job-dependencies/metrics-core-3.0.2.jar:file:/job-dependencies/metrics-core-3.1.0.jar:file:/job-dependencies/metrics-graphite-3.1.0.jar:file:/job-dependencies/metrics-json-3.1.0.jar:file:/job-dependencies/metrics-jvm-3.1.0.jar:file:/job-dependencies/minlog-1.2.jar:file:/job-dependencies/netty-3.9.0.Final.jar:file:/job-dependencies/netty-all-4.0.23.Final.jar:file:/job-dependencies/netty-buffer-4.0.33.Final.jar:file:/job-dependencies/netty-codec-4.0.33.Final.jar:file:/job-dependencies/netty-common-4.0.33.Final.jar:file:/job-dependencies/netty-handler-4.0.33.Final.jar:file:/job-dependencies/netty-transport-4.0.33.Final.jar:file:/job-dependencies/objenesis-1.2.jar:file:/job-dependencies/oro-2.0.8.jar:file:/job-dependencies/paranamer-2.3.jar:file:/job-dependencies/parquet-column-1.6.0rc3.jar:file:/job-dependencies/parquet-common-1.6.0rc3.jar:file:/job-dependencies/parquet-encoding-1.6.0rc3.jar:file:/job-dependencies/parquet-format-2.2.0-rc1.jar:file:/job-dependencies/parquet-generator-1.6.0rc3.jar:file:/job-dependencies/parquet-hadoop-1.6.0rc3.jar:file:/job-dependencies/parquet-jackson-1.6.0rc3.jar:file:/job-dependencies/protobuf-java-2.4.1-shaded.jar:file:/job-dependencies/protobuf-java-2.5.0.jar:file:/job-dependencies/py4j-0.8.2.1.jar:file:/job-dependencies/pyrolite-2.0.1.jar:file:/job-dependencies/quasiquotes_2.10-2.0.1.jar:file:/job-dependencies/reflectasm-1.07-shaded.jar:file:/job-dependencies/scala-compiler-2.10.4.jar:file:/job-dependencies/scala-library-2.10.4.jar:file:/job-dependencies/scala-reflect-2.10.5.jar:file:/job-dependencies/scalap-2.10.0.jar:file:/job-dependencies/scalatest_2.10-2.1.5.jar:file:/job-dependencies/servlet-api-2.5-6.1.14.jar:file:/job-dependencies/servlet-api-2.5.jar:file:/job-dependencies/slf4j-api-1.7.5.jar:file:/job-dependencies/slf4j-log4j12-1.7.5.jar:file:/job-dependencies/snappy-java-1.0.4.1.jar:file:/job-dependencies/spark-cassandra-connector-java_2.10-1.3.1.jar:file:/job-dependencies/spark-cassandra-connector_2.10-1.3.1.jar:file:/job-dependencies/spark-catalyst_2.10-1.3.0.jar:file:/job-dependencies/spark-core_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-hbase-0.0.2-clabs.jar:file:/job-dependencies/spark-network-common_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-network-shuffle_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-sql_2.10-1.3.0.jar:file:/job-dependencies/spark-streaming_2.10-1.2.0-cdh5.3.0-tests.jar:file:/job-dependencies/spark-streaming_2.10-1.2.0-cdh5.3.0.jar:file:/job-dependencies/stream-2.7.0.jar:file:/job-dependencies/tachyon-0.5.0.jar:file:/job-dependencies/tachyon-client-0.5.0.jar:file:/job-dependencies/uncommons-maths-1.2.2a.jar:file:/job-dependencies/unused-1.0.0.jar:file:/job-dependencies/xmlenc-0.52.jar:file:/job-dependencies/xz-1.0.jar:file:/job-dependencies/zookeeper-3.4.5-cdh5.3.0.jar\","+
	"\"spark.executor.extraClassPath\" : \"file:/job-dependencies/RoaringBitmap-0.4.5.jar:file:/job-dependencies/activation-1.1.jar:file:/job-dependencies/akka-actor_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/akka-remote_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/akka-slf4j_2.10-2.2.3-shaded-protobuf.jar:file:/job-dependencies/apacheds-i18n-2.0.0-M15.jar:file:/job-dependencies/apacheds-kerberos-codec-2.0.0-M15.jar:file:/job-dependencies/api-asn1-api-1.0.0-M20.jar:file:/job-dependencies/api-util-1.0.0-M20.jar:file:/job-dependencies/asm-3.1.jar:file:/job-dependencies/asm-5.0.3.jar:file:/job-dependencies/asm-analysis-5.0.3.jar:file:/job-dependencies/asm-commons-5.0.3.jar:file:/job-dependencies/asm-tree-5.0.3.jar:file:/job-dependencies/asm-util-5.0.3.jar:file:/job-dependencies/avro-1.7.6-cdh5.3.0.jar:file:/job-dependencies/aws-java-sdk-1.7.14.jar:file:/job-dependencies/cassandra-clientutil-2.1.5.jar:file:/job-dependencies/cassandra-driver-core-2.1.10.jar:file:/job-dependencies/chill-java-0.5.0.jar:file:/job-dependencies/chill_2.10-0.5.0.jar:file:/job-dependencies/commons-beanutils-1.7.0.jar:file:/job-dependencies/commons-beanutils-core-1.8.0.jar:file:/job-dependencies/commons-cli-1.2.jar:file:/job-dependencies/commons-codec-1.7.jar:file:/job-dependencies/commons-collections-3.2.1.jar:file:/job-dependencies/commons-compress-1.4.1.jar:file:/job-dependencies/commons-configuration-1.6.jar:file:/job-dependencies/commons-daemon-1.0.13.jar:file:/job-dependencies/commons-digester-1.8.jar:file:/job-dependencies/commons-el-1.0.jar:file:/job-dependencies/commons-httpclient-3.1.jar:file:/job-dependencies/commons-io-2.4.jar:file:/job-dependencies/commons-lang-2.6.jar:file:/job-dependencies/commons-lang3-3.3.2.jar:file:/job-dependencies/commons-logging-1.1.1.jar:file:/job-dependencies/commons-math-2.1.jar:file:/job-dependencies/commons-math3-3.2.jar:file:/job-dependencies/commons-net-2.2.jar:file:/job-dependencies/compress-lzf-1.0.0.jar:file:/job-dependencies/config-1.0.2.jar:file:/job-dependencies/core-3.1.1.jar:file:/job-dependencies/curator-client-2.6.0.jar:file:/job-dependencies/curator-framework-2.4.0.jar:file:/job-dependencies/curator-recipes-2.4.0.jar:file:/job-dependencies/findbugs-annotations-1.3.9-1.jar:file:/job-dependencies/gson-2.4.jar:file:/job-dependencies/guava-16.0.1.jar:file:/job-dependencies/hadoop-annotations-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-auth-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-aws-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-client-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-common-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-core-2.5.0-mr1-cdh5.3.0.jar:file:/job-dependencies/hadoop-hdfs-2.5.0-cdh5.3.0-tests.jar:file:/job-dependencies/hadoop-hdfs-2.5.0-cdh5.3.0.jar:file:/job-dependencies/hadoop-mapreduce-client-app-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-core-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-mapreduce-client-shuffle-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-api-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-client-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hadoop-yarn-server-common-2.6.0-cdh5.4.7.jar:file:/job-dependencies/hamcrest-core-1.3.jar:file:/job-dependencies/hbase-client-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-common-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-common-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-hadoop-compat-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-hadoop-compat-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-hadoop2-compat-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-hadoop2-compat-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-prefix-tree-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-protocol-0.98.6-cdh5.3.0.jar:file:/job-dependencies/hbase-server-0.98.6-cdh5.3.0-tests.jar:file:/job-dependencies/hbase-server-0.98.6-cdh5.3.0.jar:file:/job-dependencies/high-scale-lib-1.1.1.jar:file:/job-dependencies/hsqldb-1.8.0.10.jar:file:/job-dependencies/htrace-core-2.04.jar:file:/job-dependencies/httpclient-4.1.2.jar:file:/job-dependencies/httpcore-4.1.2.jar:file:/job-dependencies/ivy-2.4.0.jar:file:/job-dependencies/jackson-annotations-2.2.3.jar:file:/job-dependencies/jackson-core-2.2.3.jar:file:/job-dependencies/jackson-core-asl-1.8.8.jar:file:/job-dependencies/jackson-databind-2.2.3.jar:file:/job-dependencies/jackson-jaxrs-1.8.8.jar:file:/job-dependencies/jackson-mapper-asl-1.8.8.jar:file:/job-dependencies/jackson-module-scala_2.10-2.2.3.jar:file:/job-dependencies/jackson-xc-1.7.1.jar:file:/job-dependencies/jamon-runtime-2.3.1.jar:file:/job-dependencies/jasper-compiler-5.5.23.jar:file:/job-dependencies/jasper-runtime-5.5.23.jar:file:/job-dependencies/java-xmlbuilder-0.4.jar:file:/job-dependencies/javax.servlet-3.0.0.v201112011016.jar:file:/job-dependencies/jaxb-api-2.1.jar:file:/job-dependencies/jaxb-impl-2.2.3-1.jar:file:/job-dependencies/jcl-over-slf4j-1.7.5.jar:file:/job-dependencies/jersey-client-1.9.jar:file:/job-dependencies/jersey-core-1.8.jar:file:/job-dependencies/jersey-json-1.8.jar:file:/job-dependencies/jersey-server-1.8.jar:file:/job-dependencies/jets3t-0.9.0.jar:file:/job-dependencies/jettison-1.1.jar:file:/job-dependencies/jetty-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-continuation-8.1.14.v20131031.jar:file:/job-dependencies/jetty-http-8.1.14.v20131031.jar:file:/job-dependencies/jetty-io-8.1.14.v20131031.jar:file:/job-dependencies/jetty-server-8.1.14.v20131031.jar:file:/job-dependencies/jetty-sslengine-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-util-6.1.26.cloudera.4.jar:file:/job-dependencies/jetty-util-8.1.14.v20131031.jar:file:/job-dependencies/jffi-1.2.10-native.jar:file:/job-dependencies/jffi-1.2.10.jar:file:/job-dependencies/jnr-constants-0.9.0.jar:file:/job-dependencies/jnr-ffi-2.0.7.jar:file:/job-dependencies/jnr-posix-3.0.27.jar:file:/job-dependencies/jnr-x86asm-1.0.2.jar:file:/job-dependencies/joda-convert-1.2.jar:file:/job-dependencies/joda-time-2.3.jar:file:/job-dependencies/jodd-core-3.6.3.jar:file:/job-dependencies/jsch-0.1.42.jar:file:/job-dependencies/json4s-ast_2.10-3.2.10.jar:file:/job-dependencies/json4s-core_2.10-3.2.10.jar:file:/job-dependencies/json4s-jackson_2.10-3.2.10.jar:file:/job-dependencies/jsp-2.1-6.1.14.jar:file:/job-dependencies/jsp-api-2.1-6.1.14.jar:file:/job-dependencies/jsp-api-2.1.jar:file:/job-dependencies/jsr166e-1.1.0.jar:file:/job-dependencies/jsr305-1.3.9.jar:file:/job-dependencies/jul-to-slf4j-1.7.5.jar:file:/job-dependencies/junit-4.11.jar:file:/job-dependencies/kryo-2.21.jar:file:/job-dependencies/leveldbjni-all-1.8.jar:file:/job-dependencies/log4j-1.2.17.jar:file:/job-dependencies/lz4-1.2.0.jar:file:/job-dependencies/mesos-0.21.0-shaded-protobuf.jar:file:/job-dependencies/metrics-core-2.2.0.jar:file:/job-dependencies/metrics-core-3.0.2.jar:file:/job-dependencies/metrics-core-3.1.0.jar:file:/job-dependencies/metrics-graphite-3.1.0.jar:file:/job-dependencies/metrics-json-3.1.0.jar:file:/job-dependencies/metrics-jvm-3.1.0.jar:file:/job-dependencies/minlog-1.2.jar:file:/job-dependencies/netty-3.9.0.Final.jar:file:/job-dependencies/netty-all-4.0.23.Final.jar:file:/job-dependencies/netty-buffer-4.0.33.Final.jar:file:/job-dependencies/netty-codec-4.0.33.Final.jar:file:/job-dependencies/netty-common-4.0.33.Final.jar:file:/job-dependencies/netty-handler-4.0.33.Final.jar:file:/job-dependencies/netty-transport-4.0.33.Final.jar:file:/job-dependencies/objenesis-1.2.jar:file:/job-dependencies/oro-2.0.8.jar:file:/job-dependencies/paranamer-2.3.jar:file:/job-dependencies/parquet-column-1.6.0rc3.jar:file:/job-dependencies/parquet-common-1.6.0rc3.jar:file:/job-dependencies/parquet-encoding-1.6.0rc3.jar:file:/job-dependencies/parquet-format-2.2.0-rc1.jar:file:/job-dependencies/parquet-generator-1.6.0rc3.jar:file:/job-dependencies/parquet-hadoop-1.6.0rc3.jar:file:/job-dependencies/parquet-jackson-1.6.0rc3.jar:file:/job-dependencies/protobuf-java-2.4.1-shaded.jar:file:/job-dependencies/protobuf-java-2.5.0.jar:file:/job-dependencies/py4j-0.8.2.1.jar:file:/job-dependencies/pyrolite-2.0.1.jar:file:/job-dependencies/quasiquotes_2.10-2.0.1.jar:file:/job-dependencies/reflectasm-1.07-shaded.jar:file:/job-dependencies/scala-compiler-2.10.4.jar:file:/job-dependencies/scala-library-2.10.4.jar:file:/job-dependencies/scala-reflect-2.10.5.jar:file:/job-dependencies/scalap-2.10.0.jar:file:/job-dependencies/scalatest_2.10-2.1.5.jar:file:/job-dependencies/servlet-api-2.5-6.1.14.jar:file:/job-dependencies/servlet-api-2.5.jar:file:/job-dependencies/slf4j-api-1.7.5.jar:file:/job-dependencies/slf4j-log4j12-1.7.5.jar:file:/job-dependencies/snappy-java-1.0.4.1.jar:file:/job-dependencies/spark-cassandra-connector-java_2.10-1.3.1.jar:file:/job-dependencies/spark-cassandra-connector_2.10-1.3.1.jar:file:/job-dependencies/spark-catalyst_2.10-1.3.0.jar:file:/job-dependencies/spark-core_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-hbase-0.0.2-clabs.jar:file:/job-dependencies/spark-network-common_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-network-shuffle_2.10-1.3.0-cdh5.4.7.jar:file:/job-dependencies/spark-sql_2.10-1.3.0.jar:file:/job-dependencies/spark-streaming_2.10-1.2.0-cdh5.3.0-tests.jar:file:/job-dependencies/spark-streaming_2.10-1.2.0-cdh5.3.0.jar:file:/job-dependencies/stream-2.7.0.jar:file:/job-dependencies/tachyon-0.5.0.jar:file:/job-dependencies/tachyon-client-0.5.0.jar:file:/job-dependencies/uncommons-maths-1.2.2a.jar:file:/job-dependencies/unused-1.0.0.jar:file:/job-dependencies/xmlenc-0.52.jar:file:/job-dependencies/xz-1.0.jar:file:/job-dependencies/zookeeper-3.4.5-cdh5.3.0.jar\","+
    "\"spark.driver.supervise\" : \"false\","+
    "\"spark.app.name\" : \"ca.yorku.ceras.cvstsparkjobengine.job.LegisJob\","+
    "\"spark.eventLog.enabled\": \"false\","+
    "\"spark.submit.deployMode\" : \"cluster\","+
    "\"spark.master\" : \"spark://"+sparkMasterIp+":6066\","+
	"\"spark.cassandra.connection.host\" : \""+cassandraIp+"\","+
    "\"spark.executor.cores\" : \"1\","+
    "\"spark.cores.max\" : \"1\""+
  "}"+
"}'";
		
		Client client = ClientBuilder.newClient();
		Response response = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
		
		// check response status code
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }
        
		JsonReader reader = new JsonReader(new StringReader(response.readEntity(String.class)));
		reader.setLenient(true);
		JsonObject jsonResponse = new JsonParser().parse(reader).getAsJsonObject();
		
		String submissionId = jsonResponse.get("submissionId").getAsString();
		
		url = "http://"+sparkMasterIp+":6066/v1/submissions/status/" + submissionId;
		
		String status = "";
		
		int attempts = 0;
		
		while (attempts < 600) {			
			response = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get();
			
			// check response status code
	        if (response.getStatus() != 200) {
	            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
	        }
	        
	        String result = response.readEntity(String.class);
	        
	        //System.out.println("RESULT: " + result);
	        
	        if (result.contains("FINISHED")) {
	        	status = "FINISHED";
	        	break;
	        } else if (result.contains("FAILED")) {
	        	status = "FAILED";
	        	break;
	        }
	        
	        try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        
	        attempts++;
		}
		
		if (status.equals(""))
			throw new RuntimeException("Failed to retrieve Spark job status");
		
		try {
			reader.close();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int cont = 0;
		
		for (JsonElement route : o.get("routes").getAsJsonArray()) {
			for (JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
				for (JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
					JsonObject stepObject = step.getAsJsonObject();
					
					if (status.equals("FINISHED")) {
						stepObject.addProperty("score", ""+SCORES[cont]);
						cont++;
					} else {
						stepObject.addProperty("score", "FAILED");
					}
				}
			}
		}
		
		long endTime = System.currentTimeMillis();
		
		ServletContextInitializer.logger.info("TIME SPENT WITH SPARK: " + (endTime-startTime) + " milliseconds");
		
		return o;
	}

//	private void forwardResponse(CloudNode neighbor) {
//		Response response = ClientBuilder.newClient().target("http://"+neighbor.getPrivateIPAddress()+":8080/ca.yorku.asrl.legis.server/rest/directions/annotate")
//		.request()
//		.put(Entity.json(jsonResponse),Response.class);
//		jsonResponse = response.readEntity(String.class);
//		
//	}

//	private double getScoreForStep(String string) {
//		Random gen = new Random();
//		return gen.nextDouble()*100;
//	}
	
	private JsonObject calculateScore(JsonObject o, String coords) {
		System.out.println("COORDS: " + coords);

	    if (coords.length() > 0) {
	    	try {
	    		Object[] scores = (Object[]) this.getScoreFromSpark(coords);
	    		
	    		int cont = 0;
	    		
	    		for (JsonElement route : o.get("routes").getAsJsonArray()) {
	    			for (JsonElement leg : route.getAsJsonObject().get("legs").getAsJsonArray()) {
	    				for (JsonElement step : leg.getAsJsonObject().get("steps").getAsJsonArray()) {
	    					JsonObject stepObject = step.getAsJsonObject();
	    					stepObject.addProperty("score", ""+scores[cont]);
	    					cont++;
	    				}
	    			}
	    		}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
	    
	    return o;
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
		
		inputThread = null;
		errorThread = null;
		
		if (exitCode == 0) {
			int totalScores = inputStreamReaderRunnable.getTotalScoresCollected();
			scores = inputStreamReaderRunnable.getScores();
			
			System.out.println("TOTAL SCORES COLLECTED: " + totalScores);
			
			inputStreamReaderRunnable = null;
			errorStreamReaderRunnable = null;
			
			return scores;
		} else {
			inputStreamReaderRunnable = null;
			errorStreamReaderRunnable = null;
			
			System.out.println("Something went wrong. Returning null scores...");
			return scores;
		}
    }
    
	public BigDecimal calculateBigPi(int N, int cntDigits) {
		long startTime = System.currentTimeMillis();
		
		BigDecimal sum = BigDecimal.ZERO;
		BigDecimal term = BigDecimal.ZERO;
		double sign = 1.0;

		for (int k = 0; k < N; ++k)
		{
			BigDecimal tmp = BigDecimal.valueOf(2.0).multiply(BigDecimal.valueOf(k)).add(BigDecimal.ONE);
			term = BigDecimal.ONE.divide(tmp, cntDigits, BigDecimal.ROUND_HALF_UP);
			sum = sum.add(term.multiply(BigDecimal.valueOf(sign)));
			
			sign = -sign;
		}
		
		sum = sum.multiply(BigDecimal.valueOf(4.0));
		sign = (N % 2 == 0) ? 1.0 : -1.0;
		
		BigDecimal signBig = BigDecimal.valueOf(sign);
		BigDecimal NBig = BigDecimal.valueOf(N);

		
		// 1. correction term: sum += sign * 1.0/N;
		sum = sum.add(signBig.multiply(BigDecimal.ONE.divide(NBig, cntDigits, BigDecimal.ROUND_HALF_UP)));
		// 2. correction term: sum -= sign * 1.0/(4 * N^3);
		{
			BigDecimal Npow3Big = NBig.pow(3);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(4));
			BigDecimal termBig = BigDecimal.ONE.divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.subtract(signBig.multiply(termBig));
		}
		// 3. correction term: 		sum += sign * 5.0/(16 * N^5);
		{
			BigDecimal Npow3Big = NBig.pow(5);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(16));
			BigDecimal termBig = BigDecimal.valueOf(5).divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.add(signBig.multiply(termBig));
		}
		// 4. correction term: 		sum -= sign * 61.0/(64 * N^7);
		{
			BigDecimal Npow3Big = NBig.pow(7);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(64));
			BigDecimal termBig = BigDecimal.valueOf(61).divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.subtract(signBig.multiply(termBig));
		}
		// 5. correction term: 		sum += sign * 1385.0/(256 * N^9);
		{
			BigDecimal Npow3Big = NBig.pow(9);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(256));
			BigDecimal termBig = BigDecimal.valueOf(1385).divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.add(signBig.multiply(termBig));
		}
		// 6. correction term: 		sum -= sign * 50521.0/(1024 * N^11);
		{
			BigDecimal Npow3Big = NBig.pow(11);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(1024));
			BigDecimal termBig = BigDecimal.valueOf(50521).divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.subtract(signBig.multiply(termBig));
		}
		// 7. correction term: 		sum += sign * 2702765.0/(4096 * N^13);
		{
			BigDecimal Npow3Big = NBig.pow(13);
			BigDecimal divisorBig = Npow3Big.multiply(BigDecimal.valueOf(4096));
			BigDecimal termBig = BigDecimal.valueOf(2702765).divide(divisorBig, cntDigits, BigDecimal.ROUND_HALF_UP);
			
			sum = sum.add(signBig.multiply(termBig));
		}

		long endTime = System.currentTimeMillis();
		
		ServletContextInitializer.logger.info("TIME SPENT WITH PI: " + (endTime-startTime) + " milliseconds");
		
		return sum;
	}

	public static void main(String[] args) {
		Directions directions = new Directions();
		
		String result = directions.getDirections(43.6623616,-79.395533, 43.7746998,-79.5040829, false);
		System.out.println(result);
	}
}
