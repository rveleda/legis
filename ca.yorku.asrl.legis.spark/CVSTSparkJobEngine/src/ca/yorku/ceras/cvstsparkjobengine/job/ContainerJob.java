package ca.yorku.ceras.cvstsparkjobengine.job;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;


public class ContainerJob {
	
	public static final Logger log = Logger.getLogger(ContainerJob.class);
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
        conf.setAppName("ca.yorku.ceras.cvstsparkjobengine.job.ContainerJob");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        
        // Prepare the schema
        try {
        	Session session = connector.openSession();
        	
            session.execute("DROP KEYSPACE IF EXISTS java_api");
            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE java_api.cvst (id INT PRIMARY KEY, contractId TEXT, vdsId TEXT, "
            		+ "laneOcc DECIMAL, laneSpd DECIMAL, laneVol DECIMAL, laneLength DECIMAL, occ DECIMAL, "
            		+ "spd DECIMAL, vol DECIMAL, laneNumber INT, timestamp BIGINT)");
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
//        JavaRDD<CVSTData> data = sc.textFile("/Users/rveleda/Development/york/master/sipresk/rawdata_070715.csv").map(new Function<String, CVSTData>() {
//        	int cont = 1;
//        	
//			@Override
//			public CVSTData call(String row) throws Exception {
//				String[] fields = row.split(",");
//				
//				CVSTData d = new CVSTData();
//				d.setId(new Integer(cont));
//				d.setContractId(fields[1]);
//				d.setVdsId(fields[2]);
//				d.setLaneOcc(Double.parseDouble(fields[3]));
//				d.setLaneSpd(Double.parseDouble(fields[4]));
//				d.setLaneVol(Double.parseDouble(fields[5]));
//				d.setLaneLength(Double.parseDouble(fields[6]));
//				d.setOcc(Double.parseDouble(fields[7]));
//				d.setSpd(Double.parseDouble(fields[8]));
//				d.setVol(Double.parseDouble(fields[9]));
//				d.setLaneNumber(Integer.parseInt(fields[10]));
//				d.setTimestamp(Long.parseLong(fields[11]));
//				
//				cont++;
//				
//				return d;
//			}
//        });
//
//        CassandraJavaUtil.javaFunctions(data, CVSTData.class).saveToCassandra("java_api", "cvst");
        
        log.info("Done!");
	}

}
