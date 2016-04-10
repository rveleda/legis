package ca.yorku.ceras.cvstsparkjobengine.job;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ca.yorku.ceras.cvstsparkjobengine.model.LegisData;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

/**
 * This class represents a Spark Job used for the Sipresk project
 * @author rveleda
 *
 */
public class DataImporterJob {
	
	public static final Logger log = Logger.getLogger(DataImporterJob.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
        conf.setAppName("ca.yorku.ceras.cvstsparkjobengine.job.DataImporterJob");
        
        JavaSparkContext jsc = new JavaSparkContext(conf);
        
        CassandraConnector connector = CassandraConnector.apply(jsc.getConf());
        
    	Session session = connector.openSession();
    	
        session.execute("DROP KEYSPACE IF EXISTS legis");
        session.execute("CREATE KEYSPACE legis WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE legis.legis_data_all (rowkey TEXT PRIMARY KEY, id TEXT, latitude DECIMAL, "
        		+ "longitude DECIMAL, speed DECIMAL, occ DECIMAL, vol DECIMAL, lanenumber DECIMAL, timestamp BIGINT)");
        
        JavaRDD<LegisData> data = jsc.textFile("/home/legis_rawdata_April1_2015_13h-18h.csv").map(new Function<String, LegisData>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public LegisData call(String row) throws Exception {
				String[] fields = row.split(",");

				LegisData data = new LegisData();
				data.setRowkey(fields[0]);
				data.setId(fields[1]);
				data.setLatitude(Double.parseDouble(fields[2]));
				data.setLongitude(Double.parseDouble(fields[3]));
				data.setSpeed(Double.parseDouble(fields[4]));
				data.setOcc(Double.parseDouble(fields[5]));
				data.setVol(Double.parseDouble(fields[6]));
				data.setLanenumber(Double.parseDouble(fields[7]));
				data.setTimestamp(Long.parseLong(fields[8]));
				
				return data;
			}
        });
        
        CassandraJavaUtil.javaFunctions(data).writerBuilder("legis", "legis_data_all", CassandraJavaUtil.mapToRow(LegisData.class)).saveToCassandra();

        //CassandraJavaUtil.javaFunctions(data, LegisData.class).saveToCassandra("java_api", "legis_data_all");
        
        log.info("Done!");
	
		log.info("done is done");
		System.out.println("done is done.");
		
		jsc.close();
	}

}
