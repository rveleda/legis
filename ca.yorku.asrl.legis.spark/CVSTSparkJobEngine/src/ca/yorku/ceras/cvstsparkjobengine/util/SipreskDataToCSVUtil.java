package ca.yorku.ceras.cvstsparkjobengine.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;


/**
 * This class represents utility methods regarding the data collected by the Sipresk spark job (SipreskJob.class)
 * @author rveleda
 *
 */
public class SipreskDataToCSVUtil {

	public static void main(String[] args) {
		//Map<String, Integer> loopDetectorTotal = getLoopDetectorCount("/Users/rveleda/Development/york/master/sipresk/total_LD.csv");
		Map<String, Map<String, Integer>> loopDetectorTotalByMonth = getLoopDetectorCount("/Users/rveleda/Development/york/master/sipresk/total_LD_all_months.csv");
		
		//Map<String, Integer> failureDetectorTotal = getLoopDetectorCount("/Users/rveleda/Development/york/master/sipresk/failure_LD.csv");
		
		Map<String, Map<String, Integer>> failureDetectorTotalByMonth = getLoopDetectorCount("/Users/rveleda/Development/york/master/sipresk/failure_LD_all_months.csv");
		
		failureDetectorTotalByMonth = normalize(loopDetectorTotalByMonth, failureDetectorTotalByMonth);
		
		Map<String, Map<String, Double>> failurePercentageByMonth = new HashMap<String, Map<String, Double>>();
		
//		for (String loopDetectorId : loopDetectorTotal.keySet()) {
//		
//		Integer totalCount = loopDetectorTotal.get(loopDetectorId);
//		Integer failureCount = failureDetectorTotal.get(loopDetectorId);
//		
//		double percentage = ((double) failureCount * 100) / (double) totalCount;
//		
//		failurePercentage.put(loopDetectorId, new Double(percentage));
//	}

		for (String month : loopDetectorTotalByMonth.keySet()) {
			Map<String, Double> failurePercentage = new HashMap<String, Double>();
			
			for (String loopDetectorId : loopDetectorTotalByMonth.get(month).keySet()) {
				Integer totalCount = loopDetectorTotalByMonth.get(month).get(loopDetectorId);
				Integer failureCount = failureDetectorTotalByMonth.get(month).get(loopDetectorId);
				
				double percentage = ((double) failureCount * 100) / (double) totalCount;
				
				failurePercentage.put(loopDetectorId, new Double(percentage));
			}
			
			failurePercentageByMonth.put(month, failurePercentage);
		}
		
		StringBuffer sb = new StringBuffer();

		for (String month : failurePercentageByMonth.keySet()) {
			for (String loopDetectorId : failurePercentageByMonth.get(month).keySet()) {
				sb.append(month + "," + loopDetectorId + "," + failurePercentageByMonth.get(month).get(loopDetectorId));
				sb.append("\n");
			}
		}
		
		try {
			FileUtils.write(new File("/Users/rveleda/Development/york/master/sipresk/failure_percentage_allmonths.csv"), sb.toString(), false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("done is done.");
	}
	
	private static Map<String, Map<String, Integer>> normalize(Map<String, Map<String, Integer>> goldenStandard, Map<String, Map<String, Integer>> target) {
		for (String month : goldenStandard.keySet()) {
			for (String loopDetectorId : goldenStandard.get(month).keySet()) {
				if (target.get(month).get(loopDetectorId) == null) {
					target.get(month).put(loopDetectorId, new Integer(0));
				}
			}
		}
		
		return target;
	}
	
	private static Map<String, Map<String, Integer>> getLoopDetectorCount(String path) {
		
		//String csvFile = "/Users/rveleda/Development/york/master/sipresk/failure_LD.csv";
		String csvFile = path;
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		
		Map<String, Map<String, Integer>> loopDetectorCountByMonth = new HashMap<String, Map<String, Integer>>();

		try {
			
			String currentMonth = "032015";
			Map<String, Integer> loopDetectorCount = new HashMap<String, Integer>();

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
			    // use comma as separator
				String[] loopDetectorData = line.split(cvsSplitBy);
				
				if (!currentMonth.equalsIgnoreCase(loopDetectorData[0])) {
					loopDetectorCountByMonth.put(currentMonth, loopDetectorCount);
					
					loopDetectorCount = null;
					loopDetectorCount = new HashMap<String, Integer>();
					
					currentMonth = loopDetectorData[0];
				}
				
				loopDetectorCount.put(loopDetectorData[1], Integer.parseInt(loopDetectorData[2]));
			}
			
			//Adding the last one
			loopDetectorCountByMonth.put(currentMonth, loopDetectorCount);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return loopDetectorCountByMonth;
	}
}
