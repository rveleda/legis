package ca.yorku.asrl.legis.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

public class SparkInputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;

    private String name;
    
    private String[] scores;

    public SparkInputStreamReaderRunnable(InputStream is, String name, String[] scores) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
        this.scores = scores;
    }

    public void run() {
    	System.out.println("InputStream " + name + ":");
    	
        try {
        	String line;
        	
            if (name.contains("input")) {
            	int cont = 0;
            	DecimalFormat df = new DecimalFormat("#.00"); 
            	
                while ((line = reader.readLine()) != null) {
            		if (line.startsWith("SCORE:")) {
            			System.out.println(line);
            			double score = Double.parseDouble(line.substring(6));
            			scores[cont] = df.format(score);
            			cont++;
            		}
            		
            		if (cont == scores.length)
            			break;
                }
            } else {
                while ((line = reader.readLine()) != null) {
                	System.out.println(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	if (reader != null) {
        		try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        }
    }

	public String[] getScores() {
		return scores;
	}
	
	public int getTotalScoresCollected() {
		int cont = 0;
		
		for (String score : scores) {
			if (score != null)
				cont++;
		}
		
		return cont;
	}
    
}