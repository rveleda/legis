package ca.yorku.asrl.legis.server;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ServletContextInitializer implements ServletContextListener {

	public static Logger logger = Logger.getLogger("webapp-legis-log");
	
	private FileHandler fh;
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		if (fh != null) {
			fh.flush();
			fh.close();
		}
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		try {
	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler("/tmp/webapp-legis.log");
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }
		
		logger.info("Log file created...");
	}
	
	

}
