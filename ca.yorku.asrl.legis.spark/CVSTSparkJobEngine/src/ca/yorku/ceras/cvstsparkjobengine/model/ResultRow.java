package ca.yorku.ceras.cvstsparkjobengine.model;

import java.io.Serializable;

/**
 * This class represents a given Row from the target HBase table
 * @author rveleda
 *
 */
public class ResultRow implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8380379759238085874L;
	
	private String content;
	
	public ResultRow() {
		this.content = "";
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
