package ca.yorku.ceras.sparkjobengine.model;

import java.io.Serializable;

public class GeoLocation implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5240707923205437323L;

	private String id;
	
	private String latitude;
	
	private String longitude;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	
	@Override
	public String toString() {
		return this.id + "-" + this.latitude + "-" + this.longitude;
	}
	
	

}
