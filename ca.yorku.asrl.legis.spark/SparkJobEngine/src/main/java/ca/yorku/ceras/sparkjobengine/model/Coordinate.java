package ca.yorku.ceras.sparkjobengine.model;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;


public class Coordinate implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5509264853737783658L;

	@SerializedName(value="lat")
	private String lat;
	
	@SerializedName(value="lng")
	private String lng;
	
	@SerializedName(value="street-name")
	private String streetName;

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getLng() {
		return lng;
	}

	public void setLng(String lng) {
		this.lng = lng;
	}

	public String getStreetName() {
		return streetName;
	}

	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}
	
	@Override
	public String toString() {
		return getLat() + "_" + getLng() + "_" + getStreetName();
	}

}
