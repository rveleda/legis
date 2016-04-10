package ca.yorku.ceras.cvstsparkjobengine.model;

import java.io.Serializable;

public class LegisData implements Serializable {
	
	private String rowkey;
	
	private String id;
	
	private Double latitude;
	
	private Double longitude;
	
	private Double speed;
	
	private Double occ;
	
	private Double vol;
	
	private Double lanenumber;
	
	private Long timestamp;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Double getSpeed() {
		return speed;
	}

	public void setSpeed(Double speed) {
		this.speed = speed;
	}

	public Double getOcc() {
		return occ;
	}

	public void setOcc(Double occ) {
		this.occ = occ;
	}

	public Double getVol() {
		return vol;
	}

	public void setVol(Double vol) {
		this.vol = vol;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public Double getLanenumber() {
		return lanenumber;
	}

	public void setLanenumber(Double lanenumber) {
		this.lanenumber = lanenumber;
	}
	
}
