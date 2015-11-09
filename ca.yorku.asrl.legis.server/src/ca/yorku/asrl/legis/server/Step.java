package ca.yorku.asrl.legis.server;

import java.util.ArrayList;

import com.google.gson.JsonElement;

public class Step {
	
	private MapPoint start;
	private MapPoint end;
	private double distance;
	private double duration;
	private String polyline_points;
	private String travel_mode;
	private double score;
	
	public Step(JsonElement element) {
		start =  new MapPoint(element.getAsJsonObject().get("start_location").getAsJsonObject().get("lat").getAsDouble(), 
				element.getAsJsonObject().get("start_location").getAsJsonObject().get("lng").getAsDouble());
		end =  new MapPoint(element.getAsJsonObject().get("end_location").getAsJsonObject().get("lat").getAsDouble(), 
				element.getAsJsonObject().get("end_location").getAsJsonObject().get("lng").getAsDouble());
		distance = element.getAsJsonObject().get("distance").getAsJsonObject().get("value").getAsDouble();
		duration = element.getAsJsonObject().get("duration").getAsJsonObject().get("value").getAsDouble();
		polyline_points = element.getAsJsonObject().get("polyline").getAsJsonObject().get("points").getAsString();
		travel_mode = element.getAsJsonObject().get("travel_mode").getAsString();
	}

	public MapPoint getStart() {
		return start;
	}

	public void setStart(MapPoint start) {
		this.start = start;
	}

	public MapPoint getEnd() {
		return end;
	}

	public void setEnd(MapPoint end) {
		this.end = end;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public double getDuration() {
		return duration;
	}

	public void setDuration(double duration) {
		this.duration = duration;
	}

	public String getPolyline_points() {
		return polyline_points;
	}

	public void setPolyline_points(String polyline_points) {
		this.polyline_points = polyline_points;
	}

	public String getTravel_mode() {
		return travel_mode;
	}

	public void setTravel_mode(String travel_mode) {
		this.travel_mode = travel_mode;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
}
