package ca.yorku.asrl.legis.server;

import java.util.ArrayList;

import com.google.gson.JsonElement;

public class Leg {
	
	private MapPoint start;
	private MapPoint end;
	private double distance;
	private double duration;
	private ArrayList<Step> steps;
	
	public Leg(JsonElement element) {
		start =  new MapPoint(element.getAsJsonObject().get("start_location").getAsJsonObject().get("lat").getAsDouble(), 
				element.getAsJsonObject().get("start_location").getAsJsonObject().get("lng").getAsDouble());
		end =  new MapPoint(element.getAsJsonObject().get("end_location").getAsJsonObject().get("lat").getAsDouble(), 
				element.getAsJsonObject().get("end_location").getAsJsonObject().get("lng").getAsDouble());
		distance = element.getAsJsonObject().get("distance").getAsJsonObject().get("value").getAsDouble();
		duration = element.getAsJsonObject().get("duration").getAsJsonObject().get("value").getAsDouble();
		for(JsonElement e : element.getAsJsonObject().get("steps").getAsJsonArray()) {
			steps.add(new Step(e));
		}
	}

}
