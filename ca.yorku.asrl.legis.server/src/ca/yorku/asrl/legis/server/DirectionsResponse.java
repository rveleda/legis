package ca.yorku.asrl.legis.server;

import java.util.ArrayList;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DirectionsResponse {
	
	private MapPoint origin;
	private MapPoint destination;
	private ArrayList<Route> routes;
	
	public DirectionsResponse(double origin_long, double origin_lat, double dest_long, double dest_lat, JsonObject json) {
		origin = new MapPoint(origin_long, origin_lat);
		destination = new MapPoint(dest_long, dest_lat);
		for(JsonElement e : json.get("routes").getAsJsonArray()) {
			routes.add(new Route(e));
		}
	}

}
