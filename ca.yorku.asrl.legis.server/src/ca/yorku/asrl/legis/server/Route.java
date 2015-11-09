package ca.yorku.asrl.legis.server;

import java.util.ArrayList;

import com.google.gson.JsonElement;

public class Route {
	
	private ArrayList<Leg> legs;
	
	public Route(JsonElement element) {
		for(JsonElement e : element.getAsJsonObject().get("legs").getAsJsonArray()) {
			legs.add(new Leg(e));
		}
	}

}
