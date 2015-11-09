package ca.yorku.asrl.legis.gateway;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/edge")
public class Gateway {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getApplicationServer(@QueryParam("lat") double lat, @QueryParam("long") double _long) {
		for(CloudNode node : CloudNode.values()) {
			if(node.containsPoint(lat, _long)) {
				return "{ status : 200, message : \"OK\", result : { edgeName : \""+node+"\", ipAddress : \""+node.getPublicIPAddress()+"\"}}";
			}
		}
		return "{ status : 422, message : \"Geolocation out of range\", result : {}}";
	}
	
	public static void main(String[] args) {
		Gateway gw = new Gateway();
		System.out.println(gw.getApplicationServer(43.7742762,-79.5041315));
	}
}
