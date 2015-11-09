package ca.yorku.asrl.legis.gateway;

public enum CloudNode {
	
	EDGE_TR_1 ("142.150.208.145", "10.2.7.18", new Border(43.765545, 42.900100, -79.162050, -79.613552)),
	EDGE_YK_1 ("10.7.7.15", "10.7.7.15", new Border(46.348499, 43.765545, -79.162050, -79.613552)),
	EDGE_CT_1 ("134.117.57.138", "10.8.7.10", new Border(46.348499, 42.900100, -72.631295, -79.162050)),
	EDGE_WT_1 ("XXX.XXX.XXX","XX.XX.XX", new Border(46.348499, 42.900100, -79.613552, -81.425033));
	
	private final String publicIPAddress;
	private final String privateIPAddress;
	private final Border zone;
	
	CloudNode(String publicIPAddress, String privateIPAddress,
			Border zone) {
		
		this.publicIPAddress = publicIPAddress;
		this.privateIPAddress = privateIPAddress;
		this.zone = zone;
	}

	public String getPublicIPAddress() {
		return publicIPAddress;
	}

	public String getPrivateIPAddress() {
		return privateIPAddress;
	}

	public Border getZone() {
		return zone;
	}

	public boolean containsPoint(double lat, double _long) {
		return this.getZone().containsPoint(lat, _long);
	}
	
}
