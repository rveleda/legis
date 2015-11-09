package ca.yorku.asrl.legis.gateway;

public class Border {
	
	private double N;
	private double S;
	private double E;
	private double W;

	public Border(double n, double s, double e, double w) {
		N = n;
		S = s;
		E = e;
		W = w;
	}

	public double getN() {
		return N;
	}

	public void setN(double n) {
		N = n;
	}

	public double getS() {
		return S;
	}

	public void setS(double s) {
		S = s;
	}

	public double getE() {
		return E;
	}

	public void setE(double e) {
		E = e;
	}

	public double getW() {
		return W;
	}

	public void setW(double w) {
		W = w;
	}
	
	public String outOfBoundsDirection(double lat, double _long) {
		if(lat > N) {
			return "north";
		}
		else if(lat < S) {
			return "south";
		}
		else if(_long > E) {
			return "east";
		}
		else if(_long < W) {
			return "west";
		}
		else {
			return "inbounds";
		}
	}

	public boolean containsPoint(double lat, double _long) {
		//lat = Math.abs(lat);
		//_long = Math.abs(_long);
		return lat <= N && lat >= S && _long <= E && _long >= W;
		//return lat <= Math.abs(N) && lat >= Math.abs(S) && _long >= Math.abs(S) && _long <= Math.abs(N);
	}
	

}
