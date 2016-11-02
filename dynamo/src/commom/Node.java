package commom;

import java.math.BigInteger;

public class Node {
	private String ip;
	private int port;
	private BigInteger hash;
	
	public Node(String ip, int port, BigInteger hash){
		this.ip = ip;
		this.port = port;
		this.hash = hash;
	}
	
	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public BigInteger getHash() {
		return hash;
	}

}
