package node;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INode extends Remote {

		/* getResponse get(getRequest)) */
		/* Method to get data  */
		byte[] get(byte[] inp) throws RemoteException;
		
		/* putResponse put(putRequest) */
		/* Method to write data */
		byte[] put(byte[] inp) throws RemoteException;	
		
		/* sendGossip(sendGossipRequest) */
		/* Method to send gossip to other nodes */
		void sendGossip (byte[] inp) throws RemoteException;
	

}
