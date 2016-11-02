package client;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

import node.INode;

import protobuf.DynamoProto.GetRequest;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.GetResult;
import protobuf.DynamoProto.PutRequest;
import protobuf.DynamoProto.PutResponse;
 

public class Client {
	public static void main(String r[]){
	
		try {
			String Key = "23994";
			String Value = "The Lord2";
			
			PutRequest.Builder prb= PutRequest.newBuilder();
			prb.setKey(Key);
			prb.setValue(Value);
			prb.setIsReqFromCoordinator(false);
			prb.setIsReqFromLB(true);
			prb.setIsReqFromMediator(false);
			
			
			INode inode = (INode)Naming.lookup("rmi://localhost:1099/nodeServer");
			
			PutResponse prbResp= PutResponse.parseFrom(inode.put(prb.build().toByteArray()));
			
			GetRequest.Builder grb = GetRequest.newBuilder();
			//BigInteger keyInput = new BigInteger("101");

			grb.setKey(Key);
			grb.setTableName("jaffaaTable");
			grb.setIsReqFromLB(true);
			
		
			GetResponse gr = GetResponse.parseFrom(inode.get(grb.build().toByteArray()));
			List<GetResult> getResList = gr.getGetResultsList();
			for(GetResult res : getResList){
			  System.out.println("Value for "+Key +" is " + res.getValue());
			}
			
			
		} catch (MalformedURLException | RemoteException | NotBoundException | InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();	
		}
		/*final Client client = new Client();
		Thread t1 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
			    	  client.test1();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });
		Thread t2 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
		    	      client.test2();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });
		t1.start();
		t2.start();
		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	
	private void test1(){

		try {
			INode inode = (INode)Naming.lookup("rmi://localhost:1099/nodeServer");
			GetRequest.Builder grb = GetRequest.newBuilder();
			grb.setKey("Tampa10");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa11");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa12");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa13");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa14");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa15");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa16");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa17");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa18");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa19");
			inode.get(grb.build().toByteArray());
			
			//grb.setTableName("jaffaaTable");
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void test2(){

		try {
			INode inode = (INode)Naming.lookup("rmi://localhost:1099/nodeServer");
			GetRequest.Builder grb = GetRequest.newBuilder();
			grb.setKey("Tampa20");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa21");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa22");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa23");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa24");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa25");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa26");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa27");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa28");
			inode.get(grb.build().toByteArray());
			grb.setKey("Tampa29");
			inode.get(grb.build().toByteArray());
			
			//grb.setTableName("jaffaaTable");
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

