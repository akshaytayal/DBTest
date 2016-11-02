package node;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.fusesource.leveldbjni.JniDBFactory.*;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.InvalidProtocolBufferException;
import commom.Node;

import protobuf.DynamoProto.GetRequest;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.GetResult;
import protobuf.DynamoProto.MembershipDetails;
import protobuf.DynamoProto.PutRequest;
import protobuf.DynamoProto.PutResponse;
import protobuf.DynamoProto.SendGossipRequest;

import org.iq80.leveldb.*;

public class NodeImpl extends UnicastRemoteObject  implements INode{
	
	private DB db;
	private PreferenceList preferenceList;
	private int R;
	private int W;
	private int N;
	private static String myIp;
	protected NodeImpl() throws RemoteException {
		super();

		try {
			preferenceList = new PreferenceList(5);
			preferenceList.addNode("localhost", 1099, new BigInteger("000000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("100000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("200000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("300000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("400000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("500000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("600000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("700000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("800000000000000000000000000000000000000"));
			preferenceList.addNode("localhost", 1099, new BigInteger("900000000000000000000000000000000000000"));
		    Options options = new Options();
		    options.createIfMissing(true);
			db = factory.open(new File("jaffaa"), options);
			//TODO: Change R and W to configuration
			R = 3;
			W = 2;
			N=5;
			
			myIp = "localhost";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public byte[] get(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		BigInteger Key;
		String TableName = "";
		boolean IsReqFromLB;
		System.out.println("inside get ..");
		try {
			GetRequest grStream = GetRequest.parseFrom(inp);
			Key = md5(grStream.getKey());
			TableName = grStream.getTableName();
			IsReqFromLB = grStream.getIsReqFromLB();
			//String value = asString(db.get(("tam")));
		//	String value = new String(db.get(Key.getBytes()));
			//db.get(bytes("Tampa"));
			
			
			if(IsReqFromLB){
				System.out.println("Going for LB");
				byte[] res = getForLB(Key, TableName);
				return res;
			}
			else {
				System.out.println("Going for Coo");
				return getForCoordinator(Key, TableName);
			}			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private Set<String> reconcile(Set resultSet){
		return resultSet;
	}
	
	private byte[] getForLB(BigInteger Key, String TableName) throws InterruptedException{
		System.out.println("In getForLB()");
		int i;
		final AtomicInteger doneCount = new AtomicInteger(0);
		final ConcurrentMap<Integer, String> resultMap = new ConcurrentHashMap<Integer, String>();
		System.out.println("Key is : " + Key.toString());
		final List<Node> nodesList = preferenceList.getTopN(Key);
		final String KeyStr=Key.toString();
		//ConcurrentHashMap<Integer,String> resultSet = new ConcurrentHashMap<Integer,String>();
		/*ExecutorService  service = Executors.newFixedThreadPool(3);
		//ConcurrentHashMapDemo ob = new ConcurrentHashMapDemo();
		service.execute(ob.new WriteThreasOne());
		service.execute(ob.new WriteThreasTwo());
		service.execute(ob.new ReadThread());
		service.shutdownNow();*/
		System.out.println("In getForLB() 1");
		
		for(i = 0;i<5;i++){
			System.out.println("In getForLB() 2");
			final String ip = nodesList.get(i).getIp();
			System.out.println("In getForLB, i : " + i);
			Thread t = new Thread(new Runnable() {
			      public void run() {
				        // task to run goes here
				    	  
				    	  //test.load2();
				    	  INode inode;
						try {
							
							inode = (INode)Naming.lookup("rmi://" + ip + ":1099/nodeServer");
							  GetRequest.Builder grb = GetRequest.newBuilder();
							  grb.setKey(KeyStr);
								//grb.setTableName("jaffaaTable");
							  grb.setIsReqFromLB(false);
							  GetResponse gr = GetResponse.parseFrom(inode.get(grb.build().toByteArray()));
							  int currDoneCount = doneCount.incrementAndGet();
							  resultMap.put(currDoneCount, gr.getGetResultsList().get(0).getValue());
							  System.out.println("Value for Tampa1 is s:" + gr.getGetResultsList().get(0).getValue()+" from :" );
							  /*for(GetResult getRes : gr.getGetResultsList())
								System.out.println("Value for Tampa1 is s:" + getRes.getValue()+" from :" );*/
							  
						} catch (MalformedURLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (NotBoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidProtocolBufferException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
							
				    	  //System.out.println("heart beat report"+System.currentTimeMillis());
				      }
				    });
			t.start();
			t.join();
		
		
		}
		while(true){
			if(doneCount.get() >= 3){
				break;
			}
		}
		i = 0;
		Set<String> resultSet = new HashSet<String>();
		for(Map.Entry<Integer, String> entry : resultMap.entrySet()){
			if(i == R){
				break;
			}
			resultSet.add(entry.getValue());
			System.out.println( "ANswer is " + entry.getValue());
			i++;
		}
		Set<String> finalResultSet = reconcile(resultSet);
		GetResponse.Builder grpBuilder= GetResponse.newBuilder();
		GetResult.Builder gresBuilder=GetResult.newBuilder();
		for(String res: finalResultSet){
			gresBuilder.setValue(res);
			grpBuilder.addGetResults(gresBuilder);
		}
		
		return grpBuilder.build().toByteArray();
	}
	
	private byte[] getForCoordinator(BigInteger Key, String TableName){
		System.out.println("Key is :" + Key);
		//System.out.println("TableName is : " + TableName);
		
		//db.put(bytes("101"), bytes("Pradeep"));
		String value = asString(db.get(bytes(Key.toString())));
		
		//System.out.println(db.get(Key.getBytes()).toString());
		
		System.out.println("Value of Key:" + Key + " is "  + value);
		
		GetResult.Builder getResBuilder= GetResult.newBuilder();
		getResBuilder.setValue(value);
		
		GetResponse.Builder getRespBuilder = GetResponse.newBuilder();
		getRespBuilder.addGetResults(getResBuilder);
		
		return getRespBuilder.build().toByteArray();
	}

	@Override
	public byte[] put(byte[] inp) throws RemoteException {
		BigInteger Key;
		String Value;
		String TableName;
		Boolean isDelete;
		Boolean isReqFromLB;
		Boolean isReqFromMediator;
		Boolean isReqFromCoordinator;

		try {
			PutRequest PRStream= PutRequest.parseFrom(inp);
			Key= md5( PRStream.getKey());
			Value=PRStream.getValue();
			TableName = "";
			isDelete = PRStream.getIsDelete();
			isReqFromLB = PRStream.getIsReqFromLB();
			isReqFromMediator = PRStream.getIsReqFromMediator();
			isReqFromCoordinator = PRStream.getIsReqFromCoordinator();
			
			if(isReqFromLB){
				PutReqFromLB(Key, TableName, Value);
			}
			else if(isReqFromMediator){
				PutReqFromMediator(Key, TableName, Value);
			}
			else if(isReqFromCoordinator){
				PutReqFromCoordinator(Key, TableName, Value);
			}
		} catch (InvalidProtocolBufferException | NotBoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PutResponse.Builder prb = PutResponse.newBuilder();
		prb.setSuccess(true);
		return prb.build().toByteArray();
	}
	
	//End Server
	private int PutReqFromCoordinator(BigInteger Key, String TableName, String Value){
		System.out.println("Key is :" + Key);
		//System.out.println("TableName is : " + TableName);
		
		db.put(bytes(Key.toString()), bytes(Value));
		return 1;
	}
	
	
	//Coordinator
	private int PutReqFromMediator(BigInteger Key, String TableName, String Value) throws InterruptedException{
		System.out.println("CO ordinator serving the request");
		int i;
		final AtomicInteger doneCount = new AtomicInteger(0);
		final List<Node> nodesList = preferenceList.getTopN(Key);
		final String KeyStr= Key.toString();
		final String ValStr=Value;

		System.out.println("pos 1");
		
		for(i = 0;i<N;i++){
			final String ip = nodesList.get(i).getIp();
			Thread t = new Thread(new Runnable() {
			      public void run() {
				        // task to run goes here
				    	  //test.load2();
				    	  INode inode;
						try {
							
							  inode = (INode)Naming.lookup("rmi://" + ip + ":1099/nodeServer");
							  PutRequest.Builder prb = PutRequest.newBuilder();
							  prb.setKey(KeyStr);
							  prb.setValue(ValStr);
							  prb.setIsReqFromLB(false);
							  prb.setIsReqFromMediator(false);
							  prb.setIsReqFromCoordinator(true);

							  PutResponse pr = PutResponse.parseFrom(inode.put(prb.build().toByteArray()));
							  if(pr.getSuccess()){
								  doneCount.incrementAndGet();
							  }
							  //resultMap.put(currDoneCount, gr.getGetResultsList().get(0).getValue());
							  System.out.println("Put is done" );
							  /*for(GetResult getRes : gr.getGetResultsList())
								System.out.println("Value for Tampa1 is s:" + getRes.getValue()+" from :" );*/
							  
						} catch (MalformedURLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (NotBoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidProtocolBufferException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
							
				    	  //System.out.println("heart beat report"+System.currentTimeMillis());
				      }
				    });
			t.start();
			t.join();
		
		
		}
		while(true){
			if(doneCount.get() >= W){
				break;
			}
		}
		
		return 1;
	}
	
	//Mediator
	private int PutReqFromLB(BigInteger Key, String TableName, String Value) throws RemoteException, NotBoundException{
		int result = 0;
		System.out.println("Key is :" + Key);
		Node topNode = preferenceList.getTopNode(Key);
		//System.out.println("TableName is : " + TableName);
		
		if(topNode.getIp().equalsIgnoreCase(myIp)){
			result = PutReqFromCoordinator(Key, TableName, Value);
		}
		else {
			  INode inode;
			  try {
				  inode = (INode)Naming.lookup("rmi://" + topNode.getIp() + ":" + topNode.getPort() + "\nodeServer");
				  PutRequest.Builder prb = PutRequest.newBuilder();
				  prb.setKey(Key.toString());
				  prb.setValue(Value);
				  prb.setIsReqFromCoordinator(false);
				  prb.setIsReqFromMediator(true);
				  prb.setIsReqFromLB(false);
				  
				  PutResponse pr = PutResponse.parseFrom(inode.put(prb.build().toByteArray()));
				  if(pr.getSuccess()){
					 result = 1; 
				  }
				  else{
					  result = 0;
				  }
				  //resultMap.put(currDoneCount, gr.getGetResultsList().get(0).getValue());
				  System.out.println("Put is done" );
				  
			} catch (MalformedURLException | InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		//System.out.println(db.get(Key.getBytes()).toString());
		
		//System.out.println("Value of Key:" + Key + " is "  + value);
		
		
		
		return result;
	}


	
	public static void main(String args[]){
		NodeImpl node;
		try {
			node = new NodeImpl();
			Naming.rebind("rmi://localhost:1099/nodeServer",node);
			System.out.println("Node Server Started...");
						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void sendGossip(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Gossipping Started");
		Runnable runnable = new Runnable() {
		      public void run() {
		        // task to run goes here
		    	  doSendGossip();
		    	  //System.out.println("heart beat report"+System.currentTimeMillis());
		      }
		    };
		    
		    ScheduledExecutorService service = Executors
		                    .newSingleThreadScheduledExecutor();
		    service.scheduleAtFixedRate(runnable, 0, 10, TimeUnit.SECONDS);
		    
		    }
    
	private void doSendGossip(){
		INode node;
		SendGossipRequest.Builder sgr = SendGossipRequest.newBuilder();
		MembershipDetails.Builder mbr = MembershipDetails.newBuilder();
		mbr.setNodeId(1);
		
		sgr.setMemDetails(1, mbr);
		
		String RandomIp="10.0.0.3";
		int PortNum=9999;
		
		try {
			node = (INode)Naming.lookup("rmi://"+RandomIp+":"+PortNum+"/nodeServer");
			node.sendGossip(sgr.build().toByteArray());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private BigInteger md5(String input) {
		
		//String md5 = null;
		BigInteger b1 = null;
		//TODO: Exit the code if input is null
		if(null == input) return null;
		
		try {
			
		//Create MessageDigest object for MD5
		MessageDigest digest = MessageDigest.getInstance("MD5");
		
		//Update input string in message digest
		digest.update(input.getBytes(), 0, input.length());

		//Converts message digest value in base 16 (hex) 
		
		b1 = new BigInteger(1, digest.digest());
		System.out.println(b1);


		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		}
		return b1;
	}

}
