package node;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import commom.Node;

public class PreferenceList {
	private List<Node> nodesList;
	private int N;
	
	public PreferenceList(int N){
		this.N = N;
		this.nodesList = new ArrayList<Node>();
	}
	public int addNode(String ip, int port, BigInteger hash){
		try{
			int i;
			Node newNode = new Node(ip, port, hash);
			//nodesList.add(newNode);
			i = 0;
			if(nodesList != null){
				while(i < nodesList.size()){
					if(hash.compareTo( nodesList.get(i).getHash()) < 0){
						break;
					}
					i++;
				}
				nodesList.add(i, newNode);
			}
			/*else
			{
				nodesList.add(0, newNode);
			}*/
			System.out.println("Size of pref list:" + nodesList.size());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
	/*Return the top N nodes in the list for the given key*/
	@SuppressWarnings("null")
	public List<Node> getTopN(BigInteger key){
		List<Node> topNList = new ArrayList<Node>();
		try{
			int i;
			System.out.println("Key is : " + key);
			i = 0;
			while(i < nodesList.size()){
				if(key.compareTo( nodesList.get(i).getHash()) < 0){
					break;
				}
				i++;
			}
			i = i-1;
			int j =0;
			
			while(j < N && i<nodesList.size()){
				topNList.add(nodesList.get(i));
				j++;
				i++;
			}
			
			if(j<N){
				i = 0;
				while(j < N && i<nodesList.size()){
					topNList.add(nodesList.get(i));
					j++;
					i++;
				}
			}
	
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return topNList;
	}
	public Node getTopNode(BigInteger key){
		
		return nodesList.get(0);
	}	
	
}
