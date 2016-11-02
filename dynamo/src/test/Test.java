package test;
import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Test {
	private DB db1;
	//private DB db2;
	private DB db3;
	
	public Test() throws IOException{
	    Options options = new Options();
	    options.createIfMissing(true);
		db1 = factory.open(new File("myDB1"), options);
		//db3 = factory.open(new File("newjaffaa"), options);
		//factory.repair(new File("c_trydb"), options);
		//db2 = factory.open(new File("jaffaa"), options);
	}
	
	public void closeConn() throws IOException{
		//db1.close();
		//db2.close();
		//db3.close();
	}
	
   
	
	public static void main(String args[]) throws IOException{
		
		final Test test = new Test();
		
		Thread t1 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  System.out.println("Kuch to hua ha");
			    	  test.load4();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });
		/*Thread t2 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
			    	  test.load2();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });*/
		t1.start();
		//t2.start();
		try {
			t1.join();
			//t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	     test.closeConn();

	}
	
	public void load1(){
		 try {
	         // Use the db in here....
			 		System.out.println("Tampa1");
	               db1.put(bytes("Tampa1"), bytes("rocks"));
	               System.out.println("Tampa2");
	               db1.put(bytes("Tampa2"), bytes("popo"));
	               System.out.println("Tampa3");
	               db1.put(bytes("Tampa3"), bytes("rocks"));
	               System.out.println("Tampa4");
	               db1.put(bytes("Tampa4"), bytes("popo"));
	               System.out.println("Tampa5");
	               db1.put(bytes("Tampa5"), bytes("rocks"));
	               db1.put(bytes("Tampa6"), bytes("popo"));
	               db1.put(bytes("Tampa7"), bytes("rocks"));
	               db1.put(bytes("Tampa8"), bytes("popo"));
	               db1.put(bytes("Tampa9"), bytes("rocks"));
	               
	                             

	               String value = asString(db1.get(bytes("Tampa")));
	               //db.delete(bytes("Tampa"));
	               //System.out.println(" value is "+ value);
       
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public void load2(){
		
		 try {
	         // Use the db in here....
			/*       System.out.println("Tampa11");
	               db2.put(bytes("Tampa11"), bytes("rocks"));
	               System.out.println("Tampa12");
	               db2.put(bytes("Tampa12"), bytes("popo"));
	               System.out.println("Tampa13");
	               db2.put(bytes("Tampa13"), bytes("rocks"));
	               System.out.println("Tampa14");
	               db2.put(bytes("Tampa14"), bytes("popo"));
	               System.out.println("Tampa15");
	               db2.put(bytes("Tampa15"), bytes("rocks"));
	               db2.put(bytes("Tampa16"), bytes("popo"));
	               db2.put(bytes("Tampa17"), bytes("rocks"));
	               db2.put(bytes("Tampa18"), bytes("popo"));
	               db2.put(bytes("Tampa19"), bytes("rocks"));*/

	               //String value = asString(db.get(bytes("Tampa")));
	               //db.delete(bytes("Tampa"));
	               //System.out.println(" value is "+ value);
      
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
		
	}
	
	
	public void load3(){
		 try {
	         // Use the db in here....
			 Integer i;
			 
			 for(i=5000;i>=2000;i--){
				 if(i%50000 == 0)
				   System.out.println("Thread 1 Loading " + i);
				 String str = i + " Chinna";
	               db1.put(bytes(i.toString()), bytes(str));
			 }
			
     
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public void load4(){
		 try {
	         // Use the db in here....
			 String value = asString(db3.get(bytes(md5("Hello10"))));
			 System.out.println("VALUE IS: " + value);
			 
			 //db1.put(bytes("1"), bytes("Akshay1"));
			 
			 System.out.println("get Property output : " + db1.getProperty("leveldb.sstables"));
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public static String md5(String input) {
		
		String md5 = null;
		
		if(null == input) return null;
		
		try {
			
		//Create MessageDigest object for MD5
		MessageDigest digest = MessageDigest.getInstance("MD5");
		
		//Update input string in message digest
		digest.update(input.getBytes(), 0, input.length());

		//Converts message digest value in base 16 (hex) 
		System.out.println("Ho Gya");
		BigInteger b1 = new BigInteger(1, digest.digest());
		System.out.println(b1);
		System.out.println(b1.bitCount());	
		System.out.println("Ho Gya");
		md5 = new BigInteger(1, digest.digest()).toString(16);

		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		}
		return md5;
	}
	

	public void load5(){
		 try {
	         // Use the db in here....
			 		//System.out.println("Tampa1");
	               db3.put(bytes("PradeepAnumala"), bytes("rocks"));


	               String value = asString(db3.get(bytes("PradeepAnumala")));
	               //db.delete(bytes("Tampa"));
	               System.out.println(" value is "+ value);
      
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}

	
	
	
	
	
}
