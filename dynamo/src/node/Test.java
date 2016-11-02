package node;
import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Test {
	private DB db1;
	
	
	public Test() throws IOException{
	    Options options = new Options();
	    options.createIfMissing(true);
		db1 = factory.open(new File("jaffaa"), options);
		//factory.destroy(new File("jaffaa"), options);
		//db1 = factory.open(new File("jaffaa"), options);
	}
	
	public void closeConn() throws IOException{
		db1.close();
		//db1.close();
	}
	
   
	
	public static void main(String args[]) throws IOException{
		
		final Test test = new Test();
		
		Thread t1 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
			    	  test.load1();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });
		Thread t2 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
			    	  test.load2();
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
	               db1.put(bytes("Tampa9"), bytes("Akshay"));

	               String value = asString(db1.get(bytes("Tampa9")));
	               //db.delete(bytes("Tampa"));
	               System.out.println(" value of Tampa9 is "+ value);
       
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public void load2(){
		
		 try {
	         // Use the db in here....
			       System.out.println("Tampa11");
	               db1.put(bytes("Tampa11"), bytes("rocks"));
	               System.out.println("Tampa12");
	               db1.put(bytes("Tampa12"), bytes("popo"));
	               System.out.println("Tampa13");
	               db1.put(bytes("Tampa13"), bytes("rocks"));
	               System.out.println("Tampa14");
	               db1.put(bytes("Tampa14"), bytes("popo"));
	               System.out.println("Tampa15");
	               db1.put(bytes("Tampa15"), bytes("rocks"));
	               db1.put(bytes("Tampa16"), bytes("popo"));
	               db1.put(bytes("Tampa17"), bytes("rocks"));
	               db1.put(bytes("Tampa18"), bytes("popo"));
	               db1.put(bytes("Tampa19"), bytes("rocks"));

	               //String value = asString(db.get(bytes("Tampa")));
	               //db.delete(bytes("Tampa"));
	               //System.out.println(" value is "+ value);
       
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
		
	}
}
