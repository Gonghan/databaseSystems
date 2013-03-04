package postgre;

import java.io.UnsupportedEncodingException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

public class Main {

	private cassandra cass;
	private postgre post;
	
	public Main(){
		cass=new cassandra();
		post=new postgre();
	}
	public void run(){
		try {
			cass.run();
			post.run();
			insertRecord("c001","i001","2013-02-20",2,10,"Name1","Description1","Market1");
			insertRecord("c002","i001","2013-02-21",3,18,"Name2","Description1","Market1");
			insertRecord("c001","i002","2013-02-22",1,16,"Name1","Description2","Market2");
			insertRecord("c003","i003","2013-02-19",4,20,"Name3","Description3","Market3");
			insertRecord("c003","i002","2013-02-11",5,90,"Name3","Description2","Market2");
			insertRecord("c001","i003","2013-02-21",1,999,"Name1","Description3","Market3");
			cass.totalAmount();
			cass.getItemDates();
			cass.close();
		} catch (UnsupportedEncodingException | TException
				| InvalidRequestException | UnavailableException
				| NotFoundException | TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private void insertRecord(String cid,String iid,String date,int number,int price,String cname,String description, String market){
		cass.insertRecord(cid, iid, date, number, price);
		post.update(iid, cid, price, description, market, cname);
		
	}
	
	public static void main(String[] args) {
		new Main().run();
	}

}