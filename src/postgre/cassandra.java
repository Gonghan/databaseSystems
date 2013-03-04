package postgre;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class cassandra {

	private String cql;
	private Cassandra.Client client;
	private TTransport tr;
	private Set<String> set;// save all cids

	public void run() throws TException, InvalidRequestException,
			UnavailableException, UnsupportedEncodingException,
			NotFoundException, TimedOutException {

		// init
		tr = new TFramedTransport(new TSocket("127.0.0.1", 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		client = new Cassandra.Client(proto);
		tr.open();

		if (!tr.isOpen()) {
			System.out.println("failed to connect server!");
			return;
		}

		try {
			clean();
		} catch (Exception e) {

		}
		createKeyspace();
		useKeyspace();
		createTable();

	}

	public void close() {
		tr.close();
	}

	private void useKeyspace() {
		cql = "USE myKeyspace";
		try {
			execute(cql);
			System.out.println(cql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void createKeyspace() {
		cql = "CREATE KEYSPACE myKeyspace  WITH REPLICATION = {'CLASS' : 'SimpleStrategy', 'replication_factor': 1}";
		try {
			execute(cql);
			System.out.println(cql);
		} catch (Exception e) {
			System.out.println("Key space was created.");
		}
	}

	private void createTable() {
		// Create table
		cql = "CREATE TABLE myTable (key varchar,cid varchar,iid varchar,date varchar,number varchar,price varchar,primary key(cid,iid))";
		try {
			execute(cql);
			System.out.println(cql);
		} catch (Exception e) {
			System.out.println("Table was created.");
		}
	}

	public void insertRecord(String cid, String iid, String date, int number,
			int price) {
		String key = "k" + System.currentTimeMillis();
		cql = String
				.format("insert into myTable (key, cid,iid,date,number,price)values ('%s','%s','%s','%s','%d','%d');",
						key, cid, iid, date, number, price);
		System.out.println(cql);
		try {
			execute(cql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clean() {
		cql = "drop keyspace mykeyspace";
		try {
			execute(cql);
		} catch (Exception e) {
		}
	}

	private void getCids() {
		cql = "select cid from mytable";
		set = new HashSet<String>();
		CqlResult result;
		try {
			result = execute(cql);
			for (CqlRow row : result.getRows()) {
				String cid = new String(row.getColumns().get(0).getValue(),
						"UTF-8");
				set.add(cid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<String, Integer> totalAmount() {
		getCids();
		Map<String, Integer> map = new HashMap<String, Integer>();
		CqlResult result;
		try {
			for (String s : set) {
				cql = String
						.format("select price from mytable where cid='%s' allow filtering",
								s);
				result = execute(cql);
				int total = 0;
				for (CqlRow row : result.getRows()) {
					for (Column c : row.getColumns()) {
						String price = new String(c.getValue(), "UTF-8");
						total += Integer.parseInt(price);
					}
				}
				map.put(s, total);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// print map
		System.out.println("Cid\t|Total amount");
		System.out.println("-------------------");
		for (Iterator<String> it = map.keySet().iterator(); it.hasNext();) {
			String cid = it.next();
			Integer value = map.get(cid);
			System.out.printf("%s\t|%d\n", cid, value);
		}
		return map;
	}

	private void getIids() {
		cql = "select iid from mytable";
		set = new HashSet<String>();
		CqlResult result;
		try {
			result = execute(cql);
			for (CqlRow row : result.getRows()) {
				String iid = new String(row.getColumns().get(0).getValue(),
						"UTF-8");
				set.add(iid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<String, String> getItemDates() {
		Map<String, String> map = new HashMap<String, String>();
		getIids();
		CqlResult result;
		try {
			for (String s : set) {
				cql = String
						.format("select date,price from mytable where iid='%s' allow filtering",
								s);
				result = execute(cql);
				System.out.println("\nIid=" + s + "\t| Date\t\t|Price");
				System.out.println("-------------------------------------");
				for (CqlRow row : result.getRows()) {
					String date = new String(row.getColumns().get(0).getValue(), "UTF-8");
					String price = new String(row.getColumns().get(1).getValue(), "UTF-8");
					System.out.println("\t\t|" + date+"\t|"+price);
				}
			}
		} catch (Exception e) {

		}
		return map;

	}

	private CqlResult execute(String tmp_cql) throws Exception {
		return client.execute_cql3_query(toByteBuffer(tmp_cql),
				Compression.NONE, ConsistencyLevel.ALL);

	}

	/*
	 * 将String转换为bytebuffer，以便插入cassandra
	 */
	private ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

}
