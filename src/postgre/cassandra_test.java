package postgre;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class cassandra_test {
	public static void main(String[] args) throws TException,
			InvalidRequestException, UnavailableException,
			UnsupportedEncodingException, NotFoundException, TimedOutException {
		// 包装好的socket
		TTransport tr = new TFramedTransport(new TSocket("127.0.0.1", 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();

		if (!tr.isOpen()) {
			System.out.println("failed to connect server!");
			return;
		}

		long temp = System.currentTimeMillis();

		client.set_keyspace("demo");// 使用DEMO keyspace
		ColumnParent parent = new ColumnParent("student");// column family

		/*
		 * 这里我们插入100万条数据到Student内 每条数据包括id和name
		 */
		/*
		 * String key_user_id="a"; for (int i = 0; i < 10; i++) { String k =
		 * key_user_id + i;// key long timestamp = System.currentTimeMillis();//
		 * 时间戳
		 * 
		 * Column idColumn = new Column(toByteBuffer("id"));// column name
		 * idColumn.setValue(toByteBuffer(i + ""));// column value
		 * idColumn.setTimestamp(timestamp); client.insert(toByteBuffer(k),
		 * parent, idColumn, ConsistencyLevel.ONE);
		 * 
		 * Column nameColumn = new Column(toByteBuffer("name"));
		 * nameColumn.setValue(toByteBuffer("student" + i));
		 * nameColumn.setTimestamp(timestamp); client.insert(toByteBuffer(k),
		 * parent, nameColumn, ConsistencyLevel.ONE); }
		 */
		
		tr.close();
		//
		// /*
		// * 这里我们插入100万条数据到Student内 每条数据包括id和name
		// */
		// String key_user_id = "a";
		// for (int i = 0; i < 100; i++) {
		// String k = key_user_id + i;// key
		// long timestamp = System.currentTimeMillis();// 时间戳
		//
		// Column idColumn = new Column(toByteBuffer("id"));// column name
		// idColumn.setValue(toByteBuffer(i + ""));// column value
		// idColumn.setTimestamp(timestamp);
		// client.insert(toByteBuffer(k), parent, idColumn,
		// ConsistencyLevel.ONE);
		//
		// Column nameColumn = new Column(toByteBuffer("name"));
		// nameColumn.setValue(toByteBuffer("student" + i));
		// nameColumn.setTimestamp(timestamp);
		// client.insert(toByteBuffer(k), parent, nameColumn,
		// ConsistencyLevel.ONE);
		// }
		//
		// /*
		// * 读取某条数据的单个字段
		// */
		// ColumnPath path = new ColumnPath("Student");// 设置读取Student的数据
		// path.setColumn(toByteBuffer("id")); // 读取id
		// String key3 = "a1";// 读取key为a1的那条记录
		// System.out.println(toString(client.get(toByteBuffer(key3), path,
		// ConsistencyLevel.ONE).column.value));
		//
		// /*
		// * 读取整条数据
		// */
		// SlicePredicate predicate = new SlicePredicate();
		// SliceRange sliceRange = new SliceRange(toByteBuffer(""),
		// toByteBuffer(""), false, 10);
		// predicate.setSlice_range(sliceRange);
		// List<ColumnOrSuperColumn> results = client.get_slice(
		// toByteBuffer(key3), parent, predicate, ConsistencyLevel.ONE);
		//
		// for (ColumnOrSuperColumn result : results) {
		// Column column = result.column;
		// System.out.println(toString(column.name) + " -> "
		// + toString(column.value));
		// }
		//
		// long temp2 = System.currentTimeMillis();
		// System.out.println("time: " + (temp2 - temp) + " ms");// 输出耗费时间
		//
		// tr.close();
	}

	/*
	 * 将String转换为bytebuffer，以便插入cassandra
	 */
	public static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	/*
	 * 将bytebuffer转换为String
	 */
	public static String toString(ByteBuffer buffer)
			throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}
}