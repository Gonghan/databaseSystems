package postgre;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class postgre {
	private String sql;
	private Connection postgreConnection;
	private Statement stmt;
	

	public void run() {
		try {

			// first, create the connection of postgre
			Class.forName("org.postgresql.Driver");
			postgreConnection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/Assignment4", "postgres",
					"123456");
			stmt = postgreConnection.createStatement();
			clean();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
		if (postgreConnection == null) {
			System.out.println("Failed to make postgreConnection!");
		}
	}

	// update the items table when a new item request comes
	public void update(String iid, String cid, int price, String description,
			String market,String customersName) {
		//update item
		try {
			sql=String.format("insert into items values('%s',%d,'%s','%s')",iid,price,description,market );
			execute(sql);
		} catch (SQLException e) {
			sql=String.format("update items set price='%d', description='%s', market='%s' where iid='%s'", price,description,market,iid);
			try {
				execute(sql);
			} catch (SQLException e1) {
			}
		}
		//insert into customers values('c011','name11','i001');
		//update customers
		
		try {
			sql=String.format("insert into customers values ('%s','%s','%s')",cid,customersName,iid);
			execute(sql);
		} catch (SQLException e) {
			sql=String.format("update customers set iid='%s' where cid='%s'", iid,cid);
			try {
				execute(sql);
			} catch (SQLException e1) {
			}
		}
		
	}

	private void execute(String sql) throws SQLException {
		stmt.execute(sql);
	}

	public void close() {
		try {
			this.postgreConnection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void clean() throws SQLException{
		sql="delete from customers";
		execute(sql);
		sql="delete from items";
		execute(sql);
		
	}
}
