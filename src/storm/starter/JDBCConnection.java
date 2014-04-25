package storm.starter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCConnection {

	public  void connection(String word,int count) throws Exception{
		// 驱动程序名          
		String driver = "com.mysql.jdbc.Driver";          
		// URL指向要访问的数据库名scutcs           
		String url = "jdbc:mysql://127.0.0.1:3306/dangshw";          
		// MySQL配置时的用户名           
		String user = "root";            
		// MySQL配置时的密码          
		String password = "q1w2e3r4";   
		Connection conn = null;
		Statement statement = null;
		try {            
			// 加载驱动程序         
			Class.forName(driver);            
			// 连续数据库          
			 conn = (Connection) DriverManager.getConnection(url, user, password);            
			if(!conn.isClosed())              
				System.out.println("Succeeded connecting to the Database!");            
			// statement用来执行SQL语句          
			 statement = (Statement) conn.createStatement();            
			// 要执行的SQL语句          
			String sql = "insert into  word_count values('"+word+"',"+count+")";            
			// 结果集         
			 statement.executeUpdate(sql);                 
//			 conn.close();       
				} catch(ClassNotFoundException e) {      
					System.out.println("Sorry,can`t find the Driver!");  
					e.printStackTrace();       
					}
			 	finally{  
					if(statement!= null) 
				    	 conn.close(); 		
				      if(conn!= null) 
				        conn.close(); 
				   }
	 } 
}
