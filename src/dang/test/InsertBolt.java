package dang.test;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class InsertBolt extends BaseBasicBolt {

	private Connection conn = null;

	private PreparedStatement preStmt = null;

	private String strar[] = null;

	public static Log LOG = LogFactory.getLog(TestWordSpout.class);

	OutputCollector _collector;

	private int commitflag = 0;



	@Override

	public void cleanup() {

		try {

			if (preStmt != null) {

				preStmt.close();

				preStmt = null;

			}

		} catch (SQLException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}

		try {

			if (conn != null) {

				conn.close();

				conn = null;

			}

		} catch (SQLException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}

	}

	@Override

	public void prepare(Map stormConf, TopologyContext context) {

		//_collector = collector;

		getcn(2);

		getstmt();

	}



	@Override

	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try{

		String str = tuple.getString(0);

		LOG.info("--------------str----------" + str);

        str=str.replace("=", " ");
        str=str.replace("->", " ");
        
        String[] str_Arr=str.split(" ");
        for(int i=0;i<str_Arr.length;i++){
        	insertData(str_Arr[i]);
        }

		collector.emit(new Values(str));

		}catch (Exception e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}

	}



	@Override

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("word"));

	}



	private void getcn(int type) {

		if (type == 0) {

			try {

				Class.forName("com.timesten.jdbc.TimesTenDriver");

				conn = DriverManager.getConnection("jdbc:timesten:client:tt1");

				// conn.setAutoCommit(false);

			} catch (Exception e) {

				// TODO Auto-generated catch block

				e.printStackTrace();

			}

		}

		if (type == 1) {

			try {

				Class.forName("org.postgresql.Driver");

				conn = DriverManager.getConnection(

						"jdbc:postgresql://192.168.242.11:5432/test",

						"root", "q1w2e3");

				conn.setAutoCommit(false);

			} catch (Exception e) {

				// TODO Auto-generated catch block

				e.printStackTrace();

			}



		}

		if (type == 2) {

			try {

				Class.forName("com.mysql.jdbc.Driver");

				conn = DriverManager.getConnection(

						"jdbc:mysql://127.0.0.1:3306/dangshw",

						"root", "q1w2e3r4");

				conn.setAutoCommit(false);	

			} catch (Exception e) {

				// TODO Auto-generated catch block

				e.printStackTrace();

			}



		}

	

	}



	private void getstmt() {

		String INSERTINTO_TAKING_STATUS = "INSERT INTO wordcount (word,count) VALUES ( ? ,?)";

		try {

			preStmt = conn.prepareStatement(INSERTINTO_TAKING_STATUS);



		} catch (Exception e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}



	}



	private void insertData(String str) {

		try {

			LOG.info("----------------------"+str);

			strar = str.split(",");

			preStmt.setString(1, strar[0].trim());

			preStmt.setInt(2, 1);

			// System.out.print(strar[0] + i);

			// preStmt.setInt(2,

			// ("".equals(strar[1])?Integer.parseInt(strar[1]):0));



			// preStmt.setInt(3,

			// ("".equals(strar[2])?Integer.parseInt(strar[2]):0));

			// preStmt.setString(4, ("".equals(strar[3])?strar[3]:"0"));

			// preStmt.setString(5, strar[0]);

			// preStmt.setDouble(6, Double.parseDouble(strar[5]));

			// preStmt.setString(7, strar[6]);

			// preStmt.setString(8, strar[7]);

			// preStmt.setString(9, strar[8]);

			// preStmt.setString(10, strar[9]);

			// preStmt.setString(11, strar[10]);

			// preStmt.setString(12, strar[11]);

			// preStmt.setString(13, strar[12]);



			// /preStmt.setString(1,str);

			preStmt.executeUpdate();

			conn.commit();

		} catch (Exception e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		}



	}



	

}