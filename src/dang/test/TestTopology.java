package dang.test;



import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;



import backtype.storm.Config;

import backtype.storm.LocalCluster;

import backtype.storm.testing.TestWordSpout;

import backtype.storm.topology.TopologyBuilder;



public class TestTopology {

	public static Log LOG = LogFactory.getLog(TestWordSpout.class);

	public static void main(String[] args) throws Exception {

		ReadWordSpout genderSpout = new ReadWordSpout("c:/test");

		

		TopologyBuilder builder = new TopologyBuilder();

		

		builder.setSpout("read", genderSpout,1);

		builder.setBolt("Insert", new InsertBolt(),3).globalGrouping("read");

		Config conf = new Config();

		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();

		LOG.info("----TestTopology start----");

		cluster.submitTopology("join-example", conf, builder.createTopology());

		LOG.info("----TestTopology end----");

		//Utils.sleep(2000);

		//cluster.shutdown();

	}

}

