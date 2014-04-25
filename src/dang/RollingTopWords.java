package dang;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
 
public class RollingTopWords {
    public static void main(String[] args) throws Exception {
 
        final int TOP_N = 3;
 
        TopologyBuilder builder = new TopologyBuilder();
 
        builder.setSpout("spout", new TestWordSpout(), 5);
        
 
        builder.setBolt("bolt1", new RollingCountObjects(60, 10), 4).fieldsGrouping("spout", new Fields("word"));//字段
        builder.setBolt("bolt2", new RankObjects(TOP_N), 4).fieldsGrouping("bolt1", new Fields("obj")); //根据字段
        builder.setBolt("bolt3", new MergeObjects(TOP_N)).globalGrouping("bolt2");  //全部数据都整理到这里
        
        Config conf = new Config();
        conf.setDebug(true);
 
        LocalCluster cluster = new LocalCluster();  
        cluster.submitTopology("rolling-demo", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
