package storm.kafka.filereader;
import storm.kafka.filereader.WordCounterBolt;
import storm.kafka.filereader.WordSpitterBolt;
import storm.kafka.filereader.LineReaderSpout;
import storm.kafka.HelloWorldBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class FileReaderTopology {
	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("word-spitter", new WordSpitterBolt()).shuffleGrouping("line-reader-spout");
//		builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spitter");
//		builder.setBolt("word-print", new HelloWorldBolt()).shuffleGrouping("word-counter");
               builder.setBolt("word-print", new HelloWorldBolt()).shuffleGrouping("word-spitter");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
		Thread.sleep(10000);
//below would shutdown after read, comment to keep it realtime/cont.
//		cluster.shutdown();
	}

}
