package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.kafka.trident.GlobalPartitionInformation;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class HelloWorldBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String msg = tuple.getString(0);
        System.out.println("=====before write file=====");
        try {
            File file = new File("/tmp/storm.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(msg + "\n");
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("=====after write file=====");
        collector.emit(new Values(msg + " World"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("world"));
    }
}
