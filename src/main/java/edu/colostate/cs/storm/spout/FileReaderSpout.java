package edu.colostate.cs.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 10/16/14
 */
public class FileReaderSpout extends BaseRichSpout {

    BufferedReader bufferedReader;
    SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File("/Users/thilina/csu/classes/581/project/data/sorted100M.csv")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String nextLine = null;
        try {
            nextLine = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(nextLine != null){
            collector.emit(new Values(nextLine));
        }
    }
}
