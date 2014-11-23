package edu.colostate.cs.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.bolt.HouseLoadPredictorBolt;
import edu.colostate.cs.storm.bolt.PlugLoadPredictorBolt;
import edu.colostate.cs.storm.bolt.ReportBolt;
import edu.colostate.cs.storm.spout.BaseSpout;

/**
 * Author: Thilina
 * Date: 10/16/14
 */
public class LoadPredictionTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new BaseSpout(), 1);
        builder.setBolt("predict-house", new HouseLoadPredictorBolt(), 2).fieldsGrouping("spout",
                Constants.Streams.POWER_GRID_DATA,
                new Fields(Constants.DataFields.HOUSE_ID)).globalGrouping("spout",
                Constants.Streams.CUSTOM_TICK_TUPLE);
        builder.setBolt("predict-plug", new PlugLoadPredictorBolt(), 2).fieldsGrouping("spout",
                Constants.Streams.POWER_GRID_DATA,
                new Fields(Constants.DataFields.HOUSE_ID)).globalGrouping("spout",
                Constants.Streams.CUSTOM_TICK_TUPLE);
        builder.setBolt("report", new ReportBolt(), 1).globalGrouping("predict-house").globalGrouping("predict-plug");

        Config conf = new Config();
        //conf.setDebug(true);

        /*if (args != null && args.length > 1) {
            conf.setNumWorkers(3);
            conf.put(Constants.SLICE_LENGTH, Long.parseLong(args[1]));
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else { */
        conf.setMaxTaskParallelism(5);
        LocalCluster cluster = new LocalCluster();
        conf.put(Constants.SLICE_LENGTH, Long.parseLong(args[1]));
        cluster.submitTopology(args[0], conf, builder.createTopology());
        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
        //}
    }
}

