package edu.colostate.cs.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.bolt.*;
import edu.colostate.cs.storm.spout.BaseSpout;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class OutlierDetectionTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new BaseSpout(), 1);

        builder.setBolt("sliding-window-bolt", new SlidingWindowBolt(), 1).globalGrouping("spout",
                Constants.Streams.POWER_GRID_DATA);
        builder.setBolt("global-median-calc-bolt", new GlobalMedianCalculatorBolt(), 1).globalGrouping("sliding-window-bolt",
                Constants.Streams.SLIDING_WINDOW_STREAM);
        builder.setBolt("per-plug-median-calc-bolt", new PlugMedianCalculatorBolt(), 3).fieldsGrouping("sliding-window-bolt",
                Constants.Streams.SLIDING_WINDOW_STREAM, new Fields(Constants.DataFields.HOUSE_ID,
                Constants.DataFields.HOUSEHOLD_ID, Constants.DataFields.PLUG_ID));

        builder.setBolt("outlier-detection-bolt", new OutlierDetectionBolt(), 1).
                allGrouping("global-median-calc-bolt", Constants.Streams.GLOBAL_MEDIAN_STREAM).
                fieldsGrouping("per-plug-median-calc-bolt", Constants.Streams.PER_PLUG_MEDIAN_STREAM,
                        new Fields(Constants.DataFields.PLUG_SPECIFIC_KEY));
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("outlier-detection-bolt",
                Constants.Streams.OUTLIER_STREAM);

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
        //conf.put(Constants.SLICE_LENGTH, Long.parseLong(args[1]));
        cluster.submitTopology("check-sliding-window", conf, builder.createTopology());
        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
        //}
    }
}
