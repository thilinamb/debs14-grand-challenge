package edu.colostate.cs.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.bolt.HouseLoadPredictorBolt;
import edu.colostate.cs.storm.bolt.ReportBolt;
import edu.colostate.cs.storm.spout.BaseSpout;

/**
 * Author: Thilina
 * Date: 10/16/14
 */
public class BasicTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new BaseSpout(), 1);

        builder.setBolt("predict", new HouseLoadPredictorBolt(), 1).fieldsGrouping("spout",
                new Fields(Constants.InputTupleFields.HOUSE_ID));
        builder.setBolt("report", new ReportBolt(), 1).globalGrouping("predict");

        Config conf = new Config();
        //conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("record-count", conf, builder.createTopology());

            try {
                Thread.sleep(60*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            cluster.shutdown();
        }
    }
}

