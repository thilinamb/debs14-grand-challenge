package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import edu.colostate.cs.storm.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Thilina
 * Date: 11/21/14
 */
public class ReportBolt extends BaseBasicBolt {

    private List<Double> latency = new ArrayList<Double>();
    private List<Double> throughput = new ArrayList<Double>();
    private long startTime = 0;
    private int startTupleCount = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceStreamId().equals(Constants.Streams.PERF_PUNCTUATION_STREAM)) {
            System.out.println("Report-Bolt");
            if (startTime == 0) {    // very first message
                startTime = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
                startTupleCount = tuple.getIntegerByField(Constants.DataFields.TUPLE_COUNT);
            } else {
                // calculate the latency
                long currentTime = System.nanoTime();
                long emitTime = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
                // latency in milliseconds
                latency.add((currentTime - emitTime) / Math.pow(10, 6));
                // calculate throughput
                int currentTupleCount = tuple.getIntegerByField(Constants.DataFields.TUPLE_COUNT);
                double throughPutInNano = (currentTupleCount - startTupleCount) / ((emitTime - startTime) * 1.0);
                // requests per second
                throughput.add(throughPutInNano * Math.pow(10, 9));
                System.out.println((emitTime - startTime));
                startTime = emitTime;
                startTupleCount = currentTupleCount;
            }
        } else {
            // System.out.println(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
        BufferedWriter bfw = null;
        try {
            bfw = new BufferedWriter(new FileWriter(System.getProperty("user.home") + File.separator +
                    "storm-statistics.csv"));
            for (int i = 0; i < latency.size(); i++) {
                bfw.write(latency.get(i) + "," + throughput.get(i));
                bfw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bfw != null) {
                    bfw.flush();
                    bfw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        super.cleanup();
    }
}
