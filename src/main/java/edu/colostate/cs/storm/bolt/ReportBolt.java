package edu.colostate.cs.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import edu.colostate.cs.storm.Constants;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 11/21/14
 */
public class ReportBolt extends BaseBasicBolt {

    private List<Double> latency = new ArrayList<Double>();
    private List<Double> throughput = new ArrayList<Double>();
    private long startTime = 0;
    private int startTupleCount = 0;
    private String s3Bucket;
    private String s3Outfile;
    private static final Logger LOG = Logger.getLogger(ReportBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceStreamId().equals(Constants.Streams.PERF_PUNCTUATION_STREAM)) {
            LOG.info("Report-Bolt");
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
                LOG.info((emitTime - startTime));
                startTime = emitTime;
                startTupleCount = currentTupleCount;
                cleanup();
            }
        } else {
            // System.out.println(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        if (stormConf.containsKey(Constants.S3_BUCKET_NAME)) {
            s3Bucket = (String) stormConf.get(Constants.S3_BUCKET_NAME);
        }
        if (stormConf.containsKey(Constants.S3_OUTPUT_KEY)) {
            s3Outfile = (String) stormConf.get(Constants.S3_OUTPUT_KEY);
        }
    }

    @Override
    public void cleanup() {
        BufferedWriter bfw = null;
        String outFileName = System.getProperty("user.home") + File.separator +
                "storm-statistics.csv";
        try {
            bfw = new BufferedWriter(new FileWriter(outFileName));
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
        if (s3Bucket != null && s3Outfile != null) {
            // upload the outfile to S3
            AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
            try {
                System.out.println("Uploading a new object to S3 from a file\n");
                File file = new File(outFileName);
                s3client.putObject(new PutObjectRequest(
                        s3Bucket, s3Outfile, file));

            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which " +
                        "means your request made it " +
                        "to Amazon S3, but was rejected with an error response" +
                        " for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which " +
                        "means the client encountered " +
                        "an internal error while trying to " +
                        "communicate with S3, " +
                        "such as not being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
            }
        }
        super.cleanup();
    }
}
