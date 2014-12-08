package edu.colostate.cs.storm.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 12/7/14
 */
public class S3Spout extends BaseSpout {

    private AWSCredentials credentials;
    private AmazonS3 s3Client;
    private InputStream objectData;
    private BufferedReader reader;
    private final String bucketName = "smart-grid-data";
    private final String key = "1000recs.csv";
    private S3Object object;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        super.open(map, topologyContext, spoutOutputCollector);
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
            s3Client = new AmazonS3Client(credentials);
            object = s3Client.getObject(
                    new GetObjectRequest(bucketName, key));
            objectData = object.getObjectContent();
            reader = new BufferedReader(new InputStreamReader(objectData));
        } catch (Exception e) {
            System.err.println("Error connecting to bucket." + e.getMessage());
        }
    }

    @Override
    public String getNextLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        try {
            objectData.close();
            object.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        AWSCredentials credentials = null;
        final String bucketName = "smart-grid-data";
        final String key = "1000recs.csv";
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3Client = new AmazonS3Client(credentials);
        S3Object object = s3Client.getObject(
                new GetObjectRequest(bucketName, key));
        InputStream objectData = object.getObjectContent();
        BufferedReader reader = new BufferedReader(new
                InputStreamReader(objectData));
        int counter = 0;
        while (counter < 100) {
            String line = null;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (line == null) break;

            System.out.println(line);
            counter++;
        }
        System.out.println();
        try {
            objectData.close();
            object.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
