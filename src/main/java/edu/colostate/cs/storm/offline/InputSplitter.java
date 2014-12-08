package edu.colostate.cs.storm.offline;

import java.io.*;

/**
 * Process the input and split it to contain a certain number of houses
 * Author: Thilina
 * Date: 12/7/14
 */
public class InputSplitter {
    public static void main(String[] args) {
        BufferedReader bufferedReader = null;
        BufferedWriter writer10 = null;
        BufferedWriter writer20 = null;
        try {
            bufferedReader = new BufferedReader(new FileReader
                    (new File("/Users/thilina/csu/classes/581/project/data/sorted100M.csv")));
            writer10 = new BufferedWriter(new FileWriter(
                    new File("/Users/thilina/csu/classes/581/project/data/10houses_set2.csv")));
            writer20 = new BufferedWriter(new FileWriter(
                    new File("/Users/thilina/csu/classes/581/project/data/20houses_set2.csv")));
            String line;
            int counter = 0;
            while ((line = bufferedReader.readLine()) != null) {
                String[] segments = line.split(",");
                String houseIdStr = segments[6];
                int houseId = Integer.parseInt(houseIdStr);

                if (houseId >= 20 && houseId < 30) {
                    writer10.write(line);
                    writer10.newLine();
                }
                if (houseId > 20) {
                    writer20.write(line);
                    writer20.newLine();
                }

                counter++;
                if (counter % 1000000 == 0) {
                    System.out.println("Counter: " + counter);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                writer10.flush();
                writer20.flush();
                writer10.close();
                writer20.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
