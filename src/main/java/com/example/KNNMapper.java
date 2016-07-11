package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.deser.DataFormatReaders;

import java.io.IOException;

public class KNNMapper                    // Mapper class
        extends Mapper<Object, Text, DoubleWritable, Text> {

    // input pattern we are trying to classify
    private String[] inputPattern;

    // array of type Object with parsed input set
    private Object[] parsedInputPattern;

    // to hold label
    private static Text label = new Text();

    // to hold computed euclidean distance
    private static DoubleWritable distance = new DoubleWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
        // set the test example we're trying to classify
        Configuration conf = context.getConfiguration();

        // retrieve input type
        inputPattern = conf.getStrings("INPUT_PATTERN_CONF");
        // parse input pattern to correct types CarOwners.csv
//        parsedInputPattern = new Object[]{Integer.parseInt(inputPattern[0]), Integer.parseInt(inputPattern[1]),
//               inputPattern[2], inputPattern[3], Integer.parseInt(inputPattern[4])};
        parsedInputPattern = new Object[inputPattern.length];
        for(int i=0;i<inputPattern.length;i++) {
            parsedInputPattern[i] = Double.parseDouble(inputPattern[i]);
        }

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // split row into array of feature set
        String[] row = value.toString().split(",");

        // to be able to store objects of varying types
        // CarOwners.csv
//        Object[] parsedRow = {Integer.parseInt(row[0]), Integer.parseInt(row[1]), row[2], row[3], Integer.parseInt
//                (row[4]), row[5]};
//        label.set(row[5]);
        Object[] parsedRow = new Object[row.length - 1];
        for(int i=0;i<parsedRow.length;i++) {
            parsedRow[i] = Double.parseDouble(row[i+1]);
        }
        label.set(row[0]);

        // compute euclidean distance
        distance.set(getDoubleDistance(parsedInputPattern, parsedRow));

        context.write(distance, label);
    }


    /**
     *
     * @param p1 pattern to classify
     * @param p2 a pattern in data (row)
     * @return the cosine similarity of two patterns.
     */
    private static double getDistance(Object[] p1, Object[] p2) {
        double dist = 0.0;

        for (int i = 0; i < p1.length; i++) {
            double difference = 0.0;
            if (p1[i] instanceof Integer) {  // only subtract if both features are int
                difference = (Integer) p1[i] - (Integer) p2[i];  // cast them back to type Integer
            }
            else if(p1[i] instanceof Double) {
                difference = (Double) p1[i] - (Double) p2[i];  // cast them back to type Integer

            }
            else if (p1[i] instanceof String) {
                if (p1[i].equals(p2[i])) {   // distance is 0 if they are same
                    difference = 0;
                } else {
                    difference = 1;  // distance is 1 if they different
                }
            }
            dist += difference * difference;
        }
        return Math.sqrt(dist);
    }

    private static double getDoubleDistance(Object[] p1, Object[] p2) {
        double dist = 0.0;

        for (int i = 0; i < p1.length; i++) {
            double difference = (Double) p1[i] - (Double) p2[i];

            dist += difference * difference;
        }
        return Math.sqrt(dist);
    }
}
