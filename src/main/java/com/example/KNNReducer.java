package com.example;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class KNNReducer extends Reducer<DoubleWritable, Text, Text, NullWritable> {
    private static int k;
    private static final Log LOG = LogFactory.getLog(KNNReducer.class);
    // global variable to hold neighbours count
    private int count = 0;

    // the output classification
    private Text word = new Text();

    private Text valueText = new Text();
    // global variable to hold the neighbours
    private List<String> labelCounts = new ArrayList<String>();

    protected void setup(Context context) throws IOException, InterruptedException {
        // retrieve k
        Configuration conf = context.getConfiguration();
        k = conf.getInt("K_CONF", 5);
    }

    /**
     * for each key and associated label values, retrieve
     * and store label as long as we haven't exceeded k
     *
     * @param key  euclidean distance
     * @param values associated array of labels
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> valuesList = values.iterator();

        // retrieve the label
        while(count < k && valuesList.hasNext()){

            // add label to a list so we can find most frequent
            String v = valuesList.next().toString();
            labelCounts.add(v);
            count++;
            LOG.info("Count: "+k+" and Value = "+v);
            // we have as many as k
            if( count == k ){
                // get the most frequent... we're done!
//                String maxLabel = getMax(labelCounts);
//                word.set(maxLabel);
                context.write(word, null);
                valueText.set(labelCounts.toString());
                context.write(valueText, null);
            }
        }
    }

    /**
     *
     * @param labelCounts hold k-closest labels
     * @return the most frequent label
     */
    private static String getMax(List<String> labelCounts){
        int max = 0;
        int curr = 0;
        String currKey =  null;

        // identify the unique labels
        Set<String> unique = new HashSet<String>(labelCounts);

        // get the frequency each label in labelCounts
        for (String key : unique) {
            curr = Collections.frequency(labelCounts, key);

            // set current max and corresponding label
            if(max < curr){
                max = curr;
                currKey = key;
            }
        }
        return currKey;
    }
}
