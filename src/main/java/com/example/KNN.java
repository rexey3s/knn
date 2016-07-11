package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN {
    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("com.example.KNN <train_dir> <output_dir> <num_of_reducer> <k> <input_pattern> ");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // set k
        conf.set("K_CONF",args[3]);

        // set pattern to classify
        conf.set("INPUT_PATTERN_CONF",args[4]);
        // mode

        Job job = Job.getInstance(conf, "com.example.KNN");
        job.setJarByClass(KNN.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);

        // set no of reducer task
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        // specify input and output dirs
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
