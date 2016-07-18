package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KNNToolMapred extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KNNToolMapred(), args);
        System.exit(res);


    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("com.example.KNNToolMapred <train_dir> <output_dir> <num_of_reducer> <k> <input_pattern> ");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // set k
        conf.set("K_CONF",args[3]);

        // set pattern to classify
        conf.set("INPUT_PATTERN_CONF",args[4]);




        Job job = Job.getInstance(conf, "com.example.KNNToolMapred");
        job.setJarByClass(KNNToolMapred.class);

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


        return job.waitForCompletion(true) ? 0 : 1;
    }


}
