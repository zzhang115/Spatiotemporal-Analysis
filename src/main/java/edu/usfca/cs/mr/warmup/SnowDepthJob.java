package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.logging.Logger;

/**
 * Created by zzc on 11/3/17.
 */
public class SnowDepthJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "SnowDepth job1");
            job1.setJarByClass(SnowDepthJob.class);
            job1.setMapperClass(SnowDepthMapper.SnowDepthMapper1.class);

            job1.setCombinerClass(SnowDepthReducer.SnowDepthReducer1.class);
            job1.setReducerClass(SnowDepthReducer.SnowDepthReducer1.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(DoubleWritable.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            File output1 = new File(args[1]);
            if (output1.isDirectory()) {
                for (File file : output1.listFiles()) {
                    file.delete();
                }
                output1.delete();
            }

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            job1.waitForCompletion(true);

            Job job2 = Job.getInstance(conf, "SnowDepth job2");
            job2.setJarByClass(SnowDepthJob.class);
            job2.setMapperClass(SnowDepthMapper.SnowDepthMapper2.class);
            // Combiner. We use the reducer as the combiner in this case.
//            job2.setCombinerClass(SnowDepthReducer.SnowDepthReducer2.class);
            job2.setReducerClass(SnowDepthReducer.SnowDepthReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            File output2 = new File(args[2]);
            if (output2.isDirectory()) {
                for (File file : output2.listFiles()) {
                    file.delete();
                }
                output2.delete();
            }

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            job2.waitForCompletion(true);

            System.exit(0);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
