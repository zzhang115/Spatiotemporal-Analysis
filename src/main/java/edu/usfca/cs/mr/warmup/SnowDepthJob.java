package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by zzc on 11/3/17.
 */
public class SnowDepthJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        String inputDataDir = args[0];
//        String outputDataDir = args[1];
//
//        File output3 = new File(outputDataDir);
//        if (output3.exists()) {
//            if (output3.isDirectory()) {
//                for (File file : output3.listFiles()) {
//                    file.delete();
//                }
//            }
//            output3.delete();
//        }
//
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf);
//        job.setMapperClass(SnowDepthMapper.class);
//        job.setCombinerClass(SnowDepthReducer.class);
//        job.setReducerClass(SnowDepthReducer.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
//
//        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
//        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
//        job.waitForCompletion(true);
          try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "SnowDepth job");
            // Current class.
            job.setJarByClass(SnowDepthJob.class);
            // Mapper
            job.setMapperClass(SnowDepthMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(SnowDepthReducer.class);
            // Reducer
            job.setReducerClass(SnowDepthReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            File output = new File(args[1]);
            if (output.exists()) {
                if (output.isDirectory()) {
                    for (File file : output.listFiles()) {
                        file.delete();
                    }
                }
                output.delete();
            }
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
