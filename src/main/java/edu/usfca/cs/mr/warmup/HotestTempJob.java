package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Created by zzc on 11/3/17.
 */

public class HotestTempJob {

    public static class HotestTempMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String timeStamp = value.toString().split("\\s+")[0];
            String geoHash = value.toString().split("\\s+")[1];
            String temp = value.toString().split("\\s+")[40];
            context.write(new Text("Temp"), new Text(timeStamp + "&" + geoHash + "&" + temp));
        }
    }

    public static class HotestTempReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            double maxTemp = 0.0;
            String geoHash = "";
            String timeStamp = "";
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                double temp = Double.parseDouble(value.split("&")[2]);
                String timestamp = value.split("&")[0];
                String geohash = value.split("&")[1];
                if (temp > maxTemp) {
                    maxTemp = temp;
                    timeStamp = timestamp;
                    geoHash = geohash;
                }
            }
            context.write(new Text("Temp"), new Text(timeStamp + "&" + geoHash + "&" +  maxTemp));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "HotestTemp Job");
            job.setJarByClass(HotestTempJob.class);
            job.setMapperClass(HotestTempMapper.class);
            job.setCombinerClass(HotestTempReducer.class);
            job.setReducerClass(HotestTempReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            File output1 = new File(args[1]);
            if (output1.isDirectory()) {
                for (File file : output1.listFiles()) {
                    file.delete();
                }
                output1.delete();
            }

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

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
