package edu.usfca.cs.mr.warmup;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Created by zzc on 11/3/17.
 */
public class SnowDepth {
    final static Logger logger = Logger.getLogger("SnowDepth");

    private static PriorityQueue<Node> queue = new PriorityQueue<Node>(10, new Comparator<Node>() {
        public int compare(Node a, Node b) {
            if (a.snowDepth > b.snowDepth) {
                return 1;
            } else if (a.snowDepth < b.snowDepth) {
                return -1;
            } else {
                return 0;
            }
        }
    });

    private static class Node {
        String geoHash;
        Double snowDepth;
        Node(String geoHash, Double snowDepth) {
            this.geoHash = geoHash;
            this.snowDepth = snowDepth;
        }
    }

    public static class SnowDepthMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            String geoHash = value.toString().split(".+")[1];
            String snowDepth = value.toString().split(".+")[50];
            System.out.println(geoHash + " " + snowDepth);
            context.write(new Text("A"), new Text(geoHash + ":" + snowDepth));
//            context.write(new Text("A"), new IntWritable(1));
            context.write(new Text("A"), new Text("B"));
        }
    }

    public static class SnowDepthReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                String geoHash = value.split(":")[0];
                Double snowDepth = Double.parseDouble(value.split(":")[1]);
                queue.offer(new Node(geoHash, snowDepth));
            }
//            context.write(new Text("TotalLine: "), new DoubleWritable(0.07234));
            context.write(new Text(queue.peek().geoHash), new DoubleWritable(queue.peek().snowDepth));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputDataDir = args[0];
        String outputDataDir = args[1];

        File output3 = new File(outputDataDir);
        if (output3.exists()) {
            logger.info("Output3 directory already exits!\tDelete previous directory.");
            FileUtils.deleteDirectory(output3);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(SnowDepthMapper.class);
        job.setCombinerClass(SnowDepthReducer.class);
        job.setReducerClass(SnowDepthReducer.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
        System.out.println(inputDataDir);
        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
        job.waitForCompletion(true);
    }
}
