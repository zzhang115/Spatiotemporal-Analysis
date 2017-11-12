package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

/**
 * Created by zzc on 11/3/17.
 */
public class SnowDepthJob {
    public static class SnowDepthMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map (LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String geoHash = value.toString().trim().split("\t")[1];
            String snowDepthStr = value.toString().trim().split("\t")[50];
            Double snowDepth = Double.parseDouble(snowDepthStr);
            context.write(new Text(geoHash), new DoubleWritable(snowDepth));
        }
    }

    public static class SnowDepthMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map (LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String val = value.toString();
            String geoHash = val.split("\t+")[0];
            String snowDepth = val.split("\t+")[1];

            context.write(new Text("SnowDepth"), new Text(geoHash + "\t" + snowDepth));
        }
    }

    public static class SnowDepthReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce (Text key, Iterable <DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<DoubleWritable> iterator = values.iterator();
            Double snowDepthCount = 0.0;
            while (iterator.hasNext()) {
                Double temp = iterator.next().get();
                if (temp <= 0) {
                    return;
                }
                snowDepthCount += temp;
            }
            context.write(key, new DoubleWritable(snowDepthCount));
        }
    }

    private static class NodeComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.snowDepth > b.snowDepth) {
                return 1;
            } else if (a.snowDepth < b.snowDepth) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private static class Node {
        String geoHash;
        Double snowDepth;
        Node(String geoHash, Double snowDepth) {
            this.geoHash = geoHash;
            this.snowDepth = snowDepth;
        }
    }

    public static class SnowDepthReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce (Text key, Iterable <Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            PriorityQueue<Node> queue = new PriorityQueue<>(10, new NodeComparator());

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                String geohash = value.split("\t")[0];
                Double snowdepth = Double.parseDouble(value.split("\t")[1]);
                if (queue.size() < 10) {
                    queue.offer(new Node(geohash, snowdepth));
                } else {
                    if (snowdepth > queue.peek().snowDepth) {
                        queue.poll();
                        queue.offer(new Node(geohash, snowdepth));
                    }
                }
            }
            System.out.println(queue.size());
            while (!queue.isEmpty()){
                Node node = queue.poll();
                context.write(new Text(node.geoHash), new Text(node.snowDepth.toString()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "SnowDepth job1");
            job1.setJarByClass(SnowDepthJob.class);
            job1.setMapperClass(SnowDepthMapper1.class);

//            job1.setCombinerClass(SnowDepthReducer1.class);
            job1.setReducerClass(SnowDepthReducer1.class);

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
            job2.setMapperClass(SnowDepthMapper2.class);
            // Combiner. We use the reducer as the combiner in this case.
//            job2.setCombinerClass(SnowDepthReducer.SnowDepthReducer2.class);
            job2.setReducerClass(SnowDepthReducer2.class);

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
