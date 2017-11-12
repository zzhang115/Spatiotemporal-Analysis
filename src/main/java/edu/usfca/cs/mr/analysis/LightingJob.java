package edu.usfca.cs.mr.analysis;

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

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Created by zzc on 11/3/17.
 */

public class LightingJob {

    public static class LightingMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String geoHash = value.toString().split("\t")[1];
            geoHash = geoHash.substring(0, 4);
            String lighting = value.toString().split("\t")[22];
            context.write(new Text(geoHash), new DoubleWritable(Double.parseDouble(lighting)));
        }
    }

    public static class LightingReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> iterator = values.iterator();
            Double lighting = 0.0;
            while (iterator.hasNext()) {
                lighting += iterator.next().get();
            }
            context.write(key, new DoubleWritable(lighting));
        }
    }

    public static class LightingMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String geoHash = value.toString().split("\t")[0];
            Double lighting = Double.parseDouble(value.toString().split("\t")[1]);

            context.write(new Text("Lighting"), new Text(geoHash + "\t" + lighting));
        }
    }

    public static class LightingReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            PriorityQueue<Node> queue = new PriorityQueue<>(3, new NodeComparator());

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                String geoHash = value.split("\t")[0];
                Double lighting = Double.parseDouble(value.split("\t")[1]);

                if (queue.size() < 3) {
                    queue.offer(new Node(geoHash, lighting));
                } else {
                    if (lighting > queue.peek().lighting) {
                        queue.poll();
                        queue.offer(new Node(geoHash, lighting));
                    }
                }
            }

            System.out.println(queue.size());
            while (!queue.isEmpty()) {
                Node node = queue.poll();
                context.write(new Text(node.geoHash), new Text(node.lighting.toString()));
            }
        }
    }

    private static class NodeComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.lighting > b.lighting) {
                return 1;
            } else if (a.lighting < b.lighting) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private static class Node {
        String geoHash;
        Double lighting;
        Node(String geoHash, Double lighting) {
            this.geoHash = geoHash;
            this.lighting = lighting;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "Lighting Job1");
            job1.setJarByClass(LightingJob.class);
            job1.setMapperClass(LightingMapper1.class);
//            job1.setCombinerClass(LightingReducer1.class);
            job1.setReducerClass(LightingReducer1.class);

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
              
            Job job2 = Job.getInstance(conf, "Lighting Job2");
            job2.setJarByClass(LightingJob.class);
            job2.setMapperClass(LightingMapper2.class);
            // Combiner. We use the reducer as the combiner in this case.
            job2.setReducerClass(LightingReducer2.class);
              
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
