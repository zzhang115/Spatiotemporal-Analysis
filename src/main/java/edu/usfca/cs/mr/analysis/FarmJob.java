package edu.usfca.cs.mr.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class FarmJob {
    public static class FarmMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String geoHash = value.toString().split("\t")[1];
            double u_wind = Double.parseDouble(value.toString().split("\t")[14]);
            double v_wind = Double.parseDouble(value.toString().split("\t")[51]);
            double windSpeed = Math.sqrt(Math.pow(u_wind, 2) + Math.pow(v_wind, 2));
            double cloudCover = Double.parseDouble(value.toString().split("\t")[16]);

            if (cloudCover < 0) {
                return;
            }
            context.write(new Text(geoHash), new Text(windSpeed + "&" + cloudCover));
        }
    }

    public static class FarmReducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            double totalWind= 0.0;
            double totalCloudCover = 0.0;
            double averageWind;
            double averageCloudCover;
            long count = 0;
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                double windSpeed = Double.parseDouble(value.split("&")[0]);
                double cloudCover = Double.parseDouble(value.split("&")[1]);
                totalWind += windSpeed;
                totalCloudCover += cloudCover;
                count++;
            }
            averageWind = totalWind / count;
            averageCloudCover = totalCloudCover / count;
            context.write(key, new Text(averageWind+ "&" + averageCloudCover));
        }
    }

    public static class FarmMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("Farm"), value);
        }
    }

    public static class FarmReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();

            int queueSize = 10;
            PriorityQueue<Node> windQueue = new PriorityQueue<Node>(3, new WindComparator());
            PriorityQueue<Node> cloudQueue = new PriorityQueue<Node>(3, new CloudComparator());
            PriorityQueue<Node> windcloudQueue = new PriorityQueue<Node>(3, new WindCloudComparator());

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                String[] array = value.split("\t");

                String geohash = array[0];
                double windSpeed = Double.parseDouble(array[1].split("&")[0]);
                double cloudCover = Double.parseDouble(array[1].split("&")[1]);

                Node node = new Node(geohash, windSpeed, cloudCover);

                if (windQueue.size() < queueSize) {
                    windQueue.offer(node);
                } else {
                    if (node.windspeed > windQueue.peek().windspeed) {
                        windQueue.poll();
                        windQueue.offer(node);
                    }
                }

                if (cloudQueue.size() < queueSize) {
                    cloudQueue.offer(node);
                } else {
                    if (node.cloudCover < cloudQueue.peek().cloudCover) {
                        cloudQueue.poll();
                        cloudQueue.offer(node);
                    }
                }

                if (windcloudQueue.size() < queueSize) {
                    windcloudQueue.offer(node);
                } else {
                    if (node.score > windcloudQueue.peek().score) {
                        windcloudQueue.poll();
                        windcloudQueue.offer(node);
                    }
                }
            }
            while (!windQueue.isEmpty()) {
                Node node = windQueue.poll();
                context.write(new Text("Best top3 wind locations(" + node.geoHash),
                        new Text(" windSpeed: " + node.windspeed + " cloudCover: " + node.cloudCover + ")"));
            }

            while (!cloudQueue.isEmpty()) {
                Node node = cloudQueue.poll();
                context.write(new Text("Best top3 solar locations(" + node.geoHash),
                        new Text(" windSpeed: " + node.windspeed + " cloudCover: " + node.cloudCover + ")"));
            }

            while (!windcloudQueue.isEmpty()) {
                Node node = windcloudQueue.poll();
                context.write(new Text("Best top3 wind and solar locations(" +node.score + " " + node.geoHash),
                        new Text(" windSpeed: " + node.windspeed + " cloudCover: " + node.cloudCover + ")"));
            }
        }
    }

    private static class Node {
        private String geoHash;
        private double cloudCover;
        private double windspeed;
        private double score;
        public Node(String geoHash, double cloudCover, double windspeed) {
            this.geoHash = geoHash;
            this.cloudCover = cloudCover;
            this.windspeed = windspeed;
            this.score = this.windspeed + (-1) * this.cloudCover;
        }
    }

    private static class CloudComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.cloudCover < b.cloudCover) {
                return 1;
            } else if (a.cloudCover > b.cloudCover) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private static class WindComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.windspeed > b.windspeed) {
                return 1;
            } else if (a.windspeed < b.windspeed) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private static class WindCloudComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.score > b.score) {
                return 1;
            } else if (a.score < b.score) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "Farm Job1");
                job1.setJarByClass(FarmJob.class);
                job1.setMapperClass(FarmMapper1.class);
//                job1.setCombinerClass(FarmReducer1.class);
                job1.setReducerClass(FarmReducer1.class);

                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);

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

                Job job2 = Job.getInstance(conf, "Farm Job2");
                job2.setJarByClass(FarmJob.class);
                job2.setMapperClass(FarmMapper2.class);
                // Combiner. We use the reducer as the combiner in this case.
                job2.setReducerClass(FarmReducer2.class);

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
