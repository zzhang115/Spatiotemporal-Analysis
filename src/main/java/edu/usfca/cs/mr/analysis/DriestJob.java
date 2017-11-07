package edu.usfca.cs.mr.analysis;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.SpatialRange;
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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by zzc on 11/3/17.
 */

public class DriestJob {
    private static float upperLat = (float) 38.505;
    private static float lowerLat = (float) 37.265;
    private static float upperLon = (float) -121.624;
    private static float lowerLon = (float) -123.041;

    private static boolean isWithinBayArea(SpatialRange range) {
        return range.getLowerBoundForLatitude() >= lowerLat && range.getUpperBoundForLatitude() <= upperLat &&
                range.getLowerBoundForLongitude() >= lowerLon && range.getUpperBoundForLongitude() <= upperLon;
    }

    public static class DriestMapper1 extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String timestamp = value.toString().split("\t")[0];
            String geoHash = value.toString().split("\t")[1];
            String humidity = value.toString().split("\t")[12];

            Date date = new Date(Long.parseLong(timestamp));
            int month = date.getMonth();

            SpatialRange range = Geohash.decodeHash(geoHash);

            if (isWithinBayArea(range)) {
                context.write(new IntWritable(month), new DoubleWritable(Double.parseDouble(humidity)));
            }
        }
    }

    public static class DriestReducer1 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> iterator = values.iterator();
            Double humidity = 0.0;
            while (iterator.hasNext()) {
                humidity += iterator.next().get();
            }
            context.write(key, new DoubleWritable(humidity));
        }
    }

    public static class DriestMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("Driest"), value);
        }
    }

    public static class DriestReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            int month = 0;
            double humidity = Double.MAX_VALUE;

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                int monthTemp = Integer.parseInt(value.split("\t")[0]);
                double humidityTemp = Double.parseDouble(value.split("\t")[1]);
                if (humidityTemp < humidity) {
                    month = monthTemp;
                    humidity = humidityTemp;
                }
            }
            context.write(new Text(String.valueOf(month)), new Text(String.valueOf(humidity)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "Driest Job1");
                job1.setJarByClass(DriestJob.class);
                job1.setMapperClass(DriestMapper1.class);
                job1.setCombinerClass(DriestReducer1.class);
                job1.setReducerClass(DriestReducer1.class);

                job1.setMapOutputKeyClass(IntWritable.class);
                job1.setMapOutputValueClass(DoubleWritable.class);

                job1.setOutputKeyClass(IntWritable.class);
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

                Job job2 = Job.getInstance(conf, "Driest Job2");
                job2.setJarByClass(DriestJob.class);
                job2.setMapperClass(DriestMapper2.class);
                // Combiner. We use the reducer as the combiner in this case.
                job2.setReducerClass(DriestReducer2.class);

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
