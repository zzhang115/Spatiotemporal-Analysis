package edu.usfca.cs.mr.analysis;

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

public class ClimateChartJob {
    private static String geoPrefix = "9e";

    public static class ClimateChartMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String timestamp = value.toString().split("\t")[0];
            String geoHash = value.toString().split("\t")[1];
            String temperature = value.toString().split("\t")[40];
            String precipitation = value.toString().split("\t")[55];

            Date date = new Date(Long.parseLong(timestamp));
            int month = date.getMonth();

            if (geoHash.startsWith(geoPrefix)) {
                System.out.println(geoHash + "&" +temperature + "&" + precipitation);

                context.write(new IntWritable(month), new Text(temperature + "&" + precipitation));
            }
        }
    }

    public static class ClimateChartReducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            double maxTemperature = Double.MIN_VALUE;
            double minTemperature = Double.MAX_VALUE;
            double avgTemperature;
            double avgPrecipitation;
            double totalTemperature = 0.0;
            double totalPrecipitation = 0.0;
            long count = 0;

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                double temperature = Double.parseDouble(value.split("&")[0]);
                double precipitation = Double.parseDouble(value.split("&")[1]);
                if (temperature >= maxTemperature) {
                    maxTemperature = temperature;
                }
                if (temperature <= minTemperature) {
                    minTemperature = temperature;
                }
                totalTemperature += temperature;
                totalPrecipitation += precipitation;
                count++;
            }
            avgTemperature = totalTemperature / count;
            avgTemperature = (9 / 5) * (avgTemperature - 273.15) + 32;
            maxTemperature = (9 / 5) * (maxTemperature - 273.15) + 32;
            minTemperature = (9 / 5) * (minTemperature - 273.15) + 32;

            avgPrecipitation = totalPrecipitation / count;
            System.out.println(key.get() + ":" + maxTemperature + "&" + minTemperature +
                    "&" + avgPrecipitation + "&" + avgTemperature);
            context.write(new IntWritable(key.get() + 1), new Text(maxTemperature + "\t" + minTemperature +
            "\t" + avgPrecipitation + "\t" + avgTemperature));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "ClimateChart Job1");
                job1.setJarByClass(ClimateChartJob.class);
                job1.setMapperClass(ClimateChartMapper1.class);
//                job1.setCombinerClass(ClimateChartReducer1.class);
                job1.setReducerClass(ClimateChartReducer1.class);

                job1.setMapOutputKeyClass(IntWritable.class);
                job1.setMapOutputValueClass(Text.class);

                job1.setOutputKeyClass(IntWritable.class);
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
