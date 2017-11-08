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
import java.util.Date;
import java.util.Iterator;

/**
 * Created by zzc on 11/3/17.
 */

public class BestTimeJob {
    private static final float TEMPERATURE = (float) 70.0; // 20 to 22 °C (68 to 72 °F).
    private static final float HUMIDITY = (float) 55; // 50% to 60%
    // https://en.wikipedia.org/wiki/Room_temperature
    // https://en.wikipedia.org/wiki/Relative_humidity

    private static float comfortLevel(Condition condition) {
        float totalDifference = (float) 0.0;
        totalDifference += Math.abs((condition.temperature - TEMPERATURE)) / TEMPERATURE;
        totalDifference += Math.abs((condition.humidity- HUMIDITY)) / HUMIDITY;
        return totalDifference;
    }

    private static class Condition {
        private float temperature;
        private float humidity;

        Condition(float temperature, float humidity) {
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }

    public static class BestTimeMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String timestamp = value.toString().split("\t")[0];
            String geoHash = value.toString().split("\t")[1];
            String humidity = value.toString().split("\t")[12];
            String temperature = value.toString().split("\t")[40];
            Date date = new Date(Long.parseLong(timestamp));
            int month = date.getMonth();

            context.write(new Text(geoHash + "&" + month), new Text(humidity + "&" + temperature));
        }
    }

    public static class BestTimeReducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            float totalHumidity = (float) 0.0;
            float totalTemperature = (float) 0.0;
            float averageHumdity;
            float averageTemperature;
            long count = 0;
            System.out.println("key:"+key);
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                System.out.println(value);
                float humidity = Float.parseFloat(value.split("\t")[0]);
                float temperature = Float.parseFloat(value.split("\t")[1]);
                totalHumidity += humidity;
                totalTemperature += temperature;
                count++;
            }
            averageHumdity = totalHumidity / count;
            averageTemperature = totalTemperature / count;
            context.write(key, new Text(averageHumdity + "&" + averageTemperature));
        }
    }

    public static class BestTimeMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("BestTime"), value);
        }
    }

    public static class BestTimeReducer2 extends Reducer<Text, Text, Text, Text> {
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
                Job job1 = Job.getInstance(conf, "BestTime Job1");
                job1.setJarByClass(BestTimeJob.class);
                job1.setMapperClass(BestTimeMapper1.class);
//                job1.setCombinerClass(BestTimeReducer1.class);
                job1.setReducerClass(BestTimeReducer1.class);

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

//                Job job2 = Job.getInstance(conf, "BestTime Job2");
//                job2.setJarByClass(BestTimeJob.class);
//                job2.setMapperClass(BestTimeMapper2.class);
//                // Combiner. We use the reducer as the combiner in this case.
//                job2.setReducerClass(BestTimeReducer2.class);
//
//                job2.setMapOutputKeyClass(Text.class);
//                job2.setMapOutputValueClass(Text.class);
//                job2.setOutputKeyClass(Text.class);
//                job2.setOutputValueClass(Text.class);
//
//                File output2 = new File(args[2]);
//                if (output2.isDirectory()) {
//                    for (File file : output2.listFiles()) {
//                        file.delete();
//                    }
//                    output2.delete();
//                }
//
//                FileInputFormat.addInputPath(job2, new Path(args[1]));
//                FileOutputFormat.setOutputPath(job2, new Path(args[2]));
//                job2.waitForCompletion(true);

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
