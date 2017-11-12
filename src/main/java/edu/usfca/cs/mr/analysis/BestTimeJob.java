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
import java.util.*;

/**
 * Created by zzc on 11/3/17.
 */

public class BestTimeJob {
    private static final double TEMPERATURE = (float) 70.0; // 20 to 22 °C (68 to 72 °F).
    private static final double HUMIDITY = (float) 55; // 50% to 60%
    // https://en.wikipedia.org/wiki/Room_temperature
    // https://en.wikipedia.org/wiki/Relative_humidity
    
    private static double comfortLevel(double temperature, double humidity) {
        double totalDifference = 0.0;
        totalDifference += Math.abs((temperature - TEMPERATURE)) / TEMPERATURE;
        totalDifference += Math.abs((humidity- HUMIDITY)) / HUMIDITY;
        return totalDifference;
    }

    private static class RegionCondition {
        private String geohash;
        private int month;
        private double temperature;
        private double humidity;
        private double comfort;
        private double score;

        public RegionCondition(String geohash, int month, double temperature, double humidity,
                               double variance) {
            this.geohash = geohash;
            this.month = month;
            this.temperature = (float) (9 / 5) * (temperature - (float) 273.15) + 32;
//            this.temperature = temperature - (float) 273.15;
            this.humidity = humidity;
            this.comfort = comfortLevel(this.temperature, this.humidity);
            this.score = variance + this.comfort;
        }

        @Override
        public boolean equals(Object obj) {
            if (this.geohash.equals(((RegionCondition) (obj)).geohash)) {
                return true;
            }
            return false;
        }
    }

    private static class ConditionComparator implements Comparator<RegionCondition> {
        public int compare(RegionCondition a, RegionCondition b) {
            if (a.score < b.score) {
                return 1;
            } else if (a.score > b.score) {
                return -1;
            } else {
                return 0;
            }
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
            double totalHumidity = 0.0;
            double totalTemperature = 0.0;
            double averageHumdity;
            double averageTemperature;
            double humidityVariance = 0.0;
            double temperatureVariance = 0.0;
            List<Double> humiditys = new ArrayList<Double>();
            List<Double> temperatures = new ArrayList<Double>();

            long count = 0;
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                double humidity = Double.parseDouble(value.split("&")[0]);
                double temperature = Double.parseDouble(value.split("&")[1]);
                humiditys.add(humidity);
                temperatures.add(temperature);
                totalHumidity += humidity;
                totalTemperature += temperature;
                count++;
            }
            averageHumdity = totalHumidity / count;
            averageTemperature = totalTemperature / count;

            for (int i = 0; i < count; i++) {
                humidityVariance += Math.pow((humiditys.get(i) - averageHumdity), 2);
                temperatureVariance += Math.pow((temperatures.get(i) - averageTemperature), 2);
            }
//            System.out.println(humidityVariance + " : " + temperatureVariance + " " + count);
            humidityVariance /= count;
            temperatureVariance /= count;
            context.write(key, new Text(averageHumdity + "&" + averageTemperature + "&" +
            humidityVariance + "&" + temperatureVariance));
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
            PriorityQueue<RegionCondition> queue = new PriorityQueue<>(5, new ConditionComparator());
            while (iterator.hasNext()) {

                String value = iterator.next().toString();
                String[] array = value.split("\t");

                String geohash = array[0].split("&")[0];
                int month= Integer.parseInt(array[0].split("&")[1]);
                double averageHumidity = Double.parseDouble(array[1].split("&")[0]);
                double averageTemperature = Double.parseDouble(array[1].split("&")[1]);
                double humidityVariance = Double.parseDouble(array[1].split("&")[2]);
                double temperatureVariance = Double.parseDouble(array[1].split("&")[3]);

//                System.out.println(geohash + " " +month + " " +averageHumidity + " " +averageTemperature + " " +
//                        humidityVariance + " " + temperatureVariance);

                RegionCondition regionCondition = new RegionCondition(geohash, month,
                        averageTemperature, averageHumidity, humidityVariance + temperatureVariance);
                if (!queue.contains(regionCondition)) {
                    if (queue.size() < 5) {
                        queue.offer(regionCondition);
                    } else {
                        if (regionCondition.score < queue.peek().score) {
                            queue.poll();
                            queue.offer(regionCondition);
                        }
                    }
                }
            }

            System.out.println(queue.size());
            while (!queue.isEmpty()) {
                RegionCondition condition = queue.poll();
                context.write(new Text(condition.geohash + " " + condition.month), 
                        new Text(condition.humidity + " " + condition.temperature + " " + condition.score));
                
            }
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

                Job job2 = Job.getInstance(conf, "BestTime Job2");
                job2.setJarByClass(BestTimeJob.class);
                job2.setMapperClass(BestTimeMapper2.class);
                // Combiner. We use the reducer as the combiner in this case.
                job2.setReducerClass(BestTimeReducer2.class);

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
