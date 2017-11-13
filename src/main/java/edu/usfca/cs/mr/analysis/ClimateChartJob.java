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

import java.io.*;
import java.util.Date;
import java.util.Iterator;

import static com.google.common.base.Ascii.SI;

/**
 * Created by zzc on 11/3/17.
 */

public class ClimateChartJob {
    private static String geoPrefix = "9mzs";

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
//                System.out.println(geoHash + "&" +temperature + "&" + precipitation);
                context.write(new IntWritable(month), new Text(temperature + "&" + precipitation));
            }
        }
    }

    public static class ClimateChartReducer1 extends Reducer<IntWritable, Text, Text, Text> {
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
            avgPrecipitation = totalPrecipitation / count;
//            System.out.println(key.get() + ":" + maxTemperature + "&" + minTemperature +
//                    "&" + avgPrecipitation + "&" + avgTemperature);
            int month = key.get() + 1;
            String monthStr = String.valueOf(month);
            monthStr = month < 10 ? "0" + month : monthStr;

            context.write(new Text(monthStr), new Text(String.format("%.5f", maxTemperature) + " " +
                    String.format("%.5f", minTemperature) + " " + String.format("%.0f", avgPrecipitation) +
                    " " + String.format("%.3f", avgTemperature)));
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

                BufferedReader br = new BufferedReader(new FileReader(args[1] + "part-r-00000"));
                StringBuffer result = new StringBuffer();
                String line;
                while( (line = br.readLine()) != null){
                    result.append(line + "\n");
                }
                result.insert(0, "# Snowmass Village, CO (SI Units)\n");

                File output = new File("output_climate2/climate-data.clim");
                FileWriter fileWriter = new FileWriter(output);
                fileWriter.write(result.toString());
                fileWriter.flush();
                fileWriter.close();
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
