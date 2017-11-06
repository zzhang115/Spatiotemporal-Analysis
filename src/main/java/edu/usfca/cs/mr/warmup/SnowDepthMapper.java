package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SnowDepthMapper {

    public static class SnowDepthMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map (LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String geoHash = value.toString().trim().split("\\s+")[1];
            String snowDepthStr = value.toString().trim().split("\\s+")[50];
            Double snowDepth = Double.parseDouble(snowDepthStr);
            if (snowDepth > 0) {
                context.write(new Text(geoHash), new DoubleWritable(snowDepth));
            }
        }
    }

    public static class SnowDepthMapper2 extends Mapper<Text, DoubleWritable, Text, Text> {
        @Override
        protected void map (Text key, DoubleWritable value, Context context)
        throws IOException, InterruptedException {
            context.write(new Text("SnowDepth"), new Text(key + "&" + value));
        }
    }
}
