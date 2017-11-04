package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SnowDepthMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String geoHash = value.toString().trim().split("\\s+")[1];
        String snowDepthStr = value.toString().trim().split("\\s+")[50];
//            System.out.println(geoHash + ":" + snowDepth);
        Double snowDepth = Double.parseDouble(snowDepthStr);
        if (snowDepth > 0) {
            context.write(new Text("A"), new Text(geoHash + ":" + snowDepth));
        }
    }
}
