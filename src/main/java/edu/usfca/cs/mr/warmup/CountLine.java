package edu.usfca.cs.mr.warmup;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zzc on 11/3/17.
 */
public class CountLine {
    final static Logger logger = Logger.getLogger(CountLine.class);

    public static class CountLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(new Text("A"), new IntWritable(1));
        }
    }

    public static class CountLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                count += iterator.next().get();
            }
            context.write(new Text("TotalLine: "), new IntWritable(count));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputDataDir = args[0];
        String outputDataDir = args[1];

        File output2 = new File(outputDataDir);
        if (output2.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output2 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output2);
        }

        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(CountLineMapper.class);
        job.setReducerClass(CountLineReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
        job.waitForCompletion(true);
    }
}
