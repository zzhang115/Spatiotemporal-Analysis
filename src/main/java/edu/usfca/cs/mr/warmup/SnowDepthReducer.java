package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SnowDepthReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
    throws IOException, InterruptedException {
        Iterator<DoubleWritable> iterator = values.iterator();
        Double snowDepthCount = 0.0;
        while (iterator.hasNext()) {
            snowDepthCount += iterator.next().get();
        }
        context.write(key, new DoubleWritable(snowDepthCount));
    }

}
