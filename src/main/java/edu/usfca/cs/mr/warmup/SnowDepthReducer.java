package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SnowDepthReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    private class Node {
        String geoHash;
        double snowDepth;
        Node(String geoHash, Double snowDepth) {
            this.geoHash = geoHash;
            this.snowDepth = snowDepth;
        }
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        List<Node> nodes = new ArrayList<Node>();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String value = iterator.next().toString();
            String geoHash = value.trim().split(":")[0];
            Double snowDepth = Double.parseDouble(value.trim().split(":")[1]);
            System.out.println(geoHash + ":" + snowDepth);
            nodes.add(new Node(geoHash, snowDepth));
//                context.write(new Text(geoHash), new DoubleWritable(snowDepth));
//                queue.offer(new Node(geoHash, snowDepth));
        }
//            context.write(new Text("TotalLine: "), new DoubleWritable(0.07234));
        System.out.println("size: " + nodes.size());
        for (Node node : nodes) {
            System.out.println("node");
            context.write(new Text(node.geoHash), new DoubleWritable(node.snowDepth));
        }
    }

}
