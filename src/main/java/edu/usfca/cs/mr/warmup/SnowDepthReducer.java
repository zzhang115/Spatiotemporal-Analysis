package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SnowDepthReducer {
    public static class SnowDepthReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce (Text key, Iterable <DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            Iterator<DoubleWritable> iterator = values.iterator();
            Double snowDepthCount = 0.0;
            while (iterator.hasNext()) {
                snowDepthCount += iterator.next().get();
            }
            context.write(key, new DoubleWritable(snowDepthCount));
        }
    }

    private static class NodeComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            if (a.snowDepth > b.snowDepth) {
                return 1;
            } else if (a.snowDepth < b.snowDepth) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    private static class Node {
        String geoHash;
        Double snowDepth;
        Node(String geoHash, Double snowDepth) {
            this.geoHash = geoHash;
            this.snowDepth = snowDepth;
        }
    }

    public static class SnowDepthReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce (Text key, Iterable <Text> values, Context context)
        throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            PriorityQueue<Node> queue = new PriorityQueue<>(10, new NodeComparator());

            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                String geohash = value.split(".+")[0];
                Double snowdepth = Double.parseDouble(value.split(".+")[1]);
                queue.offer(new Node(geohash, snowdepth));
            }
            for (int i = 0; i < 10; i++) {
                Node node = queue.poll();
                context.write(new Text(node.geoHash), new Text(node.snowDepth.toString()));
            }
        }
    }

}
