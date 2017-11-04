package edu.usfca.cs.mr.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Created by zzc on 11/3/17.
 */
public class SnowDepthJob {
    final static Logger logger = Logger.getLogger("SnowDepthJob");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "SnowDepth job");
            // Current class.
            job.setJarByClass(SnowDepthJob.class);
            // Mapper
            job.setMapperClass(SnowDepthMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(SnowDepthReducer.class);
            // Reducer
            job.setReducerClass(SnowDepthReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            job.waitForCompletion(true);

            sortSnowDepth(new File(args[1]));
            writeResultToFile(args[1] + "result");
            System.exit(0);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    private static PriorityQueue<Node> queue = new PriorityQueue<Node>(10, new Comparator<Node>() {
        public int compare(Node a, Node b) {
            if (a.snowDepth > b.snowDepth) {
                return 1;
            } else if (a.snowDepth < b.snowDepth) {
                return -1;
            } else {
                return 0;
            }
        }
    });

    private static class Node {
        String geoHash;
        Double snowDepth;
        Node(String geoHash, Double snowDepth) {
            this.geoHash = geoHash;
            this.snowDepth = snowDepth;
        }
    }

    public static void sortSnowDepth(File dir) {
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                try {
                    BufferedReader fileReader = new BufferedReader(new FileReader(file));
                    String line;
                    while ((line = fileReader.readLine()) != null) {
                        String geoHash = line.split("\\s+")[0];
                        Double snowDepth = Double.parseDouble(line.split("\\s+")[1]);
                        queue.offer(new Node(geoHash, snowDepth));
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void writeResultToFile(String fileName) {
        try {
            FileWriter fileWriter = new FileWriter(fileName);
            for (Node node : queue) {
                fileWriter.write(node.geoHash + ":" + node.snowDepth);
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
