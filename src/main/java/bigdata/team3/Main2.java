package bigdata.team3;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class Main2 {

    final static DoubleWritable one = new DoubleWritable(1);
    volatile static double tranSize = 990002.0;

    public static void main(String[] args) throws Exception {
//        final String input = args[0];
//        String rootout = args[1];
//        tranSize = Double.parseDouble(args[2]);
//        final String NAME_NODE = "hdfs://SrvT2C2Master:8020";

//        for (int i = 1; i < 6; i++) {
//            Configuration conf = new Configuration();
//            FileSystem fs = FileSystem.get(conf);
//
//            if (i > 1) {
//                DistributedCache.addCacheFile(new URI(NAME_NODE + rootout + "/out" + (i - 1)+"/part-r-00000"), conf);
//
//            }
//
//            Job job = Job.getInstance(conf, "frequent itemset k-phase :" + i);
//            job.setJarByClass(Main2.class);
//            job.setMapperClass(Map.class);
//
//            job.setReducerClass(Reduce.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(DoubleWritable.class);
//            job.setNumReduceTasks(1);
//
//
//            FileInputFormat.addInputPath(job, new Path(input));
//
//            Path path = new Path(rootout + "/out" + i);
//            fs.delete(path, true);
//
//            FileOutputFormat.setOutputPath(job, path);
//
//            if (!job.waitForCompletion(true)) {
//                break;
//            }
//        }


        TObjectDoubleHashMap<Itemset> itemset = new TObjectDoubleHashMap<>();

        List<String> lines = Files.readAllLines(Paths.get("C:\\Users\\F\\Dropbox\\class\\big data\\Project\\code\\out\\part-r-00000"), StandardCharsets.UTF_8);
        for (String line : lines) {
            itemset.put(new Itemset(line.split("\\s+")[0].trim()), 0);
        }

        generateCandidates(itemset, 1);

    }


    private static void generateCandidates(TObjectDoubleHashMap<Itemset> itemsets, int currentItemCount) {

        Itemset[] items = itemsets.keys(new Itemset[0]);

        // compare each pair of itemsets of size n-1
        for (int i = 0; i < items.length; i++) {
            int[] X = items[i].getItems();

            for (int j = i + 1; j < items.length; j++) {
                int[] Y = items[j].getItems();

                assert (X.length == Y.length);

                //make a string of the first n-2 tokens of the strings
                int[] newCand = new int[currentItemCount + 1];
                System.arraycopy(X, 0, newCand, 0, newCand.length - 1);

                int ndifferent = 0;
                // then we find the missing value
                for (int aY : Y) {
                    boolean found = false;
                    // is Y[s1] in X?
                    for (int aX : X) {
                        if (aX == aY) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) { // Y[s1] is not in X
                        ndifferent++;
                        // we put the missing value at the end of newCand
                        newCand[newCand.length - 1] = aY;
                    }

                }

                // we have to find at least 1 different, otherwise it means that we have two times the same set in the existing candidates
                assert (ndifferent > 0);


                if (ndifferent == 1) {
                    itemsets.put(new Itemset(newCand), 0);
                }
            }
        }

        for (Itemset item : items) {
            if (item.getItems().length == currentItemCount) {
                itemsets.remove(item);
            }
        }
//            currentItemCount++;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Text item = new Text();
        TObjectDoubleHashMap<Itemset> trans = new TObjectDoubleHashMap<>();
        TObjectDoubleHashMap<Itemset> itemset = new TObjectDoubleHashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            int iter = Integer.parseInt(context.getJobName().split(":")[1]);
            if (iter > 1) {
                URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());
                for (URI cacheFile : cacheFiles) {
                    {
                        Path p = new Path(cacheFile);
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        FSDataInputStream open = new FSDataInputStream(fs.open(p));
                        BufferedReader d = new BufferedReader(new InputStreamReader(open));
                        String line;
                        while ((line = d.readLine()) != null) {
                            itemset.put(new Itemset(line.split("\\s+")[0].trim()), 0);
                        }
                        d.close();
                    }
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int iter = Integer.parseInt(context.getJobName().split(":")[1]);
            if (iter > 1) {
                String line = value.toString();
                String[] split = line.split("\\s+");
                trans.put(new Itemset(split[0].trim()), 0);
            } else {
                String line = value.toString();
                String[] split = line.split("\\s+");
                for (String s : split) {
                    item.set(s);
                    context.write(item, one);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            int k = context.getConfiguration().getInt("phaseCounter", 1);
            int k = Integer.parseInt(context.getJobName().split(":")[1]);
            if (k > 1) {
                Itemset[] keys = trans.keys(new Itemset[0]);

                generateCandidates(itemset, k - 1);

                Itemset[] candidates = itemset.keys(new Itemset[0]);

                for (Itemset c : candidates) {
                    for (Itemset basket : keys) {
                        if (basket.contains(c)) {
                            c.increment();
                        }
                    }
                }

                for (Itemset c : candidates) {
                    item.set(c.toString());
                    one.set(c.getCount());
                    context.write(item, one);
                }
            }
            super.cleanup(context);
        }

    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            final double numtran = 1.0 / 990002.0;
            final double minsup = 0.005;
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            if ((sum * numtran) >= minsup)
                context.write(key, new DoubleWritable(sum * numtran));

        }
    }

}
