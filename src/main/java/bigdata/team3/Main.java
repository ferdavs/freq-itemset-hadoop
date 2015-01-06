package bigdata.team3;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.apache.hadoop.conf.Configuration;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {

    static int setsize = 4;
    static double tranSize = 0;

    public static void main(String[] args) throws Exception {
        tranSize = Double.parseDouble(args[2]);

        Configuration conf = new Configuration();
        long milliSeconds = 1000 * 60 * 120;
        conf.setLong("mapred.task.timeout", milliSeconds);

        Job job = Job.getInstance(conf, "frequent item 4");
        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outpath = new Path(args[1]);
        FileSystem.get(conf).delete(outpath, true);
        FileOutputFormat.setOutputPath(job, outpath);

        job.setNumReduceTasks(3);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        local();
    }

    private static void local() throws IOException {
        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger(0);
        TObjectIntHashMap map = new TObjectIntHashMap();

        String path = "C:\\Users\\F\\Desktop\\retail.dat";
        List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);

        final double numtran = 1.0 / (1.0 * lines.size());
        final double minsup = 0.01;


        for (String line : lines) {

            List<String> subsets = nsubsetgen(Arrays.asList(line.split(" ")), setsize);
            for (String subset : subsets) {
                if (!map.containsKey(subset))
                    map.put(subset, 1);
                else
                    map.increment(subset);
            }

            int i = counter.incrementAndGet();
            if (i % 1000 == 0)
                System.out.println(i);
        }

        try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get("output" + setsize + ".csv"), StandardCharsets.UTF_8)) {

            map.forEachEntry(new TObjectIntProcedure() {
                @Override
                public boolean execute(Object a, int b) {
                    try {
                        if ((b * numtran) >= minsup) {
                            writer.write(a + "," + (b * numtran));
                            writer.newLine();
                        }
                    } catch (IOException ignored) {
                    }
                    return true;
                }
            });
        } catch (Exception ignored) {

        }
        long stop = System.currentTimeMillis();
        System.out.printf("done: " + (stop - start) / 1000);
    }

    private static List<String> nsubsetgen(List<String> list, int n) {

        List<String> indexset = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            List<String> set = new ArrayList<>();
            set.add(list.get(i));
            gentree(i, list.get(i), n, set, list);
            indexset.addAll(set);
        }

        return indexset;
    }

    private static void gentree(int i, String lset, int k, List<String> set, List<String> list) {
        if (k > 1) {
            for (int j = 0; j < i; j++) {
                String lse = list.get(j) + "|" + lset;
                set.add(lse);
                if (j > 0)
                    gentree(j, lse, k - 1, set, list);
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private final static Text itemset = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            List<String> set = new ArrayList<String>();

            while (tokenizer.hasMoreTokens()) {
                set.add(tokenizer.nextToken());
            }

            List<String> subsets = nsubsetgen(set, setsize);
//            List<String> subsets = subsetgen(set);
            for (String subset : subsets) {
                itemset.set(subset);
                context.write(itemset, one);
            }
        }

        private List<String> nsubsetgen(List<String> list, int n) {

            List<String> indexset = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                ArrayList<String> set = new ArrayList<>();
                set.add(list.get(i));
                gentree(i, list.get(i), n, set, list);
                indexset.addAll(set);
            }

            return indexset;
        }

        private void gentree(int i, String lset, int k, List<String> set, List<String> list) {
            if (k > 1) {
                for (int j = 0; j < i; j++) {
                    String lse = list.get(j) + "|" + lset;
                    set.add(lse);
                    if (j > 0)
                        gentree(j, lse, k - 1, set, list);
                }
            }
        }

    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            final double numtran = 1.0 / 990002.0;
            final double minsup = 0.01;
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            if ((sum * numtran) >= minsup)
                context.write(key, new DoubleWritable(sum * numtran));
        }
    }

/*
    public static ArrayList<String> subsetgen(List<String> list) {

        ArrayList<String> subsets = new ArrayList<String>();

        int size = (int) Math.pow(2, list.size());
        for (int i = 1; i < size; i++) {
            int max = (int) Math.floor(Math.log(i) / Math.log(2)) + 1;

            BitSet bitSet = getBitSet(i, max);
//            if (bitSet.cardinality() > 4) {
//                continue;
//            }
            StringBuilder buff = new StringBuilder();

            for (int j = 0; j < max; j++) {
                if (bitSet.get(j)) {
                    buff.append(" ").append(list.get(j));
                }
            }

            subsets.add(buff.toString().replaceFirst(" ", ""));
        }


        return subsets;
    }

    private static BitSet getBitSet(int value, int size) {
        BitSet bits = new BitSet(size);
        bits.set(0, size - 1, false);
        int index = 0;
        while (value != 0) {
            if (value % 2 != 0) {
                bits.set(index);
            }
            ++index;
            value = value >>> 1;
        }
        return bits;
    }
*/
}
