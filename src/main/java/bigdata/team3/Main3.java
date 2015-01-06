package bigdata.team3;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.procedure.TObjectDoubleProcedure;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main3 {

    static double tranSize = 1692082;

    public static void main(String[] args) throws Exception {


        final String NAME_NODE = "hdfs://SrvT2C2Master:8020";
        String input = args[0];
        String rootout = args[1];
        tranSize = Double.parseDouble(args[2]);
        Configuration conf = new Configuration();

        for (int i = 1; i < 3; i++) {
            long milliSeconds = 1000 * 60 * 120;
            conf.setLong("mapred.task.timeout", milliSeconds);

            FileSystem fs = FileSystem.get(conf);

            Job job = Job.getInstance(conf, "frequent item SON:" + i);
            job.setJarByClass(Main.class);
            job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setNumReduceTasks(4);

            FileInputFormat.addInputPath(job, new Path(input));

            Path path = new Path(rootout + "/out" + i);
            fs.delete(path, true);

            FileOutputFormat.setOutputPath(job, path);

            if (!job.waitForCompletion(true)) {
                break;
            }

            DistributedCache.addCacheFile(new URI(NAME_NODE + rootout + "/out" + i + "/part-r-00000"), conf);
            DistributedCache.addCacheFile(new URI(NAME_NODE + rootout + "/out" + i + "/part-r-00001"), conf);
            DistributedCache.addCacheFile(new URI(NAME_NODE + rootout + "/out" + i + "/part-r-00002"), conf);
            DistributedCache.addCacheFile(new URI(NAME_NODE + rootout + "/out" + i + "/part-r-00003"), conf);
        }

//        local();
    }

    private static void local() throws Exception {
        final double minsup = 0.01;
        final double numtran = 1.0 / tranSize;
        ExecutorService exe = Executors.newFixedThreadPool(8);
        for (int i = 1; i < 9; i++) {
            final int finalI = i;
            exe.execute(new Runnable() {
                @Override
                public void run() {


                    Apriori ap = new Apriori();

                    try (BufferedReader reader = Files.newBufferedReader(Paths.get("C:\\Users\\F\\Desktop\\disk" + finalI + ".gsd"), StandardCharsets.UTF_8)) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            ap.addLine(line);
                        }
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }

                    double p = (5.0 * ap.getNumTransactions()) / tranSize;
                    ap.setMinSup(p * minsup);

                    try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get("output3" + finalI + ".csv"), StandardCharsets.UTF_8)) {
                        TObjectDoubleHashMap<Itemset> result = ap.go(null);
                        result.forEachEntry(new TObjectDoubleProcedure<Itemset>() {
                            @Override
                            public boolean execute(Itemset a, double b) {
                                try {
                                    writer.write(a + "\t" + b);
                                    writer.newLine();
                                } catch (IOException ignored) {
                                    return false;
                                }
                                return true;
                            }
                        });
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }
                }
            });
        }
        exe.shutdown();
        exe.awaitTermination(1, TimeUnit.DAYS);

        List<String> items = Files.readAllLines(Paths.get("C:\\Users\\F\\Desktop\\retail.dat"), StandardCharsets.UTF_8);
        HashSet<Itemset> candidates = new HashSet<>();
        for (int i = 1; i < 9; i++) {
            List<String> strings = Files.readAllLines(Paths.get("output3" + i + ".csv"), StandardCharsets.UTF_8);
            for (String string : strings) {
                candidates.add(new Itemset(string.split("\\s+")[0]));
            }
        }

        for (String item : items) {
            Itemset itemset = new Itemset(item);
            for (Itemset candidate : candidates) {
                if (itemset.contains(candidate)) {
                    candidate.increment();
                }
            }
        }

        try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get("output3N.csv"), StandardCharsets.UTF_8)) {
            for (Itemset candidate : candidates) {
                if (candidate.getCount() * numtran >= minsup) {
                    writer.write(candidate.toString());
                    writer.write("\t");
                    writer.write((candidate.getCount() * numtran) + "");
                    writer.newLine();
                }
            }
        } catch (Exception ignored) {

        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final double minsup = 0.01;
        private Apriori apriori;
        private HashSet<Itemset> candidateItems = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (context.getJobName().endsWith("1")) {
                apriori = new Apriori();
            } else {
                URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile);
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    FSDataInputStream open = new FSDataInputStream(fs.open(path));
                    BufferedReader d = new BufferedReader(new InputStreamReader(open));
                    String line;
                    while ((line = d.readLine()) != null) {
                        candidateItems.add(new Itemset(line.split("\\s+")[0]));
                    }
                    d.close();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (context.getJobName().endsWith("1")) {
                apriori.addLine(line);
            } else {
                Itemset itemset = new Itemset(line);

                for (Itemset candidateItem : candidateItems) {
                    if (itemset.contains(candidateItem)) {
                        candidateItem.increment();
                    }
                }
            }
        }

        @Override
        protected void cleanup(final Context context) throws IOException, InterruptedException {
            final DoubleWritable rval = new DoubleWritable();
            final Text rkey = new Text();

            if (context.getJobName().endsWith("1")) {
                double p = (20.0 * apriori.getNumTransactions()) / tranSize;

                apriori.setMinSup(p * minsup);

                TObjectDoubleHashMap<Itemset> localResult = apriori.go(context);

                Itemset[] keys = localResult.keys(new Itemset[0]);

                for (Itemset key : keys) {
                    rkey.set(key.toString());
                    rval.set(localResult.get(key));
                    context.write(rkey, rval);
                }
            } else {
                for (Itemset candidateItem : candidateItems) {
                    rkey.set(candidateItem.toString());
                    rval.set(candidateItem.getCount());
                    context.write(rkey, rval);
                }
            }

            super.cleanup(context);
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            if (context.getJobName().endsWith("1")) {
                context.write(key, new DoubleWritable(-1.0));
            } else {
                final double minsup = 0.01;
                final double numtran = 1.0 / tranSize;

                double sum = 0.0;
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
                double sup = sum * numtran;
                if (sup >= minsup)
                    context.write(key, new DoubleWritable(sup));
            }
        }
    }

}
