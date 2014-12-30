package bigdata.team3;

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

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String rootout = args[1];

        for (int i = 1; i < 6; i++) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            conf.setInt("phaseCounter", i);

            final String NAME_NODE = "hdfs://SrvT2C2Master:8020";
            DistributedCache.addCacheFile(new URI(NAME_NODE + "/user/team3/orinput2/kosarak.dat"), conf);

            Job job = Job.getInstance(conf, "frequent itemset k-phase :" + i);
            job.setJarByClass(Main2.class);
            job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
            job.setReducerClass(Main.Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setNumReduceTasks(3);


            FileInputFormat.addInputPath(job, new Path(input));

            Path path = new Path(rootout + "/out" + i);
            fs.delete(path, true);

            FileOutputFormat.setOutputPath(job, path);

            if (!job.waitForCompletion(true)) {
                break;
            }

            input = rootout + "/out" + i;
        }

//        local();

//        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        List<String> candidate = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            candidate.add(i + "");
//        }
//
//        for (int i = 0; i < 5; i++) {
//            candidate = generateCandidates(candidate, i + 2);
//
//        }
    }

    private static void local() throws IOException {
        long start = System.currentTimeMillis();

        String path = "C:\\Users\\F\\Desktop\\retail.dat";
        ArrayList<HashSet<String>> trans = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {

            String line;
            while ((line = reader.readLine()) != null) {
                trans.add(new HashSet<>(Arrays.asList(line.split("\\s+"))));
            }
        } catch (Exception ignored) {

        }

        String out1 = "C:\\Users\\F\\Dropbox\\class\\big data\\Project\\code";
        List<String> lines = Files.readAllLines(Paths.get(out1, "output21.csv"), StandardCharsets.UTF_8);
        final double numtran = 1.0 / (1.0 * trans.size());
        final double minsup = 0.01;

        for (int i = 2; i < 6; i++) {
            TObjectIntHashMap map = new TObjectIntHashMap();
            java.nio.file.Path out = Paths.get(out1, "output2" + i + ".csv");
            ArrayList<String> list = new ArrayList<>();
            for (String s : lines) {
                list.add(s.split("\\s+|,")[0]);
            }
            List<String> candidates = generateCandidates(list, i);
            for (String s : candidates) {
                List<String> split = Arrays.asList(s.split("[|]"));
                for (HashSet<String> tran : trans) {
                    if (tran.containsAll(split)) {
                        if (!map.containsKey(s))
                            map.put(s, 1);
                        else
                            map.increment(s);
                    }
                }
            }

            try (final BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {

                map.forEachEntry(new TObjectIntProcedure() {
                    @Override
                    public boolean execute(Object a, int b) {
                        try {
                            if ((b * numtran) >= minsup) {
                                writer.write(a + "\t" + (b * numtran));
                                writer.newLine();
                            }
                        } catch (IOException ignored) {
                            return false;
                        }
                        return true;
                    }
                });
            } catch (Exception ignored) {

            }

            lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        }
        long stop = System.currentTimeMillis();
        System.out.printf("done: " + (stop - start) / 1000);

    }

    private static List<String> generateCandidates(List<String> candidates, int n) {
        List<String> tempCandidates = new ArrayList<>(); //temporary candidate string vector
        List<String> tempCandidates2 = new ArrayList<>(); //temporary candidate string vector

        String str1, str2; //strings that will be used for comparisons
        StringTokenizer st1, st2; //string tokenizers for the two itemsets being compared

        //if its the first set, candidates are just the numbers
        if (n == 1) {
            for (int i = 1; i <= candidates.size(); i++) {
                tempCandidates.add(candidates.get(i));
            }
            return tempCandidates;
        } else if (n == 2) //second itemset is just all combinations of itemset 1
        {
            //add each itemset from the previous frequent itemsets together
            for (int i = 0; i < candidates.size(); i++) {
                st1 = new StringTokenizer(candidates.get(i));
                str1 = st1.nextToken();
                for (int j = i + 1; j < candidates.size(); j++) {
                    st2 = new StringTokenizer(candidates.get(j));
                    str2 = st2.nextToken();
                    tempCandidates.add(str1 + "|" + str2);
                }
            }
        } else {
            //for each itemset
            for (int i = 0; i < candidates.size(); i++) {
                //compare to the next itemset
                for (int j = i + 1; j < candidates.size(); j++) {
                    //create the strigns
                    str1 = "";
                    str2 = "";
                    //create the tokenizers
                    st1 = new StringTokenizer(candidates.get(i), "| ");
                    st2 = new StringTokenizer(candidates.get(j), "| ");

                    //make a string of the first n-2 tokens of the strings
                    for (int s = 0; s < n - 2; s++) {
                        str1 = str1 + " " + st1.nextToken();
                        str2 = str2 + " " + st2.nextToken();
                    }

                    //if they have the same n-2 tokens, add them together
                    if (str2.compareToIgnoreCase(str1) == 0)
                        tempCandidates.add((str1 + "|" + st1.nextToken() + "|" + st2.nextToken()).trim().replaceAll("\\s+", "|"));
                }
            }
        }
        for (String tempCandidate : tempCandidates) {
            String[] split = tempCandidate.split("[|]");
            Arrays.sort(split, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return compareNatural(o1, o2);
                }
            });
            StringWriter swrite = new StringWriter();
            for (String s : split) {
                swrite.append('|').append(s);
            }
            tempCandidates2.add(swrite.toString().replaceFirst("[|]", ""));
        }
        return tempCandidates2;
    }

    private static int compareNatural(String s1, String s2) {
        // Skip all identical characters
        int len1 = s1.length();
        int len2 = s2.length();
        int i;
        char c1, c2;
        for (i = 0, c1 = 0, c2 = 0; (i < len1) && (i < len2) && (c1 = s1.charAt(i)) == (c2 = s2.charAt(i)); i++) ;

        // Check end of string
        if (c1 == c2)
            return (len1 - len2);

        // Check digit in first string
        if (Character.isDigit(c1)) {
            // Check digit only in first string
            if (!Character.isDigit(c2))
                return (1);

            // Scan all integer digits
            int x1, x2;
            for (x1 = i + 1; (x1 < len1) && Character.isDigit(s1.charAt(x1)); x1++) ;
            for (x2 = i + 1; (x2 < len2) && Character.isDigit(s2.charAt(x2)); x2++) ;

            // Longer integer wins, first digit otherwise
            return (x2 == x1 ? c1 - c2 : x1 - x2);
        }

        // Check digit only in second string
        if (Character.isDigit(c2))
            return (-1);

        // No digits
        return (c1 - c2);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Text item = new Text();
        ArrayList<HashSet<String>> trans = new ArrayList<>();
        List<String> itemset = new ArrayList<>();

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
                            trans.add(new HashSet<>(Arrays.asList(line.split("\\s+"))));
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
                itemset.add(split[0].trim());
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
            int iter = Integer.parseInt(context.getJobName().split(":")[1]);
            if (iter > 1) {
                List<String> candidateSet = generateCandidates(itemset, iter);
                for (String s : candidateSet) {
                    List<String> split = Arrays.asList(s.split("[|]"));
                    for (HashSet<String> tran : trans) {
                        if (tran.containsAll(split)) {
                            item.set(s);
                            context.write(item, one);
                        }
                    }
                }
            }
            super.cleanup(context);
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
            if (sum * numtran >= minsup)
                context.write(key, new DoubleWritable(sum * numtran));

        }
    }

}
