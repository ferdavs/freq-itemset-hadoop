package bigdata.team3;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.jdt.core.compiler.ITerminalSymbols;
import org.omg.CORBA.OBJ_ADAPTER;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * The class encapsulates an implementation of the Apriori algorithm
 * to compute frequent itemsets.
 * <p/>
 * Datasets contains integers (>=0) separated by spaces, one transaction by line, e.g.
 * 1 2 3
 * 0 9
 * 1 9
 * <p/>
 * Usage with the command line :
 * $ java mining.Apriori fileName support
 * $ java mining.Apriori /tmp/data.dat 0.8
 * $ java mining.Apriori /tmp/data.dat 0.8 > frequent-itemsets.txt
 * <p/>
 * Usage as library:
 *
 * @author Martin Monperrus, University of Darmstadt, 2010
 * @author Nathan Magnus and Su Yibin, under the supervision of Howard Hamilton, University of Regina, June 2009.
 * @copyright GNU General Public License v3
 * No reproduction in whole or part without maintaining this copyright notice
 * and imposing this condition on any subsequent users.
 */
public class Apriori extends Observable {

    /**
     * the list of current itemsets
     */
    private TObjectDoubleHashMap<Itemset> itemsets;
    private TObjectDoubleHashMap<Itemset> resultset;

    /**
     * total number of transactions in transaFile
     */
    private int numTransactions;
    /**
     * minimum support for a frequent itemset in percentage, e.g. 0.8
     */
    private double minSup;

    private StringBuilder stringBuilder;
    private int currentItemCount;

    public Apriori(double minSup) throws IOException {
        this.minSup = minSup;
        stringBuilder = new StringBuilder();
        itemsets = new TObjectDoubleHashMap<>();
        resultset = new TObjectDoubleHashMap<>();
    }

    public Apriori() {
        stringBuilder = new StringBuilder();
        itemsets = new TObjectDoubleHashMap<>();
        resultset = new TObjectDoubleHashMap<>();
    }

    public void addLine(String line) throws IOException {
        stringBuilder.append(line).append("\n");
//        Files.write(Paths.get("tmp.dsk"), (line + "\n").getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        numTransactions++;
    }

    public TObjectDoubleHashMap<Itemset> getResultset() {
        return resultset;
    }

    public int getNumTransactions() {
        return numTransactions;
    }

    /**
     * starts the algorithm after configuration
     *
     * @param context
     */
    public TObjectDoubleHashMap<Itemset> go(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException {
        configure();
        //start timer
        long start = System.currentTimeMillis();

        // first we generate the candidates of size 1
        createItemsetsOfSize1();
        int itemsetNumber = 1; //the current itemset being looked at
        int nbFrequentSets = 0;

        while (itemsets.size() > 0) {
            context.progress();
            calculateFrequentItemsets(context);

            if (itemsets.size() != 0) {
                nbFrequentSets += itemsets.size();
                log("Found " + itemsets.size() + " frequent itemsets of size " + itemsetNumber + " (with support " + (minSup * 100) + "%)");

                createNewItemsetsFromPreviousOnes(context);
            }

            itemsetNumber++;
        }

        //display the execution time
        long end = System.currentTimeMillis();
        log("Execution time is: " + ((double) (end - start) / 1000) + " seconds.");
        log("Found " + nbFrequentSets + " frequents sets for support " + (minSup * 100) + "% (absolute " + Math.round(numTransactions * minSup) + ")");
        log("Done");
//        Files.delete(Paths.get("tmp.dsk"));
        return resultset;
    }

    /**
     * outputs a message in Sys.err if not used as library
     */
    private void log(String message) {
        System.out.println(message);
    }

    private void configure() throws IOException {

        if (minSup > 1 || minSup < 0) throw new IOException("minSup: bad value");

        // going thourgh the file to compute numItems and  numTransactions
//        numItems = 0;
        numTransactions = 0;
        StringReader reader = new StringReader(stringBuilder.toString());
        BufferedReader data_in = new BufferedReader(reader);
//        BufferedReader data_in = Files.newBufferedReader(Paths.get("tmp.dsk"), Charset.defaultCharset());
        String line;
        while ((line = data_in.readLine()) != null) {
            if (line.matches("\\s*")) continue; // be friendly with empty lines
            numTransactions++;
        }
        data_in.close();

    }

    /**
     * puts in itemsets all sets of size 1,
     * i.e. all possibles items of the datasets
     */
    private void createItemsetsOfSize1() throws IOException {
        currentItemCount = 1;
        StringReader reader = new StringReader(stringBuilder.toString());
        BufferedReader data_in = new BufferedReader(reader);
//        BufferedReader data_in = Files.newBufferedReader(Paths.get("tmp.dsk"), Charset.defaultCharset());
        String line;
        while ((line = data_in.readLine()) != null) {
            if (line.matches("\\s*")) continue;
            String[] split = line.split(" ");
            for (String s : split) {
                Itemset itemset = new Itemset(s);
                if (itemsets.containsKey(itemset)) {
                    itemsets.increment(itemset);
                } else
                    itemsets.put(itemset, 1.0);
            }

        }
        data_in.close();
    }

    /**
     * if n is the size of the current itemsets,
     * generate all possible itemsets of size n+1 from pairs of current itemsets
     * replaces the itemsets of itemsets by the new ones
     *
     * @param context
     */
    private void createNewItemsetsFromPreviousOnes(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) {

        int currentSizeOfItemsets = currentItemCount;
        log("Creating itemsets of size " + (currentSizeOfItemsets + 1) + " based on " + itemsets.size() + " itemsets of size " + currentSizeOfItemsets);

        Itemset[] items = itemsets.keys(new Itemset[0]);

        // compare each pair of itemsets of size n-1
        for (int i = 0; i < items.length; i++) {
            int[] X = items[i].getItems();
            context.setStatus("status:" + System.currentTimeMillis());
            for (int j = i + 1; j < items.length; j++) {
                int[] Y = items[j].getItems();

                assert (X.length == Y.length);

                //make a string of the first n-2 tokens of the strings
                int[] newCand = new int[currentSizeOfItemsets + 1];
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
        currentItemCount++;

        log("Created " + itemsets.size() + " unique itemsets of size " + (currentSizeOfItemsets + 1));
    }

    public void setMinSup(double minSup) {
        this.minSup = minSup;
    }


    /**
     * passes through the data to measure the frequency of sets in {@link },
     * then filters thoses who are under the minimum support (minSup)
     *
     * @param context
     */
    private void calculateFrequentItemsets(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException {


        StringReader reader = new StringReader(stringBuilder.toString());
        BufferedReader data_in = new BufferedReader(reader);
//        BufferedReader data_in = Files.newBufferedReader(Paths.get("tmp.dsk"), Charset.defaultCharset());

        Itemset[] items = itemsets.keys(new Itemset[0]);

        // for each transaction
        if (currentItemCount > 1)
            for (int i = 0; i < numTransactions; i++) {

                String line = data_in.readLine();
                Itemset trans = new Itemset(line);


                // check each candidate
                for (int c = 0; c < items.length; c++) {
                    Itemset item = items[c];
                    if (trans.contains(item)) {
                        itemsets.increment(items[c]);
                    }

                    context.setStatus("status:" + System.currentTimeMillis());
                }

            }

        data_in.close();

        for (int i = 0; i < items.length; i++) {
            double sup = (itemsets.get(items[i]) / (double) (numTransactions));
            if (sup < minSup) {
                itemsets.remove(items[i]);
            } else
                itemsets.put(items[i], sup);
        }

        resultset.putAll(itemsets);
    }

}