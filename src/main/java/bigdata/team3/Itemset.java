package bigdata.team3;

import java.util.Arrays;

public class Itemset {
    private int[] items;
    private int hash;
    private int count;

    public Itemset(String line) {
        parseItems(line);
        count = 0;
    }

    public Itemset(int[] newCand) {
        setItems(newCand);
    }

    public int increment() {
        count++;
        return count;
    }

    public int getCount() {
        return count;
    }

    public void parseItems(String line) {

        String[] split = line.split("[|,;\\s+]");

        items = new int[split.length];
        for (int i = 0; i < split.length; i++) {
            items[i] = Integer.parseInt(split[i]);
        }
        Arrays.sort(items);
        hash = Arrays.hashCode(items);
    }

    public int[] getItems() {
        return items;
    }

    public void setItems(int[] items) {
        Arrays.sort(items);
        this.items = items.clone();
        hash = Arrays.hashCode(items);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Itemset) {
            Itemset i = (Itemset) obj;
            return Arrays.equals(i.items, items);
        } else return false;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public boolean contains(Itemset item) {
        for (int i = 0; i < item.items.length; i++) {
            if (Arrays.binarySearch(items, item.items[i]) < 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(items).replaceAll("]|\\[| ", "").replaceAll("[,]", "|");
    }
}
