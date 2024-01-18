package relop;

import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

    private HeapFile file = null;
    private HashIndex index = null;
    private BucketScan scan = null;
    private boolean isOpen;

    /**
     * Constructs an index scan, given the hash index and schema.
     */
    public IndexScan(Schema schema, HashIndex index, HeapFile file) {
        // Your code here
        this.schema = schema;
        this.file = file;
        this.index = index;
        this.scan = index.openScan();
        isOpen = true;
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        // Your code here
        scan.close();
        scan = index.openScan();
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        // Your code here
        return isOpen;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        // Your code here
        scan.close();
        isOpen = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        // Your code here
        return scan.hasNext();
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        // Your code here
        return new Tuple(schema, file.selectRecord(scan.getNext()));
    }

    /**
     * Gets the key of the last tuple returned.
     */
    public SearchKey getLastKey() {
        // Your code here
        return scan.getLastKey();
    }

    /**
     * Returns the hash value for the bucket containing the next tuple, or maximum
     * number of buckets if none.
     */
    public int getNextHash() {
        // Your code here
        return scan.getNextHash();
    }

} // public class IndexScan extends Iterator

