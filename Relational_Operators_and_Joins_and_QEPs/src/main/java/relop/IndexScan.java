package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import global.RID;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
    private HashIndex hash_index;
    private HeapFile heap_file;
    private RID rid;
    private BucketScan bucket_scan;
    private boolean is_open;

    /**
     * Constructs an index scan, given the hash index and schema.
     */
    public IndexScan(Schema schema, HashIndex index, HeapFile file) {
        // Initialize schema
        this.schema = schema;
        // Initialize hash index
        this.hash_index = index;
        // Initialize heap file
        this.heap_file = file;
        // Initialize bucket scan
        this.bucket_scan = hash_index.openScan();
        // Initialize rid
        this.rid = new RID();
        // Initialize open to true
        this.is_open = true;
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
        // Close the bucket scanner if it exists
        if (bucket_scan != null) {
            bucket_scan.close();
        }
        // Open the bucket scanner
        bucket_scan = hash_index.openScan();
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        // Return the value of open
        return is_open;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        // Close the bucket scanner and set open to false
        if (bucket_scan != null) {
            bucket_scan.close();
        }
        is_open = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        // Check if there is a next tuple
        boolean has_next = bucket_scan.hasNext();
        return has_next;
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        // If no next tuple, error
        if (!hasNext()) {
            throw new IllegalStateException("There are no more tuples.");
        } else {
            // Get rid of next tuple and apply schema
            rid = bucket_scan.getNext();
            byte[] byte_data = heap_file.selectRecord(rid);
            Tuple tuple = new Tuple(schema, byte_data);
            return tuple;
        }

    }

    /**
     * Gets the key of the last tuple returned.
     */
    public SearchKey getLastKey() {
        // Return the last key
        SearchKey key = bucket_scan.getLastKey();
        return key;
    }

    /**
     * Returns the hash value for the bucket containing the next tuple, or maximum
     * number of buckets if none.
     */
    public int getNextHash() {
        // return the next hash
        int hash = bucket_scan.getNextHash();
        return hash;
    }

} // public class IndexScan extends Iterator
