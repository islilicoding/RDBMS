package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
    private HeapFile heap_file;
    private boolean is_open;
    private HashScan hash_scan;
    private SearchKey search_key;
    private HashIndex hash_index;

    /**
     * Constructs an index scan, given the hash index and schema.
     */
    public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
        // Initalize schema
        this.schema = aSchema;
        // Initalize hash index
        this.hash_index = aIndex;
        // Initialize search key
        this.search_key = aKey;
        // Initalize heap file
        this.heap_file = aFile;
        // Initialize is open
        this.is_open = true;
        // Initialize hash scan
        this.hash_scan = hash_index.openScan(search_key);
    }

    /**
     * Gives a one-line explanation of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        // Close hash scanner and recreate it, setting open to true
        hash_scan.close();
        hash_scan = hash_index.openScan(search_key);
        is_open = true;
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        // return open's value
        return is_open;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        // Close hash scanner and set open to false
        hash_scan.close();
        is_open = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        // Return if there is a next tuple
        boolean has_next = hash_scan.hasNext();
        return has_next;
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        // Get rid and record data
        RID rid = hash_scan.getNext();
        byte[] byte_data = heap_file.selectRecord(rid);

        // Check if data is empty,
        if (byte_data == null) {
            throw new IllegalStateException("There are no more tuples.");
        } else {
            // return tuple with new schema
            Tuple next = new Tuple(schema, byte_data);
            return next;
        }
    }

} // public class KeyScan extends Iterator
