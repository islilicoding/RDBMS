package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayDeque;
import java.util.Queue;

public class HashJoin extends Iterator {

    // Column iterators
    Iterator left_iterator;
    Iterator right_iterator;

    // Column integers
    Integer left_column;
    Integer right_column;

    // Hash table
    HashTableDup hash_table;

    // Queue for tuples
    private Queue<Tuple> tuple_queue;

    public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2) {
        // Initialize iterators
        this.left_iterator = aIter1;
        this.right_iterator = aIter2;
        // Initialize columns
        this.left_column = aJoinCol1;
        this.right_column = aJoinCol2;
        // Initialize tuple queue
        this.tuple_queue = new ArrayDeque<Tuple>();
        // Initialize hash table
        this.hash_table = new HashTableDup();
        while(this.left_iterator.hasNext()) {
            // Get next tuple
            Tuple tuple = this.left_iterator.getNext();
            // Make search key for the tuple in the hash table
            SearchKey search_key = new SearchKey(tuple.getField(aJoinCol1));
            // Add to hash table
            this.hash_table.add(search_key, tuple);
        }
        this.left_iterator.close();
        // Initialize schema
        this.schema = Schema.join(this.left_iterator.schema, this.right_iterator.schema);
    }

    @Override
    public void explain(int depth) {
        throw new UnsupportedOperationException("Not implemented");
        //TODO: Your code here
    }

    @Override
    public void restart() {
        // Restart both iterators
        this.left_iterator.restart();
        this.right_iterator.restart();
    }

    @Override
    public boolean isOpen() {
        boolean is_open = this.right_iterator.isOpen();
        return is_open;
    }

    @Override
    public void close() {
        // Close both iterators
        this.left_iterator.close();
        this.right_iterator.close();
    }

    @Override
    public boolean hasNext() {
        boolean has_next = false;
        // Check if the queue is not empty
        if (!this.tuple_queue.isEmpty()) {
            has_next = true;
        }

        // If not, use right iterator to find matching tuples
        while (this.right_iterator.hasNext()) {
            // Get next tuple, create search key, and find matches
            Tuple next_tuple = this.right_iterator.getNext();
            SearchKey search_key = new SearchKey(next_tuple.getField(this.right_column));
            Tuple[] hash_table_matches = this.hash_table.getAll(search_key);
            for (Tuple match : hash_table_matches) {
                Tuple joined_tuple = Tuple.join(match, next_tuple, this.schema);
                this.tuple_queue.add(joined_tuple);
            }
            // Check new tuples were added
            if (!this.tuple_queue.isEmpty()) {
                has_next = true;
            }
        }
        return has_next;
    }

    @Override
    public Tuple getNext() {
        // If there is a next tuple, remove and return it. Otherwise, error.
        if (this.hasNext()) {
            Tuple tuple = this.tuple_queue.remove();
            return tuple;
        } else {
            throw new IllegalStateException("There are no more tuples in the queue.");
        }
    }
} // end class HashJoin;
