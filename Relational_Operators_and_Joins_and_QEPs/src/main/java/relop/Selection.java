package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
    // Variables to initalize in the constructor
    private Tuple next;
    private boolean is_open;
    private Iterator selection_iterator;
    private Predicate[] selection_predicates;

    /**
     * Constructs a selection, given the underlying iterator and predicates.
     */
    public Selection(Iterator aIter, Predicate... aPreds) {
        // Initialize iterator
        this.selection_iterator = aIter;
        // Initalize predicates
        this.selection_predicates = aPreds;
        // Initalize is open to true
        this.is_open = true;
        // Initalize next to nothing
        this.next = null;
        // Initalize schema
        this.schema = aIter.getSchema();
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
        // Restart iterator
        selection_iterator.restart();
        // Set the next tuple to nothing
        next = null;
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        // Return value of is_open
        return is_open;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        // Close iterator
        selection_iterator.close();
        // Set open to false
        is_open = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        boolean next_tuple = false;

        // Check if a next tuple is already stored
        if (next != null) {
            next_tuple = true;
        } else {

            // Consider remaining tuples in the iterator
            boolean has_next = selection_iterator.hasNext();
            while (has_next) {
                Tuple candidate = selection_iterator.getNext();
                for (int i = 0; i < selection_predicates.length; i++) {
                    // Check if tuple satisfies predicate
                    Predicate predicate = selection_predicates[i];
                    boolean is_match = predicate.evaluate(candidate);

                    // If the predicate is satisfied store the valid tuple
                    if (is_match) {
                        next = candidate;
                        next_tuple = true;
                        has_next = false;
                    }
                }
                if (has_next == true) {
                    has_next = selection_iterator.hasNext();
                }
            }
        }
        return next_tuple;
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        if (hasNext()) {
            // Return the tuple and reset the stored tuple to nothing
            Tuple next_tuple = next;
            next = null;
            return next_tuple;
        } else {
            throw new IllegalStateException("There are no more tuples.");
        }
    }

} // public class Selection extends Iterator
