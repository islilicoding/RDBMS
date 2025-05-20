package relop;


/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
    private boolean is_open;
    private Iterator projection_iterator;
    private Integer[] projection_fields;
    private Schema projection_schema;



    /**
     * Constructs a projection, given the underlying iterator and field numbers.
     */
    public Projection(Iterator aIter, Integer... aFields) {
        // Initalize open
        this.is_open = true;
        // Initialize iterator
        this.projection_iterator = aIter;
        // Initialize fields
        this.projection_fields = aFields;
        // Initalize projection schema and schema
        this.projection_schema = new Schema(aFields.length);
        for (int i = 0; i < projection_fields.length; i++) {
            this.projection_schema.initField(i, this.projection_iterator.schema, this.projection_fields[i]);
        }
        this.schema = this.projection_schema;
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
        // Set open to true
        is_open = true;
        // Restart iterator
        projection_iterator.restart();
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        return is_open;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        // Set open to false
        is_open = false;
        // Close iterator
        this.projection_iterator.close();
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        boolean has_next = false;
        // Check if the iterator is open
        if (this.is_open) {
            // Check if there are more elements
            has_next = this.projection_iterator.hasNext();
        }
        return has_next;
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        // Make new tuple for projection
        Tuple projected = new Tuple(this.projection_schema);
        // Get next tuple from iterator
        Tuple next = this.projection_iterator.getNext();
        // Meke new tuple with projection schema
        for (int i = 0; i < projection_fields.length; i++) {
            projected.setField(i, next.getField(projection_fields[i]));
        }
        // If projection successful, return tuple
        if (projected.getAllFields().length > 0) {
            return projected;
        } else {
            throw new IllegalStateException("There are no more tuples.");
        }
    }

} // public class Projection extends Iterator
