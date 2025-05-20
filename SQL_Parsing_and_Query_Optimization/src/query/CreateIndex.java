package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {
    private String index_file_name;
    private String table_name;
    private String column_name;
    private Schema table_schema;

    /**
     * Initializes the plan based on the given parsed query.
     * 
     * @throws QueryException if the index already exists or if the table/column is invalid
     */
    public CreateIndex(AST_CreateIndex tree) throws QueryException {

        // Get index file name from function input, verify it doesn't already exists
        this.index_file_name = tree.getFileName();
        QueryCheck.fileNotExists(index_file_name);

        // Get table and column name from function input
        this.table_name = tree.getIxTable(); 
        this.column_name = tree.getIxColumn();

        // Validate the table and column
        this.table_schema = QueryCheck.tableExists(this.table_name);
        QueryCheck.columnExists(this.table_schema, this.column_name);

        // Check if the index already exists
        if (doesIndexExist(this.index_file_name)) {
            throw new QueryException("Index already exists");
        }

    } // public CreateIndex(AST_CreateIndex parsed_tree) throws QueryException

    /**
     * Helper function to check if an index already exists.
     */
    private boolean doesIndexExist(String indexFileName) {
        try {
            QueryCheck.indexExists(indexFileName);
            return true; // Index exists
        } catch (QueryException e) {
            return false; // Index does not exist
        }
    }

    /**
     * Executes the plan and outputs the result.
     */
    public void execute() {

        // Initialize required structures: heap, file scanner, and hash index
        HeapFile heap_file = new HeapFile(this.table_name);
        FileScan file_scan = new FileScan(this.table_schema, heap_file);
        HashIndex hash_index = new HashIndex(this.index_file_name);

        // Populate the hash index with (key, rid) pairs
        populateHashIndex(file_scan, hash_index);

        // Close the file scan
        file_scan.close();

        // Register the index in the system catalog
        Minibase.SystemCatalog.createIndex(this.index_file_name, this.table_name, this.column_name);

        System.out.println("Index created");

    } // public void execute()

    /**
     * Populates the hash index with (key, rid) pairs from the file scan.
     */
    private void populateHashIndex(FileScan file_scan, HashIndex hash_index) {
        int column_field_number = this.table_schema.fieldNumber(this.column_name);

        while (file_scan.hasNext()) {
            Tuple current_tuple = file_scan.getNext();
            SearchKey search_key = new SearchKey(current_tuple.getField(column_field_number));
            RID current_rid = file_scan.getLastRID();
            hash_index.insertEntry(search_key, current_rid);
        }
    }

} // class CreateIndex implements Plan
