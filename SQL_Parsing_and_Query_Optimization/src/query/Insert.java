package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  // Variables for within this class
  private String table_name;
  private Object[] values;
  private Schema table_schema;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {

    // Initialize class variables from function input
    this.table_name = tree.getFileName();
    this.values = tree.getValues();
    this.table_schema = Minibase.SystemCatalog.getSchema(this.table_name);

    // Validate table existence and value constraints
    QueryCheck.tableExists(this.table_name);
    QueryCheck.insertValues(this.table_schema, this.values);

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // Create a HeapFile and Tuple instances
    HeapFile heap_file = new HeapFile(this.table_name);
    Tuple tuple = new Tuple(this.table_schema, this.values);

    // Insert tuple into HeapFile, get RID
    RID tuple_rid = heap_file.insertRecord(tuple.getData());

    // Update the indexes for the inserted tuple
    updateIndexes(tuple, tuple_rid);

    System.out.println("1 row affected. (Table: " + table_name + ")");

  } // public void execute()

  /**
   * Updates all associated indexes for the given tuple and RID.
   */
  private void updateIndexes(Tuple tuple, RID tuple_rid) {
    // Retrieve all associated indexes for the table from the system catalog
    IndexDesc[] table_indexes = Minibase.SystemCatalog.getIndexes(this.table_name);

    // Iterate over each index, update it with new tuple's entry
    for (IndexDesc index_desc : table_indexes) {
      HashIndex hash_index = new HashIndex(index_desc.indexName);
      SearchKey search_key = new SearchKey(tuple.getField(index_desc.columnName));
      hash_index.insertEntry(search_key, tuple_rid);
    }
  }

} // class Insert implements Plan
