package query;

import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  // Variables for the class
  private boolean explain_query;
  private Predicate[][] tree_predicates;

  private String[] tables;
  private String[] tree_columns;

  private Schema[] table_schemas;
  private Schema schema;

  private Integer[] tree_field_numbers;
  private Iterator query_tree;

  protected boolean pushdown_enabled = false;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    // Intialize table from function input
    this.tables = tree.getTables();

    // Build the schema from the function input and tables
    this.table_schemas = new Schema[this.tables.length];
    this.schema = null;
    for (int i = 0; i < this.tables.length; i++) {
      if (this.schema == null) {
        this.schema = QueryCheck.tableExists(this.tables[i]);
        this.table_schemas[i] = this.schema;
      } else {
        this.table_schemas[i] = QueryCheck.tableExists(this.tables[i]);
        this.schema = Schema.join(this.schema, this.table_schemas[i]);
      }
    }

    // Set explian flag and predicates
    this.explain_query = tree.isExplain;
    tree_predicates = tree.getPredicates();
    QueryCheck.predicates(this.schema, tree_predicates);

    // Build the tree columns and field numbers from the function input 
    tree_columns = tree.getColumns();
    tree_field_numbers = new Integer[tree_columns.length];
    for (int i = 0; i < tree_columns.length; i++) {
      tree_field_numbers[i] = QueryCheck.columnExists(this.schema, tree_columns[i]);
    }

    // Build the iterator query tree
    if (pushdown_enabled) {
      buildQueryTreeWithPushdown();
    } else {
      buildQueryTree();
    }
  }

  /**
   * Builds the iterator query tree without pushdown optimization.
   */
  private void buildQueryTree() {
    Iterator iter = null;

    // Iterate through all tables, update schema with joins
    for (int i = 0; i < this.tables.length; i++) {
      if (iter == null) {
        iter = new FileScan(this.table_schemas[i], new HeapFile(this.tables[i]));
      } else {
        Iterator currSchemaIter = new FileScan(this.table_schemas[i], new HeapFile(this.tables[i]));
        iter = new SimpleJoin(iter, currSchemaIter);
      }
    }

    // Selections
    for (Predicate[] pred : tree_predicates) {
      iter = new Selection(iter, pred);
    }

    // Projection
    if (tree_columns.length > 0) {
      iter = new Projection(iter, tree_field_numbers);
    }

    // Store iterator query tree
    this.query_tree = iter;
  }

  /**
   * Builds the iterator query tree with selection pushdown optimization.
   */
  private void buildQueryTreeWithPushdown() {
    Iterator[] tableIterators = new Iterator[this.tables.length];

    // List of pushed-down predicates
    List<Predicate[]> pushedDownPredicates = new ArrayList<>();

    // Pushdown for each table
    for (int i = 0; i < this.tables.length; i++) {
      tableIterators[i] = new FileScan(this.table_schemas[i], new HeapFile(this.tables[i]));

      // Try to push down the first single-table predicate array for this table
      for (Predicate[] pred : tree_predicates) {
        if (!pushedDownPredicates.contains(pred) && isSingleTablePredicate(pred, this.table_schemas[i])) {
          tableIterators[i] = new Selection(tableIterators[i], pred);

          // Mark predicate as pushed down
          pushedDownPredicates.add(pred);

          break;
        }
      }
    }

    // Join tables
    Iterator iter = tableIterators[0];
    for (int i = 1; i < tableIterators.length; i++) {
      iter = new SimpleJoin(iter, tableIterators[i]);
    }

    // Remaining selections
    for (Predicate[] pred : tree_predicates) {
      if (!pushedDownPredicates.contains(pred)) { // Apply only non-pushed-down predicates
        iter = new Selection(iter, pred);
      }
    }

    // Projection
    if (tree_columns.length > 0) {
      iter = new Projection(iter, tree_field_numbers);
    }

    // Store iterator query tree
    this.query_tree = iter;
  }

  /**
   * Checks if a predicate array involves only a single table.
   */
  private boolean isSingleTablePredicate(Predicate[] predicates, Schema tableSchema) {
    for (Predicate predicate : predicates) {
      if (!predicate.validate(tableSchema)) {
        // Predicate does not belong to table
        return false;
      }
    }
    // All predicates belong to one table
    return true;
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // Exit early if the query tree is null
    if (query_tree == null) {
      return;
    }

    // Explain the query tree and execute it
    if (this.explain_query) {
        query_tree.explain(0); 
        query_tree.execute();
        System.out.println("Select plan explained and executed!");
    } else {
        // Execute the query tree and capture the affected row count
        int rowCnt = query_tree.execute(); 
        System.out.println(rowCnt + " rows affected");
    }

  } // public void execute()

} // class Select implements Plan

