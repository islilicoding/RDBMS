package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

  // File name for the index to be dropped
  private String tree_file;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {
    // Get file name from function input
    this.tree_file = tree.getFileName();
    boolean exists = validateIndexExists();
    if (!exists) {
      throw new QueryException("Index '" + this.tree_file + "' does not exist.");
    }
  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Validates that the index exists; throws an exception if it doesn't.
   */
  private boolean validateIndexExists() {
    try {
      QueryCheck.indexExists(this.tree_file);
    } catch (QueryException e) {
      return false;
    }
    return true;
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // Delete the index file
    HashIndex hashIndex = new HashIndex(this.tree_file);
    hashIndex.deleteFile();

    // Drop from system catalog
    Minibase.SystemCatalog.dropIndex(this.tree_file);

    System.out.println("Index: " + this.tree_file + " dropped");
  } // public void execute()

} // class DropIndex implements Plan
