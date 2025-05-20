
# Query Execution Plans: CreateIndex, DropIndex, Select, Insert

This contains Java classes that implement execution plans for various database operations. The operations covered include creating indexes, dropping indexes, selecting tuples, and inserting tuples. Each operation is encapsulated in its respective class.

## Author
Lili Brush

## Written Classes Overview

### 1. **CreateIndex.java**
The `CreateIndex` class provides the execution plan for creating a hash index on a specified column of a table.

**Key Features:**
- Validates that the specified index does not already exist.
- Ensures the table and column specified in the query are valid.
- Scans the table and populates the index with `(key, RID)` pairs.
- Registers the index in the system catalog.

**Main Methods:**
- `CreateIndex(AST_CreateIndex tree)`: Initializes and validates the plan.
- `execute()`: Executes the plan to create the index.
- `populateHashIndex(FileScan file_scan, HashIndex hash_index)`: Populates the hash index with entries.

---

### 2. **DropIndex.java**
The `DropIndex` class provides the execution plan for dropping an existing hash index.

**Key Features:**
- Validates that the specified index exists.
- Removes the index file and unregisters it from the system catalog.

**Main Methods:**
- `DropIndex(AST_DropIndex tree)`: Initializes and validates the plan.
- `execute()`: Executes the plan to drop the index.
- `validateIndexExists()`: Checks if the specified index exists.

---

### 3. **Select.java**
The `Select` class provides the execution plan for retrieving tuples from one or more tables based on specified conditions.

**Key Features:**
- Validates the existence of tables and predicates.
- Supports selection, projection, and join operations.
- Optionally enables selection pushdown optimization for improved performance.
- Outputs the selected tuples or explains the query plan.

**Main Methods:**
- `Select(AST_Select tree)`: Initializes and validates the plan.
- `execute()`: Executes the query plan and outputs the results.
- `buildQueryTree()`: Constructs the iterator query tree without pushdown optimization.
- `buildQueryTreeWithPushdown()`: Constructs the iterator query tree with pushdown optimization.
- `isSingleTablePredicate(Predicate[] predicates, Schema tableSchema)`: Validates if predicates belong to a single table.

---

### 4. **Insert.java**
The `Insert` class provides the execution plan for inserting tuples into a table.

**Key Features:**
- Validates that the table exists and that the values to be inserted match the table schema.
- Inserts the tuple into the table's heap file.
- Updates all associated indexes with the new tuple.

**Main Methods:**
- `Insert(AST_Insert tree)`: Initializes and validates the plan.
- `execute()`: Executes the plan to insert the tuple.
- `updateIndexes(Tuple tuple, RID tuple_rid)`: Updates all indexes associated with the table.
