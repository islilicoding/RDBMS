# HeapFile

## Group Members
- Lilianne Brush
- Noa Shoval
- Aaron Neman

### Contributions
- **Part 2 written by:** Aaron Neman
- **Part 3 written by:** Noa Shoval

## Overview
The HeapFile and HeapScan classes implement a heap file system for storing and retrieving unordered records in a database. The HeapFile class provides operations for inserting, selecting, updating, and deleting records, while the HeapScan class supports efficient iteration over records in the heap file
## Key Components of Part 2: Written in HeapFile.java

### 1. **Data Structures**
- `List<PageId> pageIdList`: Stores the ordered list of pages in the heap file.
- `TreeMap<Integer, List<PageId>> freeSpaceMap`: A reverse-ordered TreeMap that tracks available space on pages.
- `int recCount`: Stores the total number of records in the heap file.

### 2. **Key Methods**
- `Heapfile(String name)`
  - Opens an existing heap file or creates a new one.
  - Loads pages into `pageIdList` and tracks free space in `freeSpaceMap`.
- `insertRecord(byte[] record)`
  - Inserts a new record into the heap file.
  - Finds an appropriate page using `freeSpaceMap` or allocates a new page.
- `selectRecord(RID rid)`
  - Retrieves a record by its RID
- `updateRecord(RID rid, Tuple newRecord)`
  - Updates an existing record
- `deleteRecord(RID rid)`
  - Removes a record and updates free space tracking.
- `deleteFile()`
  - Deletes the heap file and deallocates all pages.
- `getRecCnt()`
  - Returns the total number of records in the heap file.
- `openScan()`
  - Returns a HeapScan object for iterating over all records.

## Key Components of Part 3: Written in HeapScan.java

### 1. **Data Structures**
- `List<PageId> pageIds`: A list of all page IDs in the heap file (copied from `HeapFile.pageIdList`).
- `int currentPageIndex`: Tracks the current page being scanned.
- `RID currentRid)`: Keeps track of the current record position in the page.
- `HFPage currentPage`: Stores the currently loaded HFPage.
- `PageId currentPageId`: Stores the PageId of the current HFPage.


### 2. **Key Methods**
- `HeapScan(HeapFile hf)`: Initializes a scan by pinning the first page and setting up iteration.
- `boolean hasNext()`: Returns true if there are more records to scan.
- `Tuple getNext(RID rid)`: Returns the next record in the heap file.
- `void close()`: Closes the scan and unpins any pinned pages.
