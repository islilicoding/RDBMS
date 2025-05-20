# Buffer Manager (BufMgr)

## Group Members
- Lilianne Brush
- Noa Shoval
- Aaron Neman

### Contributions
- **Part 1 written by:** Lilianne Brush

## Overview
The `BufMgr` class is a buffer manager that handles pages in memory for a database system. It provides page retrieval, pinning, unpinning, and page replacement using a FIFO policy. The implementation includes a hash table for fast lookups and a queue for frame replacement.

## Key Components of Part 1: Written in BufMgr.java

### 1. **FrameDescriptor**
   - Stores metadata for each frame in the buffer pool.
   - Tracks the `PageId`, `pinCount`, and whether the page is `dirty`.

### 2. **HashTable**
   - Implements a hash table to map page numbers to buffer frames.
   - Supports `insert()`, `search()`, and `remove()` operations.

### 3. **BufMgr Constructor**
   - Initializes the buffer pool and hash table.
   - Supports a FIFO-based replacement policy.

### 4. **Buffer Management Functions**
   - `pinPage(PageId, Page, boolean)`: Pins a page in memory.
   - `unpinPage(PageId, boolean)`: Unpins a page and marks it dirty if needed.
   - `newPage(Page, int)`: Allocates new pages and pins the first one.
   - `freePage(PageId)`: Deallocates a page from memory and disk.
   - `flushPage(PageId)`: Writes a specific page to disk.
   - `flushAllPages()`: Writes all pages in the buffer pool to disk.

### 5. **Replacement Policy (FIFO)**
   - `get_replacement_index()`: Finds a frame in the buffer pool for replacement.
   - `add_to_fifo(int)`: Adds a buffer pool frame index to the FIFO queue.

### 6. **Utility Functions**
   - `getNumBuffers()`: Returns the total number of buffers.
   - `getNumUnpinnedBuffers()`: Returns the number of unpinned buffers.


