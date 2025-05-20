/* ... */

package bufmgr;

import java.awt.*;
import java.io.*;
import java.util.*;

import chainexception.ChainException;
import diskmgr.*;
import global.*;

public class BufMgr implements GlobalConst{
   private int num_buffers;
   private Page[] buffer_pool;
   private FrameDescriptor[] frame_descriptor;
   private HashTable hash_table;
   private Queue<Integer> fifo_queue;
   private String replacement_policy;

  /**
   * Class to store information about each frame in the buffer pool.
   */

  private class FrameDescriptor {
    PageId pageId; // ID of the page in the frame
    int pinCount; // Number of times the page is pinned
    boolean dirty; //If the page is dirty (modified)

    FrameDescriptor() {
      this.pageId = null;
      this.pinCount = 0;
      this.dirty = false;
    }
  }


  /**
   * Hash table class.
   */

  class PageFramePair {
    int pageNumber;
    int frameNumber;

    PageFramePair(int pageNumber, int frameNumber) {
      this.pageNumber = pageNumber;
      this.frameNumber = frameNumber;
    }
  }

  // Class to represent the hash table
  class HashTable {
    private LinkedList<PageFramePair>[] directory;
    private int HTSIZE;

    // Constructor to initialize the hash table with a specific size
    public HashTable(int HTSIZE) {
      this.HTSIZE = HTSIZE;
      directory = new LinkedList[HTSIZE];

      // Initialize each bucket as a linked list
      for (int i = 0; i < HTSIZE; i++) {
        directory[i] = new LinkedList<>();
      }
    }

    // Hash function to map a page number to a bucket
    private int hash(int pageNumber) {
      int a = 3;  // A random constant to multiply
      int b = 7;  // A random constant to add
      return (a * pageNumber + b) % HTSIZE;
    }

    // Insert a page-frame pair into the hash table
    public void insert(int pageNumber, int frameNumber) {
      int index = hash(pageNumber);
      LinkedList<PageFramePair> bucket = directory[index];

      // Check if the page already exists, and update the frame number if so
      for (PageFramePair pair : bucket) {
        if (pair.pageNumber == pageNumber) {
          pair.frameNumber = frameNumber; // Update the frame number
          return;
        }
      }

      // If not, insert a new page-frame pair
      bucket.add(new PageFramePair(pageNumber, frameNumber));
    }

    // Search for a page and return its frame number if found
    public Integer search(int pageNumber) {
      int index = hash(pageNumber);
      LinkedList<PageFramePair> bucket = directory[index];

      // Search for the page in the bucket
      for (PageFramePair pair : bucket) {
        if (pair.pageNumber == pageNumber) {
          return pair.frameNumber; // Return the frame number
        }
      }

      return -1; // Page not found
    }

    // Remove a page-frame pair from the hash table
    public void remove(int pageNumber) {
      int index = hash(pageNumber);
      LinkedList<PageFramePair> bucket = directory[index];

      // Iterate through the list to find and remove the page number
      bucket.removeIf(pair -> pair.pageNumber == pageNumber);
    }


    public PageId getPageIdFromFrameIndex(int frameIndex) {
      // Iterate through the hash table buckets
      for (int i = 0; i < HTSIZE; i++) {
        LinkedList<PageFramePair> bucket = directory[i];

        // Check if the bucket contains a PageFramePair with the given frameIndex
        for (PageFramePair pair : bucket) {
          if (pair.frameNumber == frameIndex) {
            // Return the PageId corresponding to this frameIndex
            return new PageId(pair.pageNumber);
          }
        }
      }

      // If not found, return null (or throw an exception if you prefer)
      return null;
    }



  }



  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg.
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */

  public BufMgr(int numbufs, String replacerArg) {
    this.num_buffers = numbufs;
    this.buffer_pool = new Page[numbufs];
    this.frame_descriptor = new FrameDescriptor[numbufs];
    this.hash_table = new HashTable(100);
    this.fifo_queue = new LinkedList<>();

    if (replacerArg.equals("Unknown") || replacerArg.equals("FIFO")) {
      this.replacement_policy = "FIFO";
    }

    for (int i = 0; i < numbufs; i++) {
      buffer_pool[i] = new Page();
      frame_descriptor[i] = new FrameDescriptor();
      fifo_queue.add(i);
    }

  }


  /**
   * Function to add to FIFO queue.
   */

  private void add_to_fifo(int frame_index) {
    boolean in = false;
    for (int num: fifo_queue) {
      if (num == frame_index) {
        in = true;
      }
    }
    if (in == false) {
      fifo_queue.add(frame_index);
    }
  }

  /**
   * Function to find a replacement frame using the FIFO queue.
   */

  private int get_replacement_index() {
    int size = fifo_queue.size();
    while (size-- > 0) {
      int frame_index = fifo_queue.poll();
      if (frame_descriptor[frame_index].pinCount == 0) {
        return frame_index;
      }
      add_to_fifo(frame_index);
    }
    return -1;
  }

  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param pin_pgid page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
   */

  public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) throws BufferPoolExceededException {

    int frame_index = hash_table.search(pin_pgid.pid);

    // If PageId is in hash table already: increment pin count, copy page pointer
    if (frame_index != -1) {
      frame_descriptor[frame_index].pinCount++;
      page.setpage(buffer_pool[frame_index].getpage());
      return;
    }

    // If PageId not in hash table, find replacement index for buffer pool
    frame_index = get_replacement_index();
    if (frame_index == -1) {
        throw new BufferPoolExceededException(null, "All buffer pool frames are pinned.");
      }

    FrameDescriptor frame = frame_descriptor[frame_index];
    PageId frame_page_id = frame.pageId;
    boolean dirty = frame.dirty;

    // Remove hash table entry for old PageId
    if (frame_page_id != null) {
      //page_id with frame_index is what we need to remove


      //TODO: see if there is a fix that doesn't require this usage
      // Check if the page already exists, and update the frame number if so
      int page_id = hash_table.getPageIdFromFrameIndex(frame_index).pid;
      hash_table.remove(page_id);
    }

    // Write old page to disk if dirty
    if (dirty) {
      flushPage(frame_page_id);
    }

    // Add new page to buffer pool at the index, update frame descriptor properties for new page
    buffer_pool[frame_index].setpage(new byte[MINIBASE_PAGESIZE]);
    frame_descriptor[frame_index].pageId = pin_pgid;
    frame_descriptor[frame_index].pinCount = 1;
    frame_descriptor[frame_index].dirty = false;

    // If new page isn't empty, write it to the buffer pool's page.
    if (!emptyPage) {
      try {
        SystemDefs.JavabaseDB.read_page(pin_pgid, buffer_pool[frame_index]);
      } catch (Exception e) {
        throw new BufferPoolExceededException(e, "Failed to read page from database.");
      }
    }

    // Insert PageId and frame index pair to hash table
    hash_table.insert(pin_pgid.pid, frame_index);

    // Set the page pointer to the buffer pool's page.
    page.setpage(new byte[MINIBASE_PAGESIZE]);
    page.setpage(buffer_pool[frame_index].getpage());

  }


  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count>0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param PageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
   */

  public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws HashEntryNotFoundException, PageUnpinnedException {
    int frame_index = hash_table.search(PageId_in_a_DB.pid);

    // Exception if PageId isn't found in hash table
    if (frame_index == -1) {
      throw new HashEntryNotFoundException(null, "Page not found in buffer pool.");
    }

    // Exception if the page is not pinned
    int pin_count = frame_descriptor[frame_index].pinCount;
    if (pin_count == 0) {
      throw new PageUnpinnedException(null, "Page is already unpinned.");
    }

    // Decrement pin count by 1
    frame_descriptor[frame_index].pinCount--;

    // Write page to disk if dirty
    if (dirty) {
      //TODO: Need "frame_description[frame_index].dirty = true;"??
      flushPage(PageId_in_a_DB);
    }

    // Add frame index to fifo if page is not pinned
    if (frame_descriptor[frame_index].pinCount == 0) {
      add_to_fifo(frame_index);
    }

  }


  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */

  public PageId newPage(Page firstpage, int howmany) {
    // Make PageId object for first new page
    PageId first_page_id = new PageId();

    try {
      // Allocate room for the new pages
      SystemDefs.JavabaseDB.allocate_page(first_page_id, howmany);
      // Pin the first page
      pinPage(first_page_id, firstpage, true);
    } catch (OutOfSpaceException | InvalidRunSizeException | InvalidPageNumberException | IOException | DiskMgrException | BufferPoolExceededException | FileIOException e) {
      try {
        // If allocation fails, deallocate the pages that were allocated
        SystemDefs.JavabaseDB.deallocate_page(first_page_id, howmany);
      } catch (Exception deallocException) {
        deallocException.printStackTrace();
      }
      return null;
    }
    return first_page_id;
  }


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
   */

  public void freePage(PageId globalPageId) throws ChainException {
    int frame_index = hash_table.search(globalPageId.pid);

     if (frame_index != -1) {
       FrameDescriptor frame = frame_descriptor[frame_index];

       // Exception if page is pinned
       if (frame.pinCount > 1) {
         throw new PagePinnedException(null, "Page is pinned.");
       } else if (frame.pinCount == 1) {
         unpinPage(globalPageId, true);
       }

       // Remove page from hash table
       hash_table.remove(globalPageId.pid);

       // Reset parameters about page
       buffer_pool[frame_index].setpage(new byte[MINIBASE_PAGESIZE]);
       frame_descriptor[frame_index].pageId = null;
       frame_descriptor[frame_index].dirty = false;
     }
     try {
       // Deallocate the page
       SystemDefs.JavabaseDB.deallocate_page(globalPageId);
     } catch (Exception e) {
       throw new ChainException(e, "Failed to deallocate the page.");
     }

  }


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
   */

  public void flushPage(PageId pageid) {

    int frame_index = hash_table.search(pageid.pid);

    // Exception if page is not in hash table / buffer pool
    if (frame_index == -1) {
      throw new IllegalArgumentException("Page not found in buffer pool.");
    }

    FrameDescriptor frame = frame_descriptor[frame_index];
    //TODO: add check for if frame is dirty?
    try {
      // Write page to disk, set dirty bit to false
      SystemDefs.JavabaseDB.write_page(pageid, buffer_pool[frame_index]);
      frame.dirty = false;
    } catch (Exception e) {
      throw new RuntimeException("Failed to flush page " + pageid + "to disk.", e);
    }

  }

  /** Flushes all pages of the buffer pool to disk
   */

  public void flushAllPages() {
    for (int i = 0; i < num_buffers; i++) {
      FrameDescriptor frame = frame_descriptor[i];
      if (frame.pageId != null) {
        if (hash_table.search(frame.pageId.pid) != -1) {
          flushPage(frame.pageId);
        }
      }
    }
  }


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */

  public int getNumBuffers() {
    return num_buffers;
  }


  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */

  public int getNumUnpinnedBuffers() {
    int count = 0;
    for (FrameDescriptor frame : frame_descriptor) {
      if (frame.pinCount == 0) {
        count++;
      }
    }
    return count;
  }

}