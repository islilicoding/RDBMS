package heap;

import global.*;
import chainexception.ChainException;

import java.util.*;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {
  protected List<PageId> pageIdList;
  protected TreeMap<Integer, List<PageId>> freeSpaceMap;
  protected int recCount;
  protected String name;
  protected int fileStatus;

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
    this.name = name;
    this.fileStatus = 0;
    this.recCount = 0;
    this.pageIdList = new ArrayList<>();
    this.freeSpaceMap = new TreeMap<>(Collections.reverseOrder());

    try {
      if (name != null) {
        PageId firstPageId = Minibase.DiskManager.get_file_entry(name);
        if (firstPageId == null) {
          initializeNewFile();
        } else {
          loadExistingFile(firstPageId);
        }
      } else {
        initializeTempFile();
      }
    } catch (Exception e) {
      throw new RuntimeException("HeapFile initialization failed", e);
    }
  }

  private void initializeNewFile() throws ChainException {
    Page newPage = new Page();
    PageId newPageId = Minibase.BufferManager.newPage(newPage, 1);
    Minibase.DiskManager.add_file_entry(name, newPageId);
    HFPage hfPage = new HFPage(newPage);
    hfPage.initDefaults();
    hfPage.setCurPage(newPageId);
    updateStructures(newPageId, Integer.MAX_VALUE, hfPage.getFreeSpace());
    Minibase.BufferManager.unpinPage(newPageId, UNPIN_DIRTY);
  }

  private void loadExistingFile(PageId firstPageId) throws ChainException {
    PageId currentPageId = firstPageId;
    while (currentPageId.pid != -1) {
      pageIdList.add(currentPageId);
      HFPage hfPage = new HFPage();
      Minibase.BufferManager.pinPage(currentPageId, hfPage, PIN_DISKIO);
      updateStructures(currentPageId, Integer.MAX_VALUE, hfPage.getFreeSpace());
      countRecords(hfPage);
      PageId nextPageId = hfPage.getNextPage();
      Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
      currentPageId = nextPageId;
    }
  }

  private void countRecords(HFPage hfPage) {
    RID rid = hfPage.firstRecord();
    while (rid != null) {
      recCount++;
      rid = hfPage.nextRecord(rid);
    }
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
    if (name == null) {
      deleteFile();
    }
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
    if (fileStatus == 0) {
      for (PageId pageId : pageIdList) {
        Minibase.DiskManager.deallocate_page(pageId);
      }
      Minibase.DiskManager.delete_file_entry(name);
      recCount = 0;
      fileStatus = 1;
      pageIdList.clear();
      freeSpaceMap.clear();
    }
  }

  /**
   * Inserts a new record into the file and returns its RID.
   *
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) throws ChainException {
    if (record.length + HFPage.HEADER_SIZE > PAGE_SIZE) {
      throw new SpaceNotAvailableException("Record too large");
    }

    int spaceNeeded = record.length + 4;
    if (!freeSpaceMap.isEmpty() && freeSpaceMap.firstKey() >= spaceNeeded) {
      return insertIntoExistingPage(record, spaceNeeded);
    } else {
      return createNewPage(record);
    }
  }

  private RID insertIntoExistingPage(byte[] record, int spaceNeeded) throws ChainException {
    Integer maxFree = freeSpaceMap.firstKey();
    List<PageId> candidates = freeSpaceMap.get(maxFree);
    PageId targetPageId = candidates.remove(0);
    if (candidates.isEmpty()) {
      freeSpaceMap.remove(maxFree);
    }

    HFPage hfPage = new HFPage();
    Minibase.BufferManager.pinPage(targetPageId, hfPage, PIN_DISKIO);
    int oldFreeSpace = hfPage.getFreeSpace();
    RID rid = hfPage.insertRecord(record);
    if (rid == null) {
      Minibase.BufferManager.unpinPage(targetPageId, UNPIN_CLEAN);
      throw new SpaceNotAvailableException("Insert failed");
    }
    int newFreeSpace = hfPage.getFreeSpace();
    updateStructures(targetPageId, oldFreeSpace, newFreeSpace);
    Minibase.BufferManager.unpinPage(targetPageId, UNPIN_DIRTY);
    recCount++;
    return rid;
  }

  private RID createNewPage(byte[] record) throws ChainException {
    Page newPage = new Page();
    PageId newPageId = Minibase.BufferManager.newPage(newPage, 1);
    HFPage hfPage = new HFPage(newPage);
    hfPage.initDefaults();
    hfPage.setCurPage(newPageId);

    if (!pageIdList.isEmpty()) {
      linkNewPage(newPageId);
    }

    RID rid = hfPage.insertRecord(record);
    if (rid == null) {
      Minibase.BufferManager.unpinPage(newPageId, UNPIN_DIRTY);
      throw new SpaceNotAvailableException("Insert into new page failed");
    }
    updateStructures(newPageId, Integer.MAX_VALUE, hfPage.getFreeSpace());
    recCount++;
    Minibase.BufferManager.unpinPage(newPageId, UNPIN_DIRTY);
    return rid;
  }

  private void linkNewPage(PageId newPageId) throws ChainException {
    PageId prevPageId = pageIdList.get(pageIdList.size() - 1);
    HFPage prevPage = new HFPage();
    Minibase.BufferManager.pinPage(prevPageId, prevPage, PIN_DISKIO);
    prevPage.setNextPage(newPageId);
    Minibase.BufferManager.unpinPage(prevPageId, UNPIN_DIRTY);

    HFPage newPage = new HFPage();
    Minibase.BufferManager.pinPage(newPageId, newPage, PIN_DISKIO);
    newPage.setPrevPage(prevPageId);
    Minibase.BufferManager.unpinPage(newPageId, UNPIN_DIRTY);
  }

  private void initializeTempFile() {
    PageId tempPageId = new PageId(-1);
    pageIdList.add(tempPageId);
    freeSpaceMap.computeIfAbsent(1024 - HFPage.HEADER_SIZE, k -> new ArrayList<>()).add(tempPageId);
  }

  private void updateStructures(PageId pageId, int oldFreeSpace, int newFreeSpace) {
    if (oldFreeSpace != Integer.MAX_VALUE) {
      List<PageId> oldList = freeSpaceMap.get(oldFreeSpace);
      if (oldList != null) {
        oldList.remove(pageId);
        if (oldList.isEmpty()) {
          freeSpaceMap.remove(oldFreeSpace);
        }
      }
    }
    freeSpaceMap.computeIfAbsent(newFreeSpace, k -> new ArrayList<>()).add(pageId);
    if (!pageIdList.contains(pageId)) {
      pageIdList.add(pageId);
    }
  }

  public Tuple getRecord(RID rid) throws ChainException {
    PageId pageno = rid.pageno;
    boolean found = false;
    for (PageId pid : pageIdList) {
      if (pid.pid == pageno.pid) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new IllegalArgumentException("Invalid RID");
    }

    HFPage hfPage = new HFPage();
    Minibase.BufferManager.pinPage(pageno, hfPage, PIN_DISKIO);
    try {
      byte[] record = hfPage.selectRecord(rid);
      return new Tuple(record, 0, record.length);
    } finally {
      Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
    }
  }

  /**
   * Reads a record from the file, given its id.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {
    PageId targetPage = rid.pageno;

    // Verify the page exists in this heap file
    boolean pageFound = false;
    for (PageId pid : pageIdList) {
      if (pid.pid == targetPage.pid) {
        pageFound = true;
        break;
      }
    }
    if (!pageFound) {
      throw new IllegalArgumentException("Invalid RID - Page not in heap file");
    }

    HFPage page = new HFPage();
    try {
      // Pin page and retrieve record
      Minibase.BufferManager.pinPage(targetPage, page, PIN_DISKIO);
      return page.selectRecord(rid);
    } finally {
      // Ensure page unpinning even if exceptions occur
      Minibase.BufferManager.unpinPage(targetPage, UNPIN_CLEAN);
    }
  }

  /**
   * Updates the specified record in the heap file.
   *
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException {
    PageId pageno = rid.pageno;
    HFPage hfPage = new HFPage();
    Minibase.BufferManager.pinPage(pageno, hfPage, PIN_DISKIO);
    try {
      byte[] oldRecord = hfPage.selectRecord(rid);
      if (oldRecord.length != newRecord.getLength()) {
        throw new InvalidUpdateException();
      }
      hfPage.updateRecord(rid, newRecord);
      return true;
    } finally {
      Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
    }
  }

  /**
   * Deletes the specified record from the heap file.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public boolean deleteRecord(RID rid) throws ChainException {
    PageId pageno = rid.pageno;
    HFPage hfPage = new HFPage();
    Minibase.BufferManager.pinPage(pageno, hfPage, PIN_DISKIO);
    try {
      int oldFreeSpace = hfPage.getFreeSpace();
      hfPage.deleteRecord(rid);
      int newFreeSpace = hfPage.getFreeSpace();
      updateStructures(pageno, oldFreeSpace, newFreeSpace);
      recCount--;
      return true;
    } finally {
      Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
    }
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    return recCount;
  }

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return name;
  }
} // public class HeapFile implements GlobalConst