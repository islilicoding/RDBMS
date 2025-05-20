package heap;

import global.*;
import chainexception.ChainException;

import java.util.ArrayList;
import java.util.List;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {
  private List<PageId> pageIds;
  private int currentPageIndex;
  private RID currentRid;
  private HFPage currentPage;
  private PageId currentPageId;

  /**
   * Constructs a file scan by pinning the directoy header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {
    this.pageIds = new ArrayList<>(hf.pageIdList);
    this.currentPageIndex = 0;
    this.currentRid = null;
    this.currentPage = null;
    this.currentPageId = null;

    if (!pageIds.isEmpty()) {
      currentPageId = pageIds.get(currentPageIndex);
      currentPage = new HFPage();
        Minibase.BufferManager.pinPage(currentPageId, currentPage, PIN_DISKIO);
        currentRid = currentPage.firstRecord();
    }
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
    close();
  }

  /**
   * Closes the file scan, releasing any pinned pages.
   */
  public void close() throws ChainException {
    unpinCurrentPage();
    pageIds = null;
    currentRid = null;
  }

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    if (currentPageIndex >= pageIds.size()) return false;
    if (currentPage == null) return true;
    return currentRid != null || currentPageIndex < pageIds.size() - 1;
  }

  /**
   * Gets the next record in the file scan.
   *
   * @param rid output parameter that identifies the returned record
   * @throws IllegalStateException if the scan has no more elements
   */
  public Tuple getNext(RID rid) {
    while (currentPageIndex < pageIds.size()) {
      if (currentPage == null) {
        currentPageId = pageIds.get(currentPageIndex);
        currentPage = new HFPage();
          Minibase.BufferManager.pinPage(currentPageId, currentPage, PIN_DISKIO);
          currentRid = currentPage.firstRecord();
      }

      if (currentRid != null) {
        byte[] record = currentPage.selectRecord(currentRid);
        Tuple tuple = new Tuple(record, 0, record.length);
        rid.copyRID(currentRid);
        currentRid = currentPage.nextRecord(currentRid);
        return tuple;
      } else {
        unpinCurrentPage();
        currentPageIndex++;
        currentPage = null;
        currentPageId = null;
      }
    }
    return null;
  }

  private void unpinCurrentPage() {
    if (currentPage != null && currentPageId != null) {
        Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
        currentPage = null;
      currentPageId = null;
    }
  }
}// public class HeapScan implements GlobalConst