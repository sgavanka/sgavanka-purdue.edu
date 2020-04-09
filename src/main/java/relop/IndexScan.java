package relop;

import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
     private HashIndex index = null;
     private HeapFile file = null;
     private BucketScan scan = null;
     private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.schema = schema;
	  this.index = index;
	  this.file = file;
	  this.scan = index.openScan();
  //Your code here
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
  //Your code here
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  scan.close();
	  scan = index.openScan();
  //Your code here
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return isOpen;
  //Your code here
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  scan.close();
	  isOpen = false;
  //Your code here
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return scan.hasNext();
  //Your code here
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  return new Tuple(schema, file.selectRecord(scan.getNext()));
  //Your code here
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  return scan.getLastKey();
  //Your code here
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  return scan.getNextHash();
  //Your code here
  }

} // public class IndexScan extends Iterator
