package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
    HeapFile afile = null;
	HashScan scan = null;
	HashIndex aIndex = null;
	SearchKey akey = null;
	private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
	  this.schema = aSchema;
	  this.aIndex = aIndex;
	  this.akey = aKey;
	  this.afile = aFile;
	  this.scan = aIndex.openScan(aKey);
    //Your code here
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  scan.close();
	  scan = aIndex.openScan(akey);

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
      RID t1 = scan.getNext();
      return new Tuple(schema, afile.selectRecord(t1));
    //Your code here
  }

} // public class KeyScan extends Iterator
