package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	private Iterator aIter;
    private Predicate[] aPreds;
	private boolean isOpen;
	private Tuple tuple;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator aIter, Predicate... aPreds) {
	  this.aIter = aIter;
	  this.aPreds = aPreds;
	  this.isOpen = true;
	  this.schema = aIter.getSchema();
	  isOpen = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
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
	  aIter.restart();
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
      aIter.close();
      isOpen = false;
    //Your code here
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
      while(aIter.hasNext()) {
              Tuple tempTup = aIter.getNext();
              for(int i = 0; i < aPreds.length ; i++) {
                  if(aPreds[i].evaluate(tempTup)) {
                      tuple = tempTup;
                      return true;
                }
          }
      }
	  tuple = null;
	  return false;
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if(tuple != null) {
	      return tuple;
      } else {
	      throw new IllegalStateException("No tuples");
      }
    //Your code here
  }

} // public class Selection extends Iterator
