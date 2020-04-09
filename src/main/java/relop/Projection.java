package relop;


/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	private boolean isOpen;
	Iterator aIter;
	Integer[] aFields;
	Schema projectSchema;
	Tuple tuple;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator aIter, Integer... aFields) {
	  this.aIter = aIter;
	  this.aFields = aFields;
	  this.schema = aIter.getSchema();
	  projectSchema = new Schema(aFields.length);
	  isOpen = true;
      //init the projection schema
      for(int i = 0 ; i < aFields.length ; i++) {
          projectSchema.initField(i,schema.fieldType(aFields[i]),schema.fieldLength(aFields[i]),schema.fieldName(aFields[i]));
      }
      setSchema(projectSchema);
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
	  if(aIter.hasNext()) {
	      Tuple cur = aIter.getNext();
	      tuple = new Tuple(projectSchema);
	      for(int i = 0 ; i < aFields.length ; i++) {
	          tuple.setField(i, cur.getField(aFields[i]));
          }
	      return true;
      } else {
	      return false;
      }
    //Your code here
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
	      throw new IllegalStateException("No tuple");
      }
    //Your code here
  }

} // public class Projection extends Iterator
