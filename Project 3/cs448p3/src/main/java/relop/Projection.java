package relop;


/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  private Iterator iter = null;
  private Integer[] fields = null;
  private boolean isOpen;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator aIter, Integer... aFields) {
    this.iter = aIter;
    this.fields = aFields;
    this.isOpen = true;

    this.schema = new Schema(fields.length);
    Schema iterSchema = iter.getSchema();
    for(int i = 0; i < fields.length; i++){
      this.schema.initField(i, iterSchema, fields[i]);
    }
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
    // Your code here
    iter.restart();
    this.isOpen = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    // Your code here
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    // Your code here
    iter.close();
    isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // Your code here
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // Your code here
    Tuple nextIter = iter.getNext();
    Tuple tup = new Tuple(this.schema);
    for (int i = 0; i < fields.length; i++) {
      tup.setField(this.schema.fieldName(i), nextIter.getField(fields[i]));
    }
    return tup;
  }

} // public class Projection extends Iterator