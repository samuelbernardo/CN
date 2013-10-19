
/**
 * 
 */
public class ReduceKey<T> implements WritableComparable {
  /**
   * Identifier can represent a phone or a cell.
   */
  private String id;
  /**
   * This class holds the date and time values.
   */
  private Date date;

  /**
   *
   */
  public ReduceKey<T>(String id, Date date) {
    this.id = id;
    this.date = date;
  }

  public Date getDate() { return this.date; }
  public String getId() { return this.id; } 

  public void write(DataOutput out) throws IOException {}
  public void readFields(DataInput in) throws IOException {}
  /**
   *
   */
  public int compareTo(ReduceKey o){
    return (id.compareTo(o.getId()) == 0 ? 
     this.date.compareTo(o.getDate()) : id.compareTo(o.getId())); 
  }
}
