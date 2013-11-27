package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a Integer value.
 */
public class IntComparator extends WritableByteArrayComparable {
  
  private Integer intValue;

  /** Nullary constructor for Writable, do not use */
  public IntComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public IntComparator(Integer value) {
    super(Bytes.toBytes(value));
    this.intValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.intValue.compareTo(Bytes.toInt(value, offset, length));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    Integer intValue = in.readInt();
    this.value = Bytes.toBytes(intValue);
    this.intValue = intValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(intValue);
  }

}
