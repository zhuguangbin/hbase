package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a Double value.
 */
public class DoubleComparator extends WritableByteArrayComparable {
  
  private Double doubleValue;

  /** Nullary constructor for Writable, do not use */
  public DoubleComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public DoubleComparator(Double value) {
    super(Bytes.toBytes(value));
    this.doubleValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.doubleValue.compareTo(Bytes.toDouble(value, offset));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    Double doubleValue = in.readDouble();
    this.value = Bytes.toBytes(doubleValue);
    this.doubleValue = doubleValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(doubleValue);
  }

}
