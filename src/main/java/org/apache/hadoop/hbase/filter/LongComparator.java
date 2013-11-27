package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a Long value.
 */
public class LongComparator extends WritableByteArrayComparable {
  
  private Long longValue;

  /** Nullary constructor for Writable, do not use */
  public LongComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public LongComparator(Long value) {
    super(Bytes.toBytes(value));
    this.longValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.longValue.compareTo(Bytes.toLong(value, offset, length));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    Long longValue = in.readLong();
    this.value = Bytes.toBytes(longValue);
    this.longValue = longValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(longValue);
  }

}
