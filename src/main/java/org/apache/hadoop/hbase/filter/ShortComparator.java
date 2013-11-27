package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a Short value.
 */
public class ShortComparator extends WritableByteArrayComparable {
  
  private Short shortValue;

  /** Nullary constructor for Writable, do not use */
  public ShortComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public ShortComparator(Short value) {
    super(Bytes.toBytes(value));
    this.shortValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.shortValue.compareTo(Bytes.toShort(value, offset, length));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    Short shortValue = in.readShort();
    this.value = Bytes.toBytes(shortValue);
    this.shortValue = shortValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(shortValue);
  }

}
