package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a String value.
 */
public class StringComparator extends WritableByteArrayComparable {
  
  private String strValue;

  /** Nullary constructor for Writable, do not use */
  public StringComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public StringComparator(String value) {
    super(Bytes.toBytes(value));
    this.strValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.strValue.compareTo(Bytes.toString(value, offset, length));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    String strValue = in.readUTF();
    this.value = Bytes.toBytes(strValue);
    this.strValue = strValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(strValue);
  }

}
