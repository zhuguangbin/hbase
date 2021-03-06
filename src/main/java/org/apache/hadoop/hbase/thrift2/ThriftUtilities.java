/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.thrift2;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

public class ThriftUtilities {

  private ThriftUtilities() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * Creates a {@link Get} (HBase) from a {@link TGet} (Thrift).
   * 
   * This ignores any timestamps set on {@link TColumn} objects.
   * 
   * @param in the <code>TGet</code> to convert
   * 
   * @return <code>Get</code> object
   * 
   * @throws IOException if an invalid time range or max version parameter is given
   */
  public static Get getFromThrift(TGet in) throws IOException {
    Get out = new Get(in.getRow());

    // Timestamp overwrites time range if both are set
    if (in.isSetTimestamp()) {
      out.setTimeStamp(in.getTimestamp());
    } else if (in.isSetTimeRange()) {
      out.setTimeRange(in.getTimeRange().getMinStamp(), in.getTimeRange().getMaxStamp());
    }

    if (in.isSetMaxVersions()) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (in.isSetFilterString()) {
      ParseFilter parseFilter = new ParseFilter();
      out.setFilter(parseFilter.parseFilterString(in.getFilterString()));
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (!in.isSetColumns()) {
      return out;
    }

    for (TColumn column : in.getColumns()) {
      if (column.isSetQualifier()) {
        out.addColumn(column.getFamily(), column.getQualifier());
      } else {
        out.addFamily(column.getFamily());
      }
    }

    return out;
  }

  /**
   * Converts multiple {@link TGet}s (Thrift) into a list of {@link Get}s (HBase).
   * 
   * @param in list of <code>TGet</code>s to convert
   * 
   * @return list of <code>Get</code> objects
   * 
   * @throws IOException if an invalid time range or max version parameter is given
   * @see #getFromThrift(TGet)
   */
  public static List<Get> getsFromThrift(List<TGet> in) throws IOException {
    List<Get> out = new ArrayList<Get>(in.size());
    for (TGet get : in) {
      out.add(getFromThrift(get));
    }
    return out;
  }
  


  public static TGet getFromHBase(Get in) throws IOException {
      TGet out = new TGet(ByteBuffer.wrap(in.getRow()));
      
      TTimeRange tr = new TTimeRange(in.getTimeRange().getMin(), in.getTimeRange().getMax());
      out.setTimeRange(tr);
      out.setMaxVersions(in.getMaxVersions());
      // Filter
      if (in.getFilter() != null) {
          out.setFilterString(Bytes.toBytes(in.getFilter().toString()));
      }
      // Attributes
      Map<ByteBuffer, ByteBuffer> attrMap = new HashMap<ByteBuffer, ByteBuffer>();
      for(Map.Entry<String, byte[]> attrEntry: in.getAttributesMap().entrySet()){
          attrMap.put(ByteBuffer.wrap(attrEntry.getKey().getBytes()), ByteBuffer.wrap(attrEntry.getValue()));
      }
      out.setAttributes(attrMap);
      
      List<TColumn> columns = new ArrayList<TColumn>();
      for (Map.Entry<byte[], NavigableSet<byte[]>> familyEntry : in.getFamilyMap().entrySet()) {
          TColumn column = new TColumn();
          column.setFamily(familyEntry.getKey());
          for (byte[] qualifier : familyEntry.getValue()) {
              column.setQualifier(qualifier);
          }
          columns.add(column);
      }
      out.setColumns(columns);
      return out;
  }

  public static List<TGet> getsFromHBase(List<Get> in) throws IOException {
      List<TGet> out = new ArrayList<TGet>(in.size());
      for (TGet tGet : out) {
          out.add(tGet);
      }
      return out;
  }
  public static Result resultFromThrift(TResult in) {
    List<KeyValue> kvs = new ArrayList<KeyValue>();

    byte[] row = in.getRow();

    List<TColumnValue> columnValues = in.getColumnValues();
    for (TColumnValue tColumnValue : columnValues) {
      byte[] family = tColumnValue.getFamily();
      byte[] qualifier = tColumnValue.getQualifier();
      long ts = tColumnValue.getTimestamp();
      byte[] value = tColumnValue.getValue();

      kvs.add(new KeyValue(row, family, qualifier, ts, value));
    }

    return new Result(kvs);
  }
  
  
  public static Result[] resultsFromThrift(List<TResult> in) {
    List<Result> out = new ArrayList<Result>(in.size());

    for (TResult result : in) {
      out.add(resultFromThrift(result));
    }

    return out.toArray(new Result[out.size()]);
  }

  /**
   * Creates a {@link TResult} (Thrift) from a {@link Result} (HBase).
   * 
   * @param in the <code>Result</code> to convert
   * 
   * @return converted result, returns an empty result if the input is <code>null</code>
   */
  public static TResult resultFromHBase(Result in) {
    KeyValue[] raw = in.raw();
    TResult out = new TResult();
    byte[] row = in.getRow();
    if (row != null) {
      out.setRow(in.getRow());
    }
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    for (KeyValue kv : raw) {
      TColumnValue col = new TColumnValue();
      col.setFamily(kv.getFamily());
      col.setQualifier(kv.getQualifier());
      col.setTimestamp(kv.getTimestamp());
      col.setValue(kv.getValue());
      columnValues.add(col);
    }
    out.setColumnValues(columnValues);
    return out;
  }

  /**
   * Converts multiple {@link Result}s (HBase) into a list of {@link TResult}s (Thrift).
   * 
   * @param in array of <code>Result</code>s to convert
   * 
   * @return list of converted <code>TResult</code>s
   * 
   * @see #resultFromHBase(Result)
   */
  public static List<TResult> resultsFromHBase(Result[] in) {
    List<TResult> out = new ArrayList<TResult>(in.length);
    for (Result result : in) {
      out.add(resultFromHBase(result));
    }
    return out;
  }

  /**
   * Creates a {@link Put} (HBase) from a {@link TPut} (Thrift)
   * 
   * @param in the <code>TPut</code> to convert
   * 
   * @return converted <code>Put</code>
   */
  public static Put putFromThrift(TPut in) {
    Put out;

    if (in.isSetTimestamp()) {
      out = new Put(in.getRow(), in.getTimestamp(), null);
    } else {
      out = new Put(in.getRow());
    }

    if (in.isSetDurability()) {
      out.setDurability(durabilityFromThrift(in.getDurability()));
    } else if (in.isSetWriteToWal()) {
      out.setWriteToWAL(in.isWriteToWal());
    }

    for (TColumnValue columnValue : in.getColumnValues()) {
      if (columnValue.isSetTimestamp()) {
        out.add(columnValue.getFamily(), columnValue.getQualifier(), columnValue.getTimestamp(),
            columnValue.getValue());
      } else {
        out.add(columnValue.getFamily(), columnValue.getQualifier(), columnValue.getValue());
      }
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    return out;
  }

  /**
   * Converts multiple {@link TPut}s (Thrift) into a list of {@link Put}s (HBase).
   * 
   * @param in list of <code>TPut</code>s to convert
   * 
   * @return list of converted <code>Put</code>s
   * 
   * @see #putFromThrift(TPut)
   */
  public static List<Put> putsFromThrift(List<TPut> in) {
    List<Put> out = new ArrayList<Put>(in.size());
    for (TPut put : in) {
      out.add(putFromThrift(put));
    }
    return out;
  }
  
  
  public static TPut putFromHBase(Put in) {
    TPut out = new TPut();

    out.setRow(ByteBuffer.wrap(in.getRow()));
    out.setWriteToWal(in.getWriteToWAL());
    if (in.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
      out.setTimestamp(in.getTimeStamp());
    }
    
    Map<ByteBuffer, ByteBuffer> attrMap = new HashMap<ByteBuffer, ByteBuffer>();
    for(Map.Entry<String, byte[]> attrEntry: in.getAttributesMap().entrySet()){
        attrMap.put(ByteBuffer.wrap(attrEntry.getKey().getBytes()), ByteBuffer.wrap(attrEntry.getValue()));
    }
    out.setAttributes(attrMap);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    // Map<family, List<KeyValue>>
    for (Map.Entry<byte[], List<KeyValue>> familyEntry : in.getFamilyMap().entrySet()) {
      TColumnValue columnValue = new TColumnValue();

      for (KeyValue keyValue : familyEntry.getValue()) {
          byte[] family = keyValue.getFamily();
          byte[] qualifier = keyValue.getQualifier();
          long timestamp = keyValue.getTimestamp();
          byte[] value = keyValue.getValue();
          if (family != null) {
            columnValue.setFamily(family);
          }
          if (qualifier != null) {
            columnValue.setQualifier(qualifier);
          }
          if (timestamp != HConstants.LATEST_TIMESTAMP) {
            columnValue.setTimestamp(keyValue.getTimestamp());
          }
          columnValue.setValue(value);
       }
       columnValues.add(columnValue);
    }

    out.setColumnValues(columnValues);
    return out;
  }


public static List<TPut> putsFromHBase(List<Put> in) {
    List<TPut> out = new ArrayList<TPut>();
    for (Put put : in) {
        out.add(putFromHBase(put));
    }
    return out;
  }

  /**
   * Creates a {@link Delete} (HBase) from a {@link TDelete} (Thrift).
   * 
   * @param in the <code>TDelete</code> to convert
   * 
   * @return converted <code>Delete</code>
   */
  public static Delete deleteFromThrift(TDelete in) {
    Delete out;

    if (in.isSetColumns()) {
      out = new Delete(in.getRow());
      for (TColumn column : in.getColumns()) {
        if (column.isSetQualifier()) {
          if (column.isSetTimestamp()) {
            if (in.isSetDeleteType() &&
                in.getDeleteType().equals(TDeleteType.DELETE_COLUMNS))
              out.deleteColumns(column.getFamily(), column.getQualifier(), column.getTimestamp());
            else
              out.deleteColumn(column.getFamily(), column.getQualifier(), column.getTimestamp());
          } else {
            if (in.isSetDeleteType() &&
                in.getDeleteType().equals(TDeleteType.DELETE_COLUMNS))
              out.deleteColumns(column.getFamily(), column.getQualifier());
            else
              out.deleteColumn(column.getFamily(), column.getQualifier());
          }

        } else {
          if (column.isSetTimestamp()) {
            out.deleteFamily(column.getFamily(), column.getTimestamp());
          } else {
            out.deleteFamily(column.getFamily());
          }
        }
      }
    } else {
      if (in.isSetTimestamp()) {
        out = new Delete(in.getRow(), in.getTimestamp(), null);
      } else {
        out = new Delete(in.getRow());
      }
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.isSetDurability()) {
      out.setDurability(durabilityFromThrift(in.getDurability()));
    } else if (in.isSetWriteToWal()) {
      out.setWriteToWAL(in.isWriteToWal());
    }

    return out;
  }

  /**
   * Converts multiple {@link TDelete}s (Thrift) into a list of {@link Delete}s (HBase).
   * 
   * @param in list of <code>TDelete</code>s to convert
   * 
   * @return list of converted <code>Delete</code>s
   * 
   * @see #deleteFromThrift(TDelete)
   */

  public static List<Delete> deletesFromThrift(List<TDelete> in) {
    List<Delete> out = new ArrayList<Delete>(in.size());
    for (TDelete delete : in) {
      out.add(deleteFromThrift(delete));
    }
    return out;
  }

  public static TDelete deleteFromHBase(Delete in) {
    TDelete out = new TDelete(ByteBuffer.wrap(in.getRow()));

    List<TColumn> columns = new ArrayList<TColumn>();
    long rowTimestamp = in.getTimeStamp();
    if (rowTimestamp != HConstants.LATEST_TIMESTAMP) {
      out.setTimestamp(rowTimestamp);
    }
    
    // Attributes
    Map<ByteBuffer, ByteBuffer> attrMap = new HashMap<ByteBuffer, ByteBuffer>();
    for(Map.Entry<String, byte[]> attrEntry: in.getAttributesMap().entrySet()){
        attrMap.put(ByteBuffer.wrap(attrEntry.getKey().getBytes()), ByteBuffer.wrap(attrEntry.getValue()));
    }
    out.setAttributes(attrMap);

    // Map<family, List<KeyValue>>
    for (Map.Entry<byte[], List<KeyValue>> familyEntry : in.getFamilyMap().entrySet()) {
      TColumn column = new TColumn(ByteBuffer.wrap(familyEntry.getKey()));
      for (KeyValue keyValue : familyEntry.getValue()) {
        byte[] family = keyValue.getFamily();
        byte[] qualifier = keyValue.getQualifier();
        long timestamp = keyValue.getTimestamp();
        if (family != null) {
          column.setFamily(family);
        }
        if (qualifier != null) {
          column.setQualifier(qualifier);
        }
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
          column.setTimestamp(keyValue.getTimestamp());
        }
      }
      columns.add(column);
    }
    out.setColumns(columns);

    return out;
  }

  public static List<TDelete> deletesFromHBase(List<Delete> in) {
    List<TDelete> out = new ArrayList<TDelete>(in.size());
    for (Delete delete : in) {
      if (delete == null) {
        out.add(null);
      } else {
        out.add(deleteFromHBase(delete));
      }
    }
    return out;
  }

  /**
   * Creates a {@link RowMutations} (HBase) from a {@link TRowMutations} (Thrift)
   *
   * @param in the <code>TRowMutations</code> to convert
   *
   * @return converted <code>RowMutations</code>
   */
  public static RowMutations rowMutationsFromThrift(TRowMutations in) throws IOException {
    RowMutations out = new RowMutations(in.getRow());
    List<TMutation> mutations = in.getMutations();
    for (TMutation mutation : mutations) {
      if (mutation.isSetPut()) {
        out.add(putFromThrift(mutation.getPut()));
      }
      if (mutation.isSetDeleteSingle()) {
        out.add(deleteFromThrift(mutation.getDeleteSingle()));
      }
    }
    return out;
  }
  
  public static TRowMutations rowMutationsFromHBase(RowMutations in) throws IOException {
      TRowMutations out = new TRowMutations();
      out.setRow(ByteBuffer.wrap(in.getRow()));
      List<Mutation> mutations = in.getMutations();
      for (Mutation mutation : mutations) {
          TMutation tmutation = new TMutation();
          if (mutation instanceof Put) {
            tmutation.setPut(putFromHBase((Put)mutation));
          }
          if (mutation instanceof Delete) {
            tmutation.setDeleteSingle(deleteFromHBase((Delete)mutation));
        }
        out.addToMutations(tmutation);
    }
      return out;
  }
  

  public static Scan scanFromThrift(TScan in) throws IOException {
    Scan out = new Scan();

    if (in.isSetStartRow())
      out.setStartRow(in.getStartRow());
    if (in.isSetStopRow())
      out.setStopRow(in.getStopRow());
    if (in.isSetCaching())
      out.setCaching(in.getCaching());
    if (in.isSetMaxVersions()) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (in.isSetColumns()) {
      for (TColumn column : in.getColumns()) {
        if (column.isSetQualifier()) {
          out.addColumn(column.getFamily(), column.getQualifier());
        } else {
          out.addFamily(column.getFamily());
        }
      }
    }

    TTimeRange timeRange = in.getTimeRange();
    if (timeRange != null &&
        timeRange.isSetMinStamp() && timeRange.isSetMaxStamp()) {
      out.setTimeRange(timeRange.getMinStamp(), timeRange.getMaxStamp());
    }

    if (in.isSetBatchSize()) {
      out.setBatch(in.getBatchSize());
    }

    if (in.isSetFilterString()) {
      ParseFilter parseFilter = new ParseFilter();
      out.setFilter(parseFilter.parseFilterString(in.getFilterString()));
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    return out;
  }
  
  public static TScan scanFromHBase(Scan in) throws IOException {
    TScan out = new TScan();
    
    if (in.getBatch() != -1) {
        out.setBatchSize(in.getBatch());
    }

    if (in.getCaching() != -1) {
      out.setCaching(in.getCaching());
    }

    if (in.getMaxVersions() != 1) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (in.getTimeRange().getMin() != 0L || in.getTimeRange().getMax() != Long.MAX_VALUE) {
      TTimeRange tr = new TTimeRange(in.getTimeRange().getMin(), in.getTimeRange().getMax());
      out.setTimeRange(tr);
    }

    if (in.getStartRow() != HConstants.EMPTY_START_ROW) {
      out.setStartRow(in.getStartRow());
    }
    if (in.getStopRow() != HConstants.EMPTY_END_ROW) {
      out.setStopRow(in.getStopRow());
    }
    
    // Filter
    if (in.getFilter() != null) {
        out.setFilterString(Bytes.toBytes(in.getFilter().toString()));
    }
    // Attributes
    Map<ByteBuffer, ByteBuffer> attrMap = new HashMap<ByteBuffer, ByteBuffer>();
    for(Map.Entry<String, byte[]> attrEntry: in.getAttributesMap().entrySet()){
        attrMap.put(ByteBuffer.wrap(attrEntry.getKey().getBytes()), ByteBuffer.wrap(attrEntry.getValue()));
    }
    out.setAttributes(attrMap);

    List<TColumn> columns = new ArrayList<TColumn>();
    for (Map.Entry<byte[], NavigableSet<byte[]>> familyEntry : in.getFamilyMap().entrySet()) {
      TColumn column = new TColumn();
      column.setFamily(familyEntry.getKey());
      for (byte[] qualifier : familyEntry.getValue()) {
        column.setQualifier(qualifier);
      }
      columns.add(column);
    }
    out.setColumns(columns);
    return out;
}

  public static Increment incrementFromThrift(TIncrement in) throws IOException {
    Increment out = new Increment(in.getRow());
    for (TColumnIncrement column : in.getColumns()) {
      out.addColumn(column.getFamily(), column.getQualifier(), column.getAmount());
    }

    out.setWriteToWAL(in.isWriteToWal());
    return out;
  }
  
  
  public static TIncrement incrementFromHBase(Increment in) throws IOException {
    TIncrement out = new TIncrement();
    out.setRow(in.getRow());
    out.setWriteToWal(in.getWriteToWAL());
    
    List<TColumnIncrement> columnIncrements = new ArrayList<TColumnIncrement>();
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : in.getFamilyMap().entrySet()) {
        TColumnIncrement columnIncrement = new TColumnIncrement();
        columnIncrement.setFamily(familyEntry.getKey());
        for (Map.Entry<byte[], Long> increment : familyEntry.getValue().entrySet()) {
            columnIncrement.setQualifier(increment.getKey());
            columnIncrement.setAmount(increment.getValue());
        }
    columnIncrements.add(columnIncrement);
  }
  out.setColumns(columnIncrements);
  return out;
}

  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
                                    Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.size() == 0) {
      return;
    }
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : attributes.entrySet()) {
      String name = Bytes.toStringBinary(getBytes(entry.getKey()));
      byte[] value =  getBytes(entry.getValue());
      op.setAttribute(name, value);
    }
  }

  private static Durability durabilityFromThrift(TDurability tDurability) {
    switch (tDurability.getValue()) {
      case 1: return Durability.SKIP_WAL;
      case 2: return Durability.ASYNC_WAL;
      case 3: return Durability.SYNC_WAL;
      case 4: return Durability.FSYNC_WAL;
      default: return null;
    }
  }
}
