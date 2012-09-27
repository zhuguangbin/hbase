/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IOError exception signals that an error occurred communicating
 * to the Hbase master or an Hbase region server.  Also used to return
 * more general Hbase error conditions.
 */
public class IOError extends java.io.IOException implements org.apache.thrift.TBase<IOError, IOError._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("IOError");

  private static final org.apache.thrift.protocol.TField MESSAGE_FIELD_DESC = new org.apache.thrift.protocol.TField("message", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField BACKOFF_TIME_MILLIS_FIELD_DESC = new org.apache.thrift.protocol.TField("backoffTimeMillis", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField EXCEPTION_CLASS_FIELD_DESC = new org.apache.thrift.protocol.TField("exceptionClass", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new IOErrorStandardSchemeFactory());
    schemes.put(TupleScheme.class, new IOErrorTupleSchemeFactory());
  }

  public String message; // required
  public long backoffTimeMillis; // required
  public String exceptionClass; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    MESSAGE((short)1, "message"),
    BACKOFF_TIME_MILLIS((short)2, "backoffTimeMillis"),
    EXCEPTION_CLASS((short)3, "exceptionClass");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // MESSAGE
          return MESSAGE;
        case 2: // BACKOFF_TIME_MILLIS
          return BACKOFF_TIME_MILLIS;
        case 3: // EXCEPTION_CLASS
          return EXCEPTION_CLASS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __BACKOFFTIMEMILLIS_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.MESSAGE, new org.apache.thrift.meta_data.FieldMetaData("message", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.BACKOFF_TIME_MILLIS, new org.apache.thrift.meta_data.FieldMetaData("backoffTimeMillis", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.EXCEPTION_CLASS, new org.apache.thrift.meta_data.FieldMetaData("exceptionClass", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(IOError.class, metaDataMap);
  }

  public IOError() {
  }

  public IOError(
    String message,
    long backoffTimeMillis,
    String exceptionClass)
  {
    this();
    this.message = message;
    this.backoffTimeMillis = backoffTimeMillis;
    setBackoffTimeMillisIsSet(true);
    this.exceptionClass = exceptionClass;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public IOError(IOError other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetMessage()) {
      this.message = other.message;
    }
    this.backoffTimeMillis = other.backoffTimeMillis;
    if (other.isSetExceptionClass()) {
      this.exceptionClass = other.exceptionClass;
    }
  }

  public IOError deepCopy() {
    return new IOError(this);
  }

  @Override
  public void clear() {
    this.message = null;
    setBackoffTimeMillisIsSet(false);
    this.backoffTimeMillis = 0;
    this.exceptionClass = null;
  }

  public String getMessage() {
    return this.message;
  }

  public IOError setMessage(String message) {
    this.message = message;
    return this;
  }

  public void unsetMessage() {
    this.message = null;
  }

  /** Returns true if field message is set (has been assigned a value) and false otherwise */
  public boolean isSetMessage() {
    return this.message != null;
  }

  public void setMessageIsSet(boolean value) {
    if (!value) {
      this.message = null;
    }
  }

  public long getBackoffTimeMillis() {
    return this.backoffTimeMillis;
  }

  public IOError setBackoffTimeMillis(long backoffTimeMillis) {
    this.backoffTimeMillis = backoffTimeMillis;
    setBackoffTimeMillisIsSet(true);
    return this;
  }

  public void unsetBackoffTimeMillis() {
    __isset_bit_vector.clear(__BACKOFFTIMEMILLIS_ISSET_ID);
  }

  /** Returns true if field backoffTimeMillis is set (has been assigned a value) and false otherwise */
  public boolean isSetBackoffTimeMillis() {
    return __isset_bit_vector.get(__BACKOFFTIMEMILLIS_ISSET_ID);
  }

  public void setBackoffTimeMillisIsSet(boolean value) {
    __isset_bit_vector.set(__BACKOFFTIMEMILLIS_ISSET_ID, value);
  }

  public String getExceptionClass() {
    return this.exceptionClass;
  }

  public IOError setExceptionClass(String exceptionClass) {
    this.exceptionClass = exceptionClass;
    return this;
  }

  public void unsetExceptionClass() {
    this.exceptionClass = null;
  }

  /** Returns true if field exceptionClass is set (has been assigned a value) and false otherwise */
  public boolean isSetExceptionClass() {
    return this.exceptionClass != null;
  }

  public void setExceptionClassIsSet(boolean value) {
    if (!value) {
      this.exceptionClass = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case MESSAGE:
      if (value == null) {
        unsetMessage();
      } else {
        setMessage((String)value);
      }
      break;

    case BACKOFF_TIME_MILLIS:
      if (value == null) {
        unsetBackoffTimeMillis();
      } else {
        setBackoffTimeMillis((Long)value);
      }
      break;

    case EXCEPTION_CLASS:
      if (value == null) {
        unsetExceptionClass();
      } else {
        setExceptionClass((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case MESSAGE:
      return getMessage();

    case BACKOFF_TIME_MILLIS:
      return Long.valueOf(getBackoffTimeMillis());

    case EXCEPTION_CLASS:
      return getExceptionClass();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case MESSAGE:
      return isSetMessage();
    case BACKOFF_TIME_MILLIS:
      return isSetBackoffTimeMillis();
    case EXCEPTION_CLASS:
      return isSetExceptionClass();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof IOError)
      return this.equals((IOError)that);
    return false;
  }

  public boolean equals(IOError that) {
    if (that == null)
      return false;

    boolean this_present_message = true && this.isSetMessage();
    boolean that_present_message = true && that.isSetMessage();
    if (this_present_message || that_present_message) {
      if (!(this_present_message && that_present_message))
        return false;
      if (!this.message.equals(that.message))
        return false;
    }

    boolean this_present_backoffTimeMillis = true;
    boolean that_present_backoffTimeMillis = true;
    if (this_present_backoffTimeMillis || that_present_backoffTimeMillis) {
      if (!(this_present_backoffTimeMillis && that_present_backoffTimeMillis))
        return false;
      if (this.backoffTimeMillis != that.backoffTimeMillis)
        return false;
    }

    boolean this_present_exceptionClass = true && this.isSetExceptionClass();
    boolean that_present_exceptionClass = true && that.isSetExceptionClass();
    if (this_present_exceptionClass || that_present_exceptionClass) {
      if (!(this_present_exceptionClass && that_present_exceptionClass))
        return false;
      if (!this.exceptionClass.equals(that.exceptionClass))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(IOError other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    IOError typedOther = (IOError)other;

    lastComparison = Boolean.valueOf(isSetMessage()).compareTo(typedOther.isSetMessage());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMessage()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.message, typedOther.message);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBackoffTimeMillis()).compareTo(typedOther.isSetBackoffTimeMillis());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBackoffTimeMillis()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.backoffTimeMillis, typedOther.backoffTimeMillis);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetExceptionClass()).compareTo(typedOther.isSetExceptionClass());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExceptionClass()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.exceptionClass, typedOther.exceptionClass);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("IOError(");
    boolean first = true;

    sb.append("message:");
    if (this.message == null) {
      sb.append("null");
    } else {
      sb.append(this.message);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("backoffTimeMillis:");
    sb.append(this.backoffTimeMillis);
    first = false;
    if (!first) sb.append(", ");
    sb.append("exceptionClass:");
    if (this.exceptionClass == null) {
      sb.append("null");
    } else {
      sb.append(this.exceptionClass);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class IOErrorStandardSchemeFactory implements SchemeFactory {
    public IOErrorStandardScheme getScheme() {
      return new IOErrorStandardScheme();
    }
  }

  private static class IOErrorStandardScheme extends StandardScheme<IOError> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, IOError struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // MESSAGE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.message = iprot.readString();
              struct.setMessageIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BACKOFF_TIME_MILLIS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.backoffTimeMillis = iprot.readI64();
              struct.setBackoffTimeMillisIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // EXCEPTION_CLASS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.exceptionClass = iprot.readString();
              struct.setExceptionClassIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, IOError struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.message != null) {
        oprot.writeFieldBegin(MESSAGE_FIELD_DESC);
        oprot.writeString(struct.message);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(BACKOFF_TIME_MILLIS_FIELD_DESC);
      oprot.writeI64(struct.backoffTimeMillis);
      oprot.writeFieldEnd();
      if (struct.exceptionClass != null) {
        oprot.writeFieldBegin(EXCEPTION_CLASS_FIELD_DESC);
        oprot.writeString(struct.exceptionClass);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class IOErrorTupleSchemeFactory implements SchemeFactory {
    public IOErrorTupleScheme getScheme() {
      return new IOErrorTupleScheme();
    }
  }

  private static class IOErrorTupleScheme extends TupleScheme<IOError> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, IOError struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetMessage()) {
        optionals.set(0);
      }
      if (struct.isSetBackoffTimeMillis()) {
        optionals.set(1);
      }
      if (struct.isSetExceptionClass()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetMessage()) {
        oprot.writeString(struct.message);
      }
      if (struct.isSetBackoffTimeMillis()) {
        oprot.writeI64(struct.backoffTimeMillis);
      }
      if (struct.isSetExceptionClass()) {
        oprot.writeString(struct.exceptionClass);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, IOError struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.message = iprot.readString();
        struct.setMessageIsSet(true);
      }
      if (incoming.get(1)) {
        struct.backoffTimeMillis = iprot.readI64();
        struct.setBackoffTimeMillisIsSet(true);
      }
      if (incoming.get(2)) {
        struct.exceptionClass = iprot.readString();
        struct.setExceptionClassIsSet(true);
      }
    }
  }

}

