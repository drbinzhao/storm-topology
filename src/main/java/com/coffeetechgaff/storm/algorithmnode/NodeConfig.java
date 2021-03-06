/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.coffeetechgaff.storm.algorithmnode;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class NodeConfig extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2960941586708632949L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NodeConfig\",\"namespace\":\"com.coffeetechgaff.storm.algorithmnode\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Name for the stream. Used for configuration purposes, not related to topic name.\"},{\"name\":\"type\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** Name for the stream. Used for configuration purposes, not related to topic name. */
  @Deprecated public java.lang.String name;
  /** Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator. */
  @Deprecated public java.lang.String type;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public NodeConfig() {}

  /**
   * All-args constructor.
   * @param name Name for the stream. Used for configuration purposes, not related to topic name.
   * @param type Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
   */
  public NodeConfig(java.lang.String name, java.lang.String type) {
    this.name = name;
    this.type = type;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return type;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.String)value$; break;
    case 1: type = (java.lang.String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return Name for the stream. Used for configuration purposes, not related to topic name.
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * Name for the stream. Used for configuration purposes, not related to topic name.
   * @param value the value to set.
   */
  public void setName(java.lang.String value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'type' field.
   * @return Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Sets the value of the 'type' field.
   * Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
   * @param value the value to set.
   */
  public void setType(java.lang.String value) {
    this.type = value;
  }

  /**
   * Creates a new NodeConfig RecordBuilder.
   * @return A new NodeConfig RecordBuilder
   */
  public static com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder newBuilder() {
    return new com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder();
  }

  /**
   * Creates a new NodeConfig RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new NodeConfig RecordBuilder
   */
  public static com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder newBuilder(com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder other) {
    return new com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder(other);
  }

  /**
   * Creates a new NodeConfig RecordBuilder by copying an existing NodeConfig instance.
   * @param other The existing instance to copy.
   * @return A new NodeConfig RecordBuilder
   */
  public static com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder newBuilder(com.coffeetechgaff.storm.algorithmnode.NodeConfig other) {
    return new com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder(other);
  }

  /**
   * RecordBuilder for NodeConfig instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<NodeConfig>
    implements org.apache.avro.data.RecordBuilder<NodeConfig> {

    /** Name for the stream. Used for configuration purposes, not related to topic name. */
    private java.lang.String name;
    /** Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator. */
    private java.lang.String type;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.type)) {
        this.type = data().deepCopy(fields()[1].schema(), other.type);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing NodeConfig instance
     * @param other The existing instance to copy.
     */
    private Builder(com.coffeetechgaff.storm.algorithmnode.NodeConfig other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.type)) {
        this.type = data().deepCopy(fields()[1].schema(), other.type);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'name' field.
      * Name for the stream. Used for configuration purposes, not related to topic name.
      * @return The value.
      */
    public java.lang.String getName() {
      return name;
    }

    /**
      * Sets the value of the 'name' field.
      * Name for the stream. Used for configuration purposes, not related to topic name.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder setName(java.lang.String value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * Name for the stream. Used for configuration purposes, not related to topic name.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'name' field.
      * Name for the stream. Used for configuration purposes, not related to topic name.
      * @return This builder.
      */
    public com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'type' field.
      * Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
      * @return The value.
      */
    public java.lang.String getType() {
      return type;
    }

    /**
      * Sets the value of the 'type' field.
      * Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
      * @param value The value of 'type'.
      * @return This builder.
      */
    public com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder setType(java.lang.String value) {
      validate(fields()[1], value);
      this.type = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'type' field has been set.
      * Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
      * @return True if the 'type' field has been set, false otherwise.
      */
    public boolean hasType() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'type' field.
      * Stream type. Can be namespace scoped reference for a particular type, or could be a generic type indicator.
      * @return This builder.
      */
    public com.coffeetechgaff.storm.algorithmnode.NodeConfig.Builder clearType() {
      type = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public NodeConfig build() {
      try {
        NodeConfig record = new NodeConfig();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.String) defaultValue(fields()[0]);
        record.type = fieldSetFlags()[1] ? this.type : (java.lang.String) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
