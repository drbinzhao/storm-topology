/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.coffeetechgaff.storm.datanode;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum Operation {
  CREATE, UPDATE, DELETE, WRONG  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"Operation\",\"namespace\":\"com.coffeetechgaff.storm.datanode\",\"symbols\":[\"CREATE\",\"UPDATE\",\"DELETE\",\"WRONG\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}
