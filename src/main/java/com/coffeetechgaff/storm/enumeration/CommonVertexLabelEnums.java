package com.coffeetechgaff.storm.enumeration;

/**
 * 
 * @author VivekSubedi
 *
 */
public enum CommonVertexLabelEnums{

	ID("id"), NAME("name"), STATUS("status"), DESCRIPTION("description"), AUTHOR("author"), EMAIL("email"), VERSION(
			"version"), DATATYPE("dataTypes"), INPUT("input"), OUTPUT("output"), PARAMETERS("parameters"), CLASSIFICATION(
			"classification"), MATURITY("maturity");

	private String vertexLabelName;

	private CommonVertexLabelEnums(String vertexLabelName){
		this.vertexLabelName = vertexLabelName;
	}

	public String getVertexLabelName(){
		return vertexLabelName;
	}

	public static CommonVertexLabelEnums fromValue(String value){
		for(CommonVertexLabelEnums b : CommonVertexLabelEnums.values()){
			if(b.getVertexLabelName().equals(value)){
				return b;
			}
		}
		return null;
	}
}
