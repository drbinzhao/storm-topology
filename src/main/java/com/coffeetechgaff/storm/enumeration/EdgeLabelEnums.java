package com.coffeetechgaff.storm.enumeration;

/**
 * The Property of Edge that is being used to define relationship between
 * datasource and analytic vertices
 * 
 * @author VivekSubedi
 *
 */
public enum EdgeLabelEnums{

	KNOWS("knows"), WORKSWITH("works with");

	private String edgeLabelName;

	private EdgeLabelEnums(String edgeLabelName){
		this.edgeLabelName = edgeLabelName;
	}

	public String getEdgeLabelName(){
		return edgeLabelName;
	}

	public static EdgeLabelEnums fromValue(String value){
		for(EdgeLabelEnums b : EdgeLabelEnums.values()){
			if(b.getEdgeLabelName().equals(value)){
				return b;
			}
		}
		return null;
	}
}
