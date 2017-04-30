package com.coffeetechgaff.storm.enumeration;

/**
 * The label of vertex itself
 * 
 * @author VivekSubedi
 *
 */
public enum MainVertexName{

	DATASOURCE("datasource"), ANALYTIC("analytic");

	private String vertexLabelName;

	private MainVertexName(String vertexLabelName){
		this.vertexLabelName = vertexLabelName;
	}

	public String getVertexLabelName(){
		return vertexLabelName;
	}

	public static MainVertexName fromValue(String value){
		for(MainVertexName b : MainVertexName.values()){
			if(b.getVertexLabelName().equals(value)){
				return b;
			}
		}
		return null;
	}

}
