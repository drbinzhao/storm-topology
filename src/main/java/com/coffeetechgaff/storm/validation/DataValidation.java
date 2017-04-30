package com.coffeetechgaff.storm.validation;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;

public class DataValidation{

	public Map<String, String> validateDatasource(DataNode dataSourceNodeRecord){
		Map<String, String> map = new HashMap<>();

		if(StringUtils.isNotBlank(dataSourceNodeRecord.getId())){
			map.put(CommonVertexLabelEnums.ID.getVertexLabelName(), dataSourceNodeRecord.getId());
		}

		if(dataSourceNodeRecord.getDataTypes() != null && !dataSourceNodeRecord.getDataTypes().isEmpty()){
			map.put(CommonVertexLabelEnums.DATATYPE.getVertexLabelName(), dataSourceNodeRecord.getDataTypes()
					.toString());
		}

		if(StringUtils.isNotBlank(dataSourceNodeRecord.getName())){
			map.put(CommonVertexLabelEnums.NAME.getVertexLabelName(), dataSourceNodeRecord.getName());
		}

		if(StringUtils.isNotBlank(dataSourceNodeRecord.getDescription())){
			map.put(CommonVertexLabelEnums.DESCRIPTION.getVertexLabelName(), dataSourceNodeRecord.getDescription());
		}

		if(StringUtils.isNotBlank(dataSourceNodeRecord.getClassification())){
			map.put(CommonVertexLabelEnums.CLASSIFICATION.getVertexLabelName(),
					dataSourceNodeRecord.getClassification());
		}

		if(StringUtils.isNotBlank(dataSourceNodeRecord.getMaturity())){
			map.put(CommonVertexLabelEnums.MATURITY.getVertexLabelName(), dataSourceNodeRecord.getMaturity());
		}
	
		return map;
	}

}
