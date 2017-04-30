package com.coffeetechgaff.storm.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.algorithmnode.AlgorithmParameter;
import com.coffeetechgaff.storm.algorithmnode.NodeConfig;
import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;

public class AlgorithmValidation{

	/**
	 * Makes sure the fields in the AnalyticDefinition are not null. The null
	 * fields are set to the empty string. This modifies the AnalyticDefinition
	 * parameter in-place.
	 *
	 * @param analyticDefinition
	 *            the AnalyticDefinition to be validated
	 * @return analyticDefinition
	 */

	public Map<String, String> validateAnalytic(AlgorithmNode analyticDefinition){

		Map<String, String> map = new HashMap<>();

		if(StringUtils.isNotBlank(analyticDefinition.getUid())){
			map.put(CommonVertexLabelEnums.ID.getVertexLabelName(), analyticDefinition.getUid());
		}

		if(StringUtils.isNotBlank(analyticDefinition.getName())){
			map.put(CommonVertexLabelEnums.NAME.getVertexLabelName(), analyticDefinition.getName());
		}

		if(StringUtils.isNotBlank(analyticDefinition.getDescription())){
			map.put(CommonVertexLabelEnums.DESCRIPTION.getVertexLabelName(), analyticDefinition.getDescription());
		}

		if(StringUtils.isNotBlank(analyticDefinition.getAuthor())){
			map.put(CommonVertexLabelEnums.AUTHOR.getVertexLabelName(), analyticDefinition.getAuthor());
		}

		if(StringUtils.isNotBlank(analyticDefinition.getEmail())){
			map.put(CommonVertexLabelEnums.EMAIL.getVertexLabelName(), analyticDefinition.getEmail());
		}

		if(StringUtils.isNotBlank(analyticDefinition.getVersion())){
			map.put(CommonVertexLabelEnums.VERSION.getVertexLabelName(), analyticDefinition.getVersion());
		}

		return validateInputOutputAndParameters(analyticDefinition, map);

	}

	private Map<String, String> validateInputOutputAndParameters(AlgorithmNode analyticDefinition,
			Map<String, String> map){
		if(analyticDefinition.getInput() != null && !analyticDefinition.getInput().isEmpty()){
			JSONArray newInputArray = validateInputOutput(analyticDefinition.getInput());
			if(newInputArray.length() > 0){
				map.put(CommonVertexLabelEnums.INPUT.getVertexLabelName(), newInputArray.toString());
			}
		}

		if(analyticDefinition.getOutput() != null && !analyticDefinition.getOutput().isEmpty()){
			JSONArray newOutputArray = validateInputOutput(analyticDefinition.getOutput());
			if(newOutputArray.length() > 0){
				map.put(CommonVertexLabelEnums.OUTPUT.getVertexLabelName(), newOutputArray.toString());
			}
		}

		if(analyticDefinition.getParameters() != null && !analyticDefinition.getParameters().isEmpty()){
			JSONArray newParameterArray = validateParameters(analyticDefinition.getParameters());
			if(newParameterArray.length() > 0){
				map.put(CommonVertexLabelEnums.PARAMETERS.getVertexLabelName(), newParameterArray.toString());
			}
		}

		return map;
	}

	private JSONArray validateParameters(List<AlgorithmParameter> parameterList){
		JSONArray array = new JSONArray();
		for(AlgorithmParameter analyticParameter : parameterList){
			Map<String, String> valueMap = new HashMap<>();
			if(StringUtils.isNotBlank(analyticParameter.getDefaultValue())){
				valueMap.put("defaultValue", analyticParameter.getDefaultValue());
			}
			if(StringUtils.isNotBlank(analyticParameter.getName())){
				valueMap.put("name", analyticParameter.getName());
			}
			if(StringUtils.isNotBlank(analyticParameter.getDescription())){
				valueMap.put("description", analyticParameter.getDescription());
			}
			if(StringUtils.isNotBlank(analyticParameter.getUiHint())){
				valueMap.put("uiHint", analyticParameter.getUiHint());
			}
			if(StringUtils.isNotBlank(analyticParameter.getType())){
				valueMap.put("type", analyticParameter.getType());
			}
			if(StringUtils.isNotBlank(analyticParameter.getEntity())){
				valueMap.put("entity", analyticParameter.getEntity());
			}

			// checking if the map is null or empty
			if(MapUtils.isNotEmpty(valueMap)){
				array.put(new JSONObject(valueMap));
			}
		}

		return array;
	}

	private JSONArray validateInputOutput(List<NodeConfig> inputOutputList){
		JSONArray array = new JSONArray();
		for(NodeConfig streamConfig : inputOutputList){
			Map<String, String> valueMap = new HashMap<>();
			if(StringUtils.isNotBlank(streamConfig.getName())){
				valueMap.put("name", streamConfig.getName());
			}
			if(StringUtils.isNotBlank(streamConfig.getType())){
				valueMap.put("type", streamConfig.getType());
			}
			// checking if the map is null or empty
			if(MapUtils.isNotEmpty(valueMap)){
				array.put(new JSONObject(valueMap));
			}
		}
		return array;
	}

}
