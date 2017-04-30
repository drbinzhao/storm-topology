package com.coffeetechgaff.storm.exception;

public class ObjectIsNullException extends ExampleTopologyException{

	private static final long serialVersionUID = 6118136235181078852L;

	public ObjectIsNullException(String message){
		super(message);
	}
}
