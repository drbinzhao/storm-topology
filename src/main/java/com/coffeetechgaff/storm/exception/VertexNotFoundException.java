package com.coffeetechgaff.storm.exception;

public class VertexNotFoundException extends ExampleTopologyException{

	private static final long serialVersionUID = 3592461898097898105L;

	public VertexNotFoundException(String message){
		super(message);
	}
}
