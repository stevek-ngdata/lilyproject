package org.lilycms.repository.api;

public interface SecondaryTask {

	String getId();
	String getInitialState();
	boolean isDone(String state);
	String execute();
}
