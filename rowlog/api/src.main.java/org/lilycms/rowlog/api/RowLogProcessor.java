package org.lilycms.rowlog.api;

public interface RowLogProcessor {
	void start();
	void stop();
	boolean isRunning();
}
