package org.lilyproject.rowlog.api;

/**
 * A processor notify observers is interested in getting notified when a message is available for processing.
 * 
 * <p>The {@link RowLogConfigurationManager} is used to communicate the notifications from the {@link RowLog}
 * to the {@link RowLogProcessor}. The {@link RowLogProcessor} implements this interface and registers itself
 * on the {@link RowLogConfigurationManager} 
 */
public interface ProcessorNotifyObserver {
	/**
	 * Method to be called when a message has been posted on the rowlog that needs to be processed.
	 */
	void notifyProcessor();
}
