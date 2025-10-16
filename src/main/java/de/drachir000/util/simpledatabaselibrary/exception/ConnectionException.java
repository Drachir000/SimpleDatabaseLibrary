package de.drachir000.util.simpledatabaselibrary.exception;

/**
 * Exception thrown when connection to database fails.
 */
public class ConnectionException extends DatabaseException {
	
	public ConnectionException(String message, Throwable cause) {
		super(message, cause);
	}
	
}