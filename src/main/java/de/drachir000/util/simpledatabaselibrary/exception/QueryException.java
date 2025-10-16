package de.drachir000.util.simpledatabaselibrary.exception;

/**
 * Exception thrown when query execution fails.
 */
public class QueryException extends DatabaseException {
	
	public QueryException(String message, Throwable cause) {
		super(message, cause);
	}
	
}