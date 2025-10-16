package de.drachir000.util.simpledatabaselibrary.exception;

/**
 * Base exception for all database-related errors.
 * Provides consistent error handling across the framework.
 */
public class DatabaseException extends Exception {
	
	public DatabaseException(String message) {
		super(message);
	}
	
	public DatabaseException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
