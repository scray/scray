package scray.common.errorhandling;

/**
 * Used to abstract away error handling functionality.
 * 
 */
public interface ErrorHandler {

	public void handleError(ScrayProcessableErrors error, LoadCycle loadCycle);
	public void handleError(ScrayProcessableErrors error, LoadCycle loadCycle, String message);
}
