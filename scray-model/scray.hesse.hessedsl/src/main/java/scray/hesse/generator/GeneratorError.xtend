package scray.hesse.generator

/**
 * Exception that is thrown if a generator encounters an unrecoverable error
 */
class GeneratorError extends RuntimeException {
	
	new(String msg) {
		super(msg)
	}

	new(String msg, Throwable cause) {
		super(msg, cause)
	}
}