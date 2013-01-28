package org.springframework.amqp;

@SuppressWarnings("serial")
public class BlockingQueueException extends AmqpException {

	public BlockingQueueException(String message) {
		super(message);
	}

	public BlockingQueueException(Throwable cause) {
		super(cause);
	}

	public BlockingQueueException(String message, Throwable cause) {
		super(message, cause);
	}
}
