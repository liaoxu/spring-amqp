package org.springframework.amqp;

/**
 * this exception should be caught by MessageListenerContainer which should
 * requeue the message to local queue and retry some time later
 * 
 * @author liaoxu
 * 
 */
@SuppressWarnings("serial")
public class ShouldRetryLocallyException extends AmqpException {

	public ShouldRetryLocallyException(String message) {
		super(message);
	}

	public ShouldRetryLocallyException(Throwable cause) {
		super(cause);
	}

	public ShouldRetryLocallyException(String message, Throwable cause) {
		super(message, cause);
	}
}
