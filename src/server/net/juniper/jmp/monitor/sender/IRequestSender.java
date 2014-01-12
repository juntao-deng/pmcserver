package net.juniper.jmp.monitor.sender;

/**
 * Sending a signal to a share memory channel
 * @author juntaod
 *
 */
public interface IRequestSender {
	public Integer send(String requestParam);
}
