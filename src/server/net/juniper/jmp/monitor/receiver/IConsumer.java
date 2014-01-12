package net.juniper.jmp.monitor.receiver;
/**
 * Consumer contents from share memory channel
 * @author juntaod
 *
 */
public interface IConsumer {
	public void setCurrentSeq(Integer seq);
}
