package net.juniper.jmp.monitor.receiver;

/**
 * A singleton result stands for waiting
 * 
 * @author juntaod
 *
 */
public final class SuggestWaiting extends ResultObject{
	public static SuggestWaiting INSTANCE = new SuggestWaiting();
	private SuggestWaiting() {
		super(-1, null);
	}
}
