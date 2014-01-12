package net.juniper.jmp.monitor.receiver;

public final class NullResultObject extends ResultObject{
	public static NullResultObject INSTANCE = new NullResultObject();
	private NullResultObject() {
		super(-1, null);
	}
}
