package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class ThreadInfoSender extends AbstractRequestSender {

	public ThreadInfoSender() {
		super(MonitorConstants.ACTION_THREADINFO);
	}

	@Override
	protected void doSend(String requestParam) {
	}

}
