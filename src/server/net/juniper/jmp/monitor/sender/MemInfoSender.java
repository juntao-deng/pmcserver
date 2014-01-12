package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class MemInfoSender extends AbstractRequestSender {

	public MemInfoSender() {
		super(MonitorConstants.ACTION_MEMINFO);
	}

	@Override
	protected void doSend(String requestParam) {
	}

}
