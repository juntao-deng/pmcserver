package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class StateInfoSender extends AbstractRequestSender {

	public StateInfoSender() {
		super(MonitorConstants.ACTION_STATE);
	}

	@Override
	protected void doSend(String requestParam) {
	}

}
