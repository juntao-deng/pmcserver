package net.juniper.jmp.monitor.sender;

import net.juniper.jmp.tracer.common.MonitorConstants;

public class RecordStartSender extends AbstractRequestSender {

	public RecordStartSender() {
		super(MonitorConstants.ACTION_RECORD);
	}

	@Override
	protected void doSend(String requestParam) {
	}

}
