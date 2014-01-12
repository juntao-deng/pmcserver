package net.juniper.jmp.monitor.receiver;
/**
 * The result object for share memory
 * @author juntaod
 *
 */
public class ResultObject {
	private byte[] result;
	private Integer actionSeq;
	public ResultObject(Integer actionSeq, byte[] result) {
		this.actionSeq = actionSeq;
		this.result = result;
	}
	
	public byte[] getResult() {
		return result;
	}

	public int count() {
		return result == null ? 0 : result.length;
	}

	public Integer getActionReq() {
		return actionSeq;
	}
	
}
