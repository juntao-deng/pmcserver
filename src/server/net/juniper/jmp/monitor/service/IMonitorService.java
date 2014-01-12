package net.juniper.jmp.monitor.service;
/**
 * 
 * @author juntaod
 *
 */
public interface IMonitorService {
	public byte[] getThreadInfos();
	public byte[] getPeriodThreadInfos(String startTs, String endTs, String wherePart);
	public byte[] getSqlInfos(String startTs, String endTs, String wherePart);
	public byte[] getStagesByParentId(String pid);
	public byte[] getStageById(String callId);
	public byte[] getCpuInfo();
	public byte[] getMemInfo();
	/**
	 * Get if node is monitored
	 * @return
	 */
	public byte[] getState();
	/**
	 * Get if node server (jboss) is started. Either it is monitored or not
	 * @return
	 */
	public byte[] getNodeState();
	public byte[] getServerState();
	public byte[] startRecord(String recordId);
	public byte[] endRecord(String recordId);
	public byte[] getRecordResult(String recordId);
}
