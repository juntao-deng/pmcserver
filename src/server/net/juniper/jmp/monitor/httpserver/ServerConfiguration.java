package net.juniper.jmp.monitor.httpserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServerConfiguration {
	public static final String NODE_IP = "NODE_IP";
	public static final String NODE_PORT = "NODE_PORT";
	public static final String NODE_ALIVE = "SERVER_ALIVE";
	
	public static final String PMC_SERVER_IP = "PMC_SERVER_IP";
	public static final String PMC_SERVER_PORT = "PMC_SERVER_PORT";
	
	private static ServerConfiguration instance = new ServerConfiguration();
	private Map<String, String> configMap = new ConcurrentHashMap<String, String>();
	public static ServerConfiguration getInstance() {
		return instance;
	}
	public void setConfig(String key, String value){
		configMap.put(key, value);
	}
	
	public String getConfig(String key){
		return configMap.get(key);
	}
}
