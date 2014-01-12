package net.juniper.jmp.monitor.service;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import net.juniper.jmp.monitor.core.InvocationInfo;
import net.juniper.jmp.monitor.httpserver.PmcServer;
import net.juniper.jmp.monitor.service.impl.MonitorServiceImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceFactory {
	private static Logger logger = LoggerFactory.getLogger(PmcServer.class);
	private static Map<String, Object> serviceMap = new HashMap<String, Object>();
	static{
		serviceMap.put(IMonitorService.class.getName(), new MonitorServiceImpl());
	}
	public static Object getService(String serviceName){
		return serviceMap.get(serviceName);
	}
	
	public static byte[] execute(InvocationInfo info){
		String serviceName = info.getClassName();
		String method = info.getMethod();
		Object[] params = info.getParams();
		Object service = getService(serviceName);
		if(service == null)
			return null;
		Method m = getMethod(service.getClass(), method);
		if(m == null)
			return null;
		try {
			return (byte[]) m.invoke(service, params);
		} 
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}
	
	private static Method getMethod(Class<?> c, String name){
		Method[] methods = c.getMethods();
		for(int i = 0; i < methods.length; i ++){
			if(methods[i].getName().equals(name))
				return methods[i];
		}
		return null;
	}
}
