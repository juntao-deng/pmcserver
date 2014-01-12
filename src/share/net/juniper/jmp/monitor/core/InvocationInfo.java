package net.juniper.jmp.monitor.core;

import java.io.Serializable;

public class InvocationInfo implements Serializable{
	private static final long serialVersionUID = 7430333307121219687L;
	private String method;
	private String className;
	private Object[] params;
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public Object[] getParams() {
		return params;
	}
	public void setParams(Object[] params) {
		this.params = params;
	}
}
