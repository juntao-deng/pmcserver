package net.juniper.jmp.monitor.extractor;

import java.util.List;

public interface IInfoExtractor <T>{
	public List<T> extract();
}
