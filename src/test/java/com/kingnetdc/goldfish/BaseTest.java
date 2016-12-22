package com.kingnetdc.goldfish;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

/**
 * Created by jiyc on 2016/12/22.
 */
public class BaseTest {
	@Test
	public void testMap() {
		Map<String, String> map = Maps.newHashMap();
		map.remove("1");
	}
}
