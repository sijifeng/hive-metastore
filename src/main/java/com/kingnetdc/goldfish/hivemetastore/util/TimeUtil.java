package com.kingnetdc.goldfish.hivemetastore.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by jiyc on 2016/12/21.
 */
public class TimeUtil {

	public static String dayBeforeYesterday(Date date) {
		String format = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -2);
		return sdf.format(calendar.getTime());
	}

	public static void main(String[] args) {
		System.out.println(TimeUtil.dayBeforeYesterday(new Date()));
	}
}
