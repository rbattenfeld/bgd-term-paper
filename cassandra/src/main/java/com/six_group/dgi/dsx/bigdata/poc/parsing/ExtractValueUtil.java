package com.six_group.dgi.dsx.bigdata.poc.parsing;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public enum ExtractValueUtil {
	INSTANCE;
	
	public String getValue(final String str, final String key, final boolean throwException) {
		final int begin = str.indexOf(key);
		if (begin >=0 ) {
			final int end = str.indexOf("=", begin);
			if (end > begin) {
				if (str.charAt(end + 1) == '"') {
					final int qouteEnd = str.indexOf('"', end + 2);
					return str.substring(end + 2, qouteEnd);
				} else {
					final int valueEnd = str.indexOf(' ', end);
					return str.substring(end + 1, valueEnd);
				}
			}
		}
		if (throwException) {
			throw new RuntimeException();
		} else {
			return null;
		}
	}
	
	public Date addDays(final Date date, final int days) {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.DATE, days);                 
        return cal.getTime();
    }
	
	public void sleep(final long sleepTime) {
		try {
			Thread.currentThread();
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}			
	}
}
