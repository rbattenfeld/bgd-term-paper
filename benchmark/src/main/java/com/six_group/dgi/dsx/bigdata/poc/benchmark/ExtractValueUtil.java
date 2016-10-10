package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
	
    public Date addHours(final Calendar cal1, final Calendar cal2, final Date date, final int hours) {
        cal1.setTime(date);
        cal1.set(Calendar.YEAR, cal2.get(Calendar.YEAR));
        cal1.set(Calendar.MONTH, cal2.get(Calendar.MONTH));
        cal1.set(Calendar.DAY_OF_MONTH, cal2.get(Calendar.DAY_OF_MONTH));
        cal1.add(Calendar.HOUR, hours);                 
        return cal1.getTime();
    }
	   
	public Date addDays(final Date date, final int days) {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);      
        cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);           
        return cal.getTime();
    }
	   
    public Date zeroMinSecMilli(final Date date) {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);           
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

	public synchronized String getDateAsString(final Date date) {
		final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormatter.format(date);
	}

	protected String getDateFromFileName(final String fileName) {
        final String[] items = fileName.split("_", -1);
        if (items.length == 3) {
            return items[2].substring(0, 8);
        }
        throw new RuntimeException("Cannot extract business date from filename: " + fileName);
    }
	
	public List<String> fileList(final String pathToDirOrFile) {
		final List<String> fileNames = new ArrayList<>();
		final File pathTo = new File(pathToDirOrFile);
		if (pathTo.isDirectory()) {	        
	        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(pathToDirOrFile))) {
	            for (Path path : directoryStream) {
	                fileNames.add(path.toString());
	            }
	        } catch (IOException ex) {
	        	ex.printStackTrace();
	        }
		} else {
			fileNames.add(pathToDirOrFile);
		}
        return fileNames;
    }
	
	public void addToMap(final Map<BigDecimal, String> decimalMap, final String venue, BigDecimal value) {
		if (value != null) {
			final String vv = decimalMap.get(value);
			if (vv != null) {
				decimalMap.put(value, vv + "=" + venue);
			} else {
				decimalMap.put(value, venue);
			}
		}
	}
	
	public Map<BigDecimal, String> sortMap(final Map<BigDecimal, String> decimalMap, final boolean reversedOrder) {
		final Map<BigDecimal, String> result = new LinkedHashMap<>();
		if (reversedOrder) {
			decimalMap.entrySet().stream()
			    .sorted(Map.Entry.<BigDecimal, String>comparingByKey().reversed())
			    .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
		} else {
			decimalMap.entrySet().stream()
			    .sorted(Map.Entry.<BigDecimal, String>comparingByKey())
			    .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
		}
		return result;
	}
	

	public String getValuesAsString(final Map<BigDecimal, String> decimalMap) {
		StringBuffer buf = new StringBuffer();
    	for (Map.Entry<BigDecimal, String> entry : decimalMap.entrySet()) {
			if (buf.length() > 0) {
				buf = buf.append(", ");
			}
			buf.append(entry.getValue());
		}
    	if (buf.length() > 0) {
    		return buf.toString();
    	} else {
    		return "EMPTY";
    	}
	}
}
