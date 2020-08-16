package com.fink.demo.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Jiazhi on 2020/6/22.
 */
public class DateUtils {

    public static Long toTimestamp(String time, String format) throws ParseException {
        return new SimpleDateFormat(format).parse(time).getTime();
    }

    public static String format(Date time, String format) throws ParseException {
        return new SimpleDateFormat(format).format(time);
    }

    public static Integer getHour(Long time) throws ParseException {
        return Integer.valueOf(new SimpleDateFormat("HH").format(new Date(time)));
    }

    public static String getDateAndHourAnd10Min(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return sdf.format(date).substring(0, 15) + "0";
    }

    public static Long jumpSeconds(Long time) {
        Date date = new Date(time);
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int milliSecs = calendar.get(Calendar.MILLISECOND);
        time -= milliSecs;
        final int seconds = calendar.get(Calendar.SECOND);
        if (seconds < 30) {
            time += (29 - seconds) * 1000L;
        } else {
            time += (59 - seconds) * 1000L;
        }
        calendar.setTime(new Date(time));
        return calendar.getTime().getTime();
    }
}
