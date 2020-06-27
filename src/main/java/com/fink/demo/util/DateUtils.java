package com.fink.demo.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    public static String getHourAnd10Min(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        return sdf.format(date).substring(0, 4) + "0";
    }
}
