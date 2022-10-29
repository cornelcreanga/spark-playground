package com.creanga.playground.spark.parsers;


import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;

/**
 * not thread safe
 * 2020-01-27T13:54:51Z
 * 2019-12-19T16:03:37.263+00:00
 * 2020-03-25T10:03:10.025-05:00
 */
public class Iso8601TimeStampFormat extends DateFormat {

    private final Calendar calendar = Calendar.getInstance();


    private static int parse4D(String str, int index) {
        return (1000 * (str.charAt(index) - '0'))
                + (100 * (str.charAt(index+1) - '0'))
                + (10 * (str.charAt(index+2) - '0'))
                + (str.charAt(index+3) - '0');
    }

    private static int parse3D(String str, int index) {
        return (100 * (str.charAt(index+1) - '0'))
                + (10 * (str.charAt(index+2) - '0'))
                + (str.charAt(index+3) - '0');
    }

    private static int parse2D(String str, int index) {
        return (10 * (str.charAt(index) - '0'))
                + (str.charAt(index+1) - '0');
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        return null;
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
        //calendar.clear();
        try {
            if (source.length() < 19)
                return null;
            int start = pos.getIndex();
            int year = parse4D(source, start);
            int month = parse2D(source, start + 5) - 1;
            int day = parse2D(source, start + 8);
            int hour = parse2D(source, start + 11);
            int minute = parse2D(source, start + 14);
            int second = parse2D(source, start + 17);
            calendar.clear();
            if (source.length() < 21) {
                calendar.set(year, month, day, hour, minute, second);
                pos.setIndex(start + 21);
            } else {
                int milliseconds = parse3D(source, start + 19);
                char sign = source.charAt(start + 23);
                int hourOffset = parse2D(source, start + 24);
                int minOffset = parse2D(source, start + 27);
                int offsetSecs = hourOffset * 3600 + minOffset * 60;

                if (sign == '-') {
                    offsetSecs *= -1000;
                } else {
                    offsetSecs *= 1000;
                }
                calendar.set(Calendar.ZONE_OFFSET, offsetSecs);
                calendar.set(Calendar.DST_OFFSET, 0);
                calendar.set(year, month, day, hour, minute, second);
                calendar.set(Calendar.MILLISECOND, milliseconds);
                pos.setIndex(start + 30);
            }

            return calendar.getTime();
        }catch (StringIndexOutOfBoundsException s){
            return null;
        }
    }

    public static void main(String[] args) throws Exception{
        Iso8601TimeStampFormat format = new Iso8601TimeStampFormat();

//        System.out.println(format.parse("2019-12-19"));
        System.out.println(format.parse("2020-01-27T13:54:51Z"));
        System.out.println(format.parse("2019-12-19T16:03:37.263+00:00"));
        System.out.println(format.parse("2020-03-25T10:03:10.025-05:00"));

    }
}
