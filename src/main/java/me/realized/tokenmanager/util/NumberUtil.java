package me.realized.tokenmanager.util;

import java.text.DecimalFormat;
import java.util.OptionalDouble;

public final class NumberUtil {

    private static final DecimalFormat COMMA_FORMAT = new DecimalFormat("#,###");

    /**
     * Copy of {@link Long#parseLong(String)} but returns an empty {@link OptionalDouble} instead of throwing a {@link NumberFormatException}.
     *
     * @param s String to parse.
     * @return {@link OptionalDouble} instance with parsed value inside or empty if string is invalid.
     */
    public static OptionalDouble parseLong(final String s) throws NumberFormatException {
        if (s == null) {
            return OptionalDouble.empty();
        }

        long result = 0;
        boolean negative = false;
        int i = 0, len = s.length();
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        if (len > 0) {
            char firstChar = s.charAt(0);
            if (firstChar < '0') {
                if (firstChar == '-') {
                    negative = true;
                    limit = Long.MIN_VALUE;
                } else if (firstChar != '+') {
                    return OptionalDouble.empty();
                }

                if (len == 1) {
                    return OptionalDouble.empty();
                }

                i++;
            }
            multmin = limit / 10;
            while (i < len) {
                digit = Character.digit(s.charAt(i++), 10);

                if (digit < 0) {
                    return OptionalDouble.empty();
                }
                if (result < multmin) {
                    return OptionalDouble.empty();
                }
                result *= 10;
                if (result < limit + digit) {
                    return OptionalDouble.empty();
                }
                result -= digit;
            }
        } else {
            return OptionalDouble.empty();
        }

        return OptionalDouble.of(negative ? result : -result);
    }

    public static String withCommas(final double n) {
        return COMMA_FORMAT.format(n);
    }

    // Source: https://stackoverflow.com/questions/9769554/how-to-convert-number-into-k-thousands-m-million-and-b-billion-suffix-in-jsp
    public static String withSuffix(final double n) {
        if (n < 1000) {
            return "" + n;
        }

        final int exp = (int) (Math.log(n) / Math.log(1000));
        return String.format("%.1f%c", n / Math.pow(1000, exp), "kMBTQ".charAt(exp - 1));
    }


    private NumberUtil() {}
}
