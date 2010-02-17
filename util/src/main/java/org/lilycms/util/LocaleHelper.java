package org.lilycms.util;

import java.util.Locale;
import java.util.StringTokenizer;

public class LocaleHelper {
    public static Locale parseLocale(String localeString) {
        StringTokenizer localeParser = new StringTokenizer(localeString, "-_");

        String lang = null, country = null, variant = null;

        if (localeParser.hasMoreTokens())
            lang = localeParser.nextToken();
        if (localeParser.hasMoreTokens())
            country = localeParser.nextToken();
        if (localeParser.hasMoreTokens())
            variant = localeParser.nextToken();

        if (lang != null && country != null && variant != null)
            return new Locale(lang, country, variant);
        else if (lang != null && country != null)
            return new Locale(lang, country);
        else if (lang != null)
            return new Locale(lang);
        else
            return new Locale("");
    }

    public static String getString(Locale locale) {
        return getString(locale, "_");
    }

    public static String getString(Locale locale, String separator) {
        boolean hasLanguage = !locale.getLanguage().equals("");
        boolean hasCountry = !locale.getCountry().equals("");
        boolean hasVariant = !locale.getVariant().equals("");

        if (hasLanguage && hasCountry && hasVariant)
            return locale.getLanguage() + separator + locale.getCountry() + separator + locale.getVariant();
        else if (hasLanguage && hasCountry)
            return locale.getLanguage() + separator + locale.getCountry();
        else if (hasLanguage)
            return locale.getLanguage();
        else
            return "";
    }
}
