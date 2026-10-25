package com.naisargi.taxiwarehouse;

import java.util.HashMap;
import java.util.Map;

/** Minimal "--key value" argument parser. */
public final class Args {
    private final Map<String, String> values;

    private Args(Map<String, String> values) {
        this.values = values;
    }

    public static Args parse(String[] argv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            if (!a.startsWith("--")) {
                throw new IllegalArgumentException("Expected --option, got: " + a);
            }
            if (i + 1 >= argv.length || argv[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + a);
            }
            map.put(a.substring(2), argv[++i]);
        }
        return new Args(map);
    }

    public String get(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    public String getOrNull(String key) {
        return values.get(key);
    }

    public int getInt(String key, int fallback) {
        String v = values.get(key);
        return v == null ? fallback : Integer.parseInt(v);
    }
}
