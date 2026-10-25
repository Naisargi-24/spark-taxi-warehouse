package com.naisargi.taxiwarehouse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/** Where each output lives under the --out directory. */
public final class Locations {
    private Locations() { }

    public static String warehouse(String out, String table) {
        return out + "/warehouse/" + table;
    }

    public static String olap(String out, String query) {
        return out + "/olap/" + query;
    }

    public static String bench(String out, String name) {
        return out + "/bench/" + name;
    }

    public static Path report(String out, String file) {
        Path p = Path.of(out, "reports", file);
        try {
            Files.createDirectories(p.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }

    /** Total size in bytes of every file under a local directory. */
    public static long sizeOnDisk(String dir) {
        Path root = Path.of(dir);
        if (!Files.exists(root)) {
            return 0;
        }
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().endsWith(".crc"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).sum();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
