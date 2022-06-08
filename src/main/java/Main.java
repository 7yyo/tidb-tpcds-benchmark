import lombok.SneakyThrows;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        if (System.getProperty("f") == null) {
            System.setProperty("f", "src/main/resources/tpcds.properties");
        }
        Map<String, String> properties = loadProperties(System.getProperty("f"));
        Source source = new Source(properties);
        Objects.requireNonNull(RunFactory.getRun(properties.get("task"))).run(source, properties);
    }

    @SneakyThrows
    public static Map<String, String> loadProperties(String path) {
        Properties properties = new Properties();
        InputStream inputStream = new BufferedInputStream(Files.newInputStream(Paths.get(path)));
        properties.load(inputStream);
        Map<String, String> map = new HashMap<>();
        System.out.print("\n[variables]\n");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String k = entry.getKey().toString();
            String v = entry.getValue().toString();
            map.put(k, v);
            if ("variables".equals(k)) {
                System.out.println("variables = ");
                for (String variable : v.split(";")) {
                    System.out.println("    " + variable);
                }
            } else {
                System.out.println(k + ": " + v);
            }
        }
        System.out.println();
        return map;
    }
}

class RunFactory {

    public static Run getRun(String k) {
        switch (k) {
            case "row":
            case "analyze":
                return new Table();
            case "replace":
                return new Replace();
            case "tidb":
            case "explain":
            case "explain_analyze":
                return new TiDBBench();
            case "tispark":
                return new TiSparkBench();
            default:
                return null;
        }
    }

}
