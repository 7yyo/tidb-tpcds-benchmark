import lombok.Data;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

public class TiDBBench implements Run {

    @SneakyThrows
    public void run(Source source, Map<String, String> properties) {

        String task = properties.get("task");

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://" + source.getHost() + "/" + source.getDb() + "?autoReconnect=true&maxReconnects=99999", source.getUser(), source.getPassword());
        Statement sm = conn.createStatement();
        ResultSet rs = null;

        String[] variables = source.getVariables().split(";");
        executeVariables(sm, variables);

        double totalDuration = 0;
        int success = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();

        File folder = new File(source.getFolderPath());
        File[] files = folder.listFiles();
        Arrays.sort(Objects.requireNonNull(files), new CompareByNo());

        StringBuffer sql = new StringBuffer();
        for (File f : Objects.requireNonNull(files)) {

            if (f.getName().contains("38")) {
                System.out.println("query_38.sql | see detail https://github.com/pingcap/tidb/issues/27745");
                continue;
            }

            try (Stream<String> ls = Files.lines(Paths.get(f.getAbsolutePath()))) {
                ls.forEach(s -> {
                    if (!s.startsWith("--")) {
                        sql.append(s).append(" ");
                    }
                });
            } catch (Exception e) {
                System.out.println(f.getName() + "|" + " load failed: " + e.getMessage());
                continue;
            }

            long startTime;
            try {

                System.out.print(f.getName());
                startTime = System.currentTimeMillis();
                switch (task) {
                    case "tidb":
                        rs = sm.executeQuery(sql.toString());
                        break;
                    case "explain_analyze":
                        rs = sm.executeQuery("explain analyze " + sql);
                        break;
                }
                double duration = Util.mills2Second(startTime);
                System.out.print(" | " + duration + " | ");
                totalDuration += duration;
                success++;

                switch (task) {
                    case "tidb":
                        int rows = 0;
                        while (rs.next()) {
                            rows++;
                        }
                        System.out.println(rows);
                        break;
                    case "explain_analyze":
                        int index = 0;
                        ExplainAnalyze ea = new ExplainAnalyze();

                        String[][] row = new String[9999][9];
                        while (rs.next()) {
                            for (int i = 1; i < 10; i++) {
                                row[index][i - 1] = rs.getString(i);
                            }
                            index++;
                        }

                        ea.setIndex(index);
                        ea.setRows(row);
                        row = new String[ea.getIndex()][9];
                        System.arraycopy(ea.getRows(), 0, row, 0, row.length);
                        System.out.println(Util.explainAnalyzeTable(row));
                        break;
                }

            } catch (Exception e) {
                if (e.getMessage().contains("Communications link failure")) {
                    System.out.println(" | Communications link failure");
                    executeVariables(sm, variables);
                } else {
                    System.out.println(" | " + e.getMessage());
                }
                if (!e.getMessage().contains("rollup") && !e.getMessage().contains("Communications link failure")) {
                    Thread.sleep(60000);
                }
                errors.add(f.getName());
                failed++;
            }
            sql.delete(0, sql.length());
        }

        if (rs != null) {
            rs.close();
        }
        sm.close();
        conn.close();

        System.out.println("total duration:" + totalDuration + "s | success: " + success + " | failed:" + failed);
        System.out.println("failed queries: ");
        for (String error : errors) {
            System.out.println(error);
        }

    }

    @SneakyThrows
    public static void executeVariables(Statement sm, String[] variables) {
        for (String variable : variables) {
            try {
                sm.execute(variable);
            } catch (Exception e) {
                executeVariables(sm, variables);
            }
        }
    }

}

class CompareByNo implements Comparator<File> {

    @Override
    public int compare(File o1, File o2) {
        int a = Integer.parseInt(o1.getName().split("_")[1].split("\\.")[0]);
        int b = Integer.parseInt(o2.getName().split("_")[1].split("\\.")[0]);
        return Integer.compare(a, b);
    }

}

@Data
class ExplainAnalyze {
    private int index;
    private String[][] rows;
}

