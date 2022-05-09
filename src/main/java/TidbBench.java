import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;
import java.util.stream.Stream;

public class TidbBench {

    @SneakyThrows
    public static void main(String[] args) {

        String host = System.getProperty("h");
        String user = System.getProperty("u");
        String password = System.getProperty("p");
        String db = System.getProperty("db");
        boolean set = Boolean.parseBoolean(System.getProperty("s"));
//        set = true;
        String path = System.getProperty("f");
//        path = "/Users/yuyang/Downloads/sql";

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://" + host + "/" + db + "?autoReconnect=true&maxReconnects=99999", user, password);
//        Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:4000/test?useServerPrepStmts=true&cachePrepStmts=true", "root", "");
        Statement sm = conn.createStatement();
        ResultSet rs;
        if (set) {
            sm.execute("set @@tidb_isolation_read_engines='tiflash';");
            sm.execute("set @@tidb_allow_mpp=1;");
            sm.execute("set @@tidb_mem_quota_query = 10 << 30;");
            System.out.println("#########");
            rs = sm.executeQuery("show variables like 'tidb_isolation_read_engines';");
            while (rs.next()) {
                System.out.println("tidb_isolation_read_engines = " + rs.getString("value"));
            }
            rs = sm.executeQuery("show variables like 'tidb_allow_mpp';");
            while (rs.next()) {
                System.out.println("tidb_allow_mpp = " + rs.getString("value"));
            }
            rs = sm.executeQuery("show variables like 'tidb_mem_quota_query';");
            while (rs.next()) {
                System.out.println("tidb_mem_quota_query = " + rs.getString("value"));
            }
            System.out.println("#########");
        }

        File folder = new File(path);
        StringBuffer sql = new StringBuffer();
        for (File f : Objects.requireNonNull(folder.listFiles())) {

            Stream<String> lines = Files.lines(Paths.get(f.getAbsolutePath()));
            lines.forEach(s -> {
                if (!s.startsWith("--")) {
                    sql.append(s).append(" ");
                }
            });
            long startTime = System.currentTimeMillis();
            String s = sql.toString().replaceAll(";", "");
            try {
                System.out.println("start execute query: " + f.getAbsolutePath());
                rs = sm.executeQuery(sql.toString());
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                System.out.println("fetch rows = " + count + ", duration = " + (System.currentTimeMillis() - startTime) + "ms");
                System.out.println();
            } catch (Exception e) {
                System.out.println(f.getAbsolutePath() + " execute failed, error = " + e);
                System.out.println();
            }
            sql.delete(0, sql.length());
        }
    }

}
