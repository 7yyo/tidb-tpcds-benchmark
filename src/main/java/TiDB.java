import com.jakewharton.fliptables.FlipTable;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

@Data
public class TiDB implements Run {

  private String job;

  private String db;
  private Connection conn;
  private Statement sm;
  private ResultSet rs;

  private StringBuilder sql = new StringBuilder();
  private static Map<String, String> skipQueries = new HashMap<>();
  private String queryFolderPath;
  private File[] queryFiles;

  private String[] variables;

  private int success = 0;
  private int failed = 0;
  private List<String> failedQueries = new ArrayList<>();
  private double totalDuration = 0;

  @SneakyThrows
  public TiDB(Source source) {
    job = source.getJob();
    db = source.getDb();
    Class.forName("com.mysql.cj.jdbc.Driver");
    conn =
        DriverManager.getConnection(
            "jdbc:mysql://"
                + source.getHost()
                + "/"
                + source.getDb()
                + "?autoReconnect=true&maxReconnects=99999",
            source.getUser(),
            source.getPassword());
    sm = conn.createStatement();
    variables = source.getVariables().split(";");
    setVariables();
    queryFolderPath = source.getFolderPath();
    queryFiles = Util.sortFolder(source.getFolderPath());
    skipQueries();
  }

  @SneakyThrows
  public void run() {

    File explainFolder = null;
    if (isExplainAnalyze(job)) {
      String parentFolderPath = new File(queryFolderPath).getParent();
      String targetFolderPath = parentFolderPath + "/" + job + "_" + db;
      try {
        Util.deleteFolder(targetFolderPath);
        explainFolder = Util.createFolder(parentFolderPath + "/" + job + "_" + db);
        System.out.println("job is " + job + ", mkdir " + targetFolderPath + " success");
      } catch (Exception e) {
        System.out.println(
            "job is " + job + ", mkdir " + targetFolderPath + " failed, error: " + e.getMessage());
        System.exit(1);
      }
    }

    for (File f : Objects.requireNonNull(queryFiles)) {

      if (shouldSkipQuery(f)) continue;

      try (Stream<String> l = Files.lines(Paths.get(f.getAbsolutePath()))) {
        l.forEach(s -> sql.append(s).append(" \n"));
      } catch (Exception e) {
        System.out.println(f.getName() + "|" + " load failed: " + e.getMessage());
        continue;
      }

      try {

        System.out.print(f.getName());
        if ("explain_analyze".equals(job)) {
          sql.insert(0, "explain analyze ");
        }

        long startTime = System.currentTimeMillis();
        rs = sm.executeQuery(sql.toString());
        double duration = Util.mills2Second(startTime);
        success++;

        if ("tidb".equals(job)) {
          System.out.print(" | " + duration + "s | ");
        } else {
          System.out.println(" | " + duration + "s | ");
        }

        totalDuration += duration;

        File file;
        switch (job) {
          case "tidb":
            int rows = 0;
            while (rs.next()) {
              rows++;
            }
            System.out.println(rows);
            break;
          case "explain_analyze":
            ExplainAnalyze ea = new ExplainAnalyze();
            file =
                new File(
                    Objects.requireNonNull(explainFolder).getAbsolutePath()
                        + "/"
                        + f.getName()
                        + "_"
                        + duration
                        + ".sql");
            FileUtils.write(file, sql.append("\n"), "utf-8");
            FileUtils.write(file, ea.explainAnalyzeTable(rs), "utf-8");
            break;
        }

      } catch (Exception e) {
        if (e.getMessage().contains("Communications link failure")) {
          System.out.println(" | Communications link failure");
          setVariables();
          Thread.sleep(5000);
        } else {
          System.out.println(" | " + e.getMessage());
        }
        if (!e.getMessage().contains("rollup")
            && !e.getMessage().contains("Communications link failure")) {
          Thread.sleep(60000);
        }
        failedQueries.add(f.getName());
        failed++;
      }
      sql.delete(0, sql.length());
    }

    if (rs != null) {
      rs.close();
    }
    sm.close();
    conn.close();

    System.out.println(
        "total duration:" + totalDuration + "s | success: " + success + " | failed:" + failed);
    System.out.println("failed queries: ");
    for (String query : failedQueries) {
      System.out.println(query);
    }
  }

  @SneakyThrows
  public void setVariables() {
    for (String v : variables) {
      try {
        sm.execute(v);
      } catch (Exception e) {
        setVariables();
      }
    }
  }

  public static void skipQueries() {
    skipQueries.put("38", "https://github.com/pingcap/tidb/issues/27745");
  }

  public static boolean isExplainAnalyze(String job) {
    return "explain_analyze".equals(job);
  }

  public boolean shouldSkipQuery(File queryFile) {
    for (Map.Entry<String, String> entry : skipQueries.entrySet()) {
      if (queryFile.getName().contains(entry.getKey()) && (!"explain".equals(job))) {
        System.out.println(queryFile.getName() + " | " + entry.getValue());
        return true;
      }
    }
    return false;
  }
}

@Data
class ExplainAnalyze {

  private static final String[][] rows = new String[1][9];
  private static final String[] header =
      new String[] {
        "id",
        "estRows",
        "actRows",
        "task",
        "access object",
        "execution info",
        "operator info",
        "memory",
        "disk"
      };
  private StringBuilder id = new StringBuilder();
  private StringBuilder estRows = new StringBuilder();
  private StringBuilder actRows = new StringBuilder();
  private StringBuilder task = new StringBuilder();
  private StringBuilder accessObject = new StringBuilder();
  private StringBuilder executionInfo = new StringBuilder();
  private StringBuilder operatorInfo = new StringBuilder();
  private StringBuilder memory = new StringBuilder();
  private StringBuilder disk = new StringBuilder();

  @SneakyThrows
  public String explainAnalyzeTable(ResultSet rs) {
    while (rs.next()) {
      id.append(rs.getString(1)).append("\n");
      estRows.append(rs.getString(2)).append("\n");
      actRows.append(rs.getString(3)).append("\n");
      task.append(rs.getString(4)).append("\n");
      accessObject.append(rs.getString(5)).append("\n");
      executionInfo.append(rs.getString(6)).append("\n");
      operatorInfo.append(rs.getString(7)).append("\n");
      memory.append(rs.getString(8)).append("\n");
      disk.append(rs.getString(9)).append("\n");
    }
    rows[0][0] = id.toString();
    rows[0][1] = estRows.toString();
    rows[0][2] = actRows.toString();
    rows[0][3] = task.toString();
    rows[0][4] = accessObject.toString();
    rows[0][5] = executionInfo.toString();
    rows[0][6] = operatorInfo.toString();
    rows[0][7] = memory.toString();
    rows[0][8] = disk.toString();

    return FlipTable.of(header, rows);
  }
}
