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

public class TiDB implements Run {

  @SneakyThrows
  public void run(Source source, Map<String, String> properties) {

    String t = properties.get("task");

    Class.forName("com.mysql.cj.jdbc.Driver");
    Connection conn =
        DriverManager.getConnection(
            "jdbc:mysql://"
                + source.getHost()
                + "/"
                + source.getDb()
                + "?autoReconnect=true&maxReconnects=99999",
            source.getUser(),
            source.getPassword());
    Statement sm = conn.createStatement();
    ResultSet rs = null;
    executeVariables(sm, source);

    double totalDuration = 0;
    int success = 0;
    int failed = 0;
    List<String> errors = new ArrayList<>();

    File explainFolder = null;
    if ("explain".equals(t) || "explain_analyze".equals(t)) {
      String parentFile = new File(properties.get("file")).getParent();
      Util.deleteFolder(parentFile + "/" + t + "_" + source.getDb());
      explainFolder = Util.createFolder(parentFile + "/" + t + "_" + source.getDb());
    }

    File folder = new File(source.getFolderPath());
    File[] files = folder.listFiles();
    Arrays.sort(Objects.requireNonNull(files), new CompareByNo());

    StringBuffer sql = new StringBuffer();
    for (File f : Objects.requireNonNull(files)) {

      if (f.getName().contains("38") && (!"explain".equals(t))) {
        System.out.println("query_38.sql | issue: https://github.com/pingcap/tidb/issues/27745");
        continue;
      }

      try (Stream<String> ls = Files.lines(Paths.get(f.getAbsolutePath()))) {
        ls.forEach(
            s -> {
              if (!s.startsWith("--")) {
                sql.append(s).append(" \n");
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
        switch (t) {
          case "explain":
            sql.insert(0, "explain ");
            break;
          case "explain_analyze":
            sql.insert(0, "explain analyze ");
            break;
        }

        rs = sm.executeQuery(sql.toString());

        double duration = Util.mills2Second(startTime);
        if ("tidb".equals(t)) {
          System.out.print(" | " + duration + "s | ");
        } else {
          System.out.println(" | " + duration + "s | ");
        }
        totalDuration += duration;
        success++;

        File file;
        switch (t) {
          case "tidb":
            int rows = 0;
            while (rs.next()) {
              rows++;
            }
            System.out.println(rows);
            break;
          case "explain":
            Explain e = new Explain();
            file =
                new File(
                    explainFolder.getAbsolutePath() + "/" + f.getName() + "_" + duration + ".sql");
            FileUtils.write(file, sql.append("\n"), "utf-8");
            FileUtils.write(file, e.explainTable(rs), "utf-8");
            break;
          case "explain_analyze":
            ExplainAnalyze ea = new ExplainAnalyze();
            file =
                new File(
                    explainFolder.getAbsolutePath() + "/" + f.getName() + "_" + duration + ".sql");
            FileUtils.write(file, sql.append("\n"), "utf-8");
            FileUtils.write(file, ea.explainAnalyzeTable(rs), "utf-8");
            break;
        }

      } catch (Exception e) {
        if (e.getMessage().contains("Communications link failure")) {
          System.out.println(" | Communications link failure");
          executeVariables(sm, source);
          Thread.sleep(30000);
        } else {
          System.out.println(" | " + e.getMessage());
        }
        if (!e.getMessage().contains("rollup")
            && !e.getMessage().contains("Communications link failure")) {
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

    System.out.println(
        "total duration:" + totalDuration + "s | success: " + success + " | failed:" + failed);
    System.out.println("failed queries: ");
    for (String error : errors) {
      System.out.println(error);
    }
  }

  @SneakyThrows
  public static void executeVariables(Statement sm, Source source) {
    String[] variables = source.getVariables().split(";");
    for (String variable : variables) {
      try {
        sm.execute(variable);
      } catch (Exception e) {
        executeVariables(sm, source);
      }
    }
  }
}

@Data
class Explain {

  private static final String[] header =
      new String[] {"id", "estRows", "task", "access object", "operator info"};
  private static final String[][] rows = new String[1][5];
  private StringBuilder id = new StringBuilder();
  private StringBuilder estRows = new StringBuilder();
  private StringBuilder task = new StringBuilder();
  private StringBuilder accessObject = new StringBuilder();
  private StringBuilder operatorInfo = new StringBuilder();

  @SneakyThrows
  public String explainTable(ResultSet rs) {
    while (rs.next()) {
      this.id.append(rs.getString(1)).append("\n");
      this.estRows.append(rs.getString(2)).append("\n");
      this.task.append(rs.getString(3)).append("\n");
      this.accessObject.append(rs.getString(4)).append("\n");
      this.operatorInfo.append(rs.getString(5)).append("\n");
    }
    rows[0][0] = id.toString();
    rows[0][1] = estRows.toString();
    rows[0][2] = task.toString();
    rows[0][3] = accessObject.toString();
    rows[0][4] = operatorInfo.toString();
    return FlipTable.of(header, rows);
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
      this.id.append(rs.getString(1)).append("\n");
      this.estRows.append(rs.getString(2)).append("\n");
      this.actRows.append(rs.getString(3)).append("\n");
      this.task.append(rs.getString(4)).append("\n");
      this.accessObject.append(rs.getString(5)).append("\n");
      this.executionInfo.append(rs.getString(6)).append("\n");
      this.operatorInfo.append(rs.getString(7)).append("\n");
      this.memory.append(rs.getString(8)).append("\n");
      this.disk.append(rs.getString(9)).append("\n");
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
