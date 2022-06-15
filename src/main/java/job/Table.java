package job;

import lombok.Data;
import lombok.SneakyThrows;
import main.Source;
import util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Data
public class Table implements Run {

  private String job;

  private Connection conn;
  private Statement sm;
  private ResultSet rs;

  private String db;

  @SneakyThrows
  public Table(Source source) {
    job = source.getJob();
    Class.forName("com.mysql.cj.jdbc.Driver");
    conn = DriverManager.getConnection(source.JDBCUrl(), source.getUser(), source.getPassword());
    sm = conn.createStatement();
    db = source.getDb();
  }

  @SneakyThrows
  public void run() {
    ResultSet rs = sm.executeQuery("show tables");
    List<String> tables = new ArrayList<>();
    while (rs.next()) {
      tables.add(rs.getString("Tables_in_" + db));
    }
    for (String table : tables) {
      switch (job) {
        case "row":
          rs = sm.executeQuery("select count(*) from " + table);
          while (rs.next()) {
            System.out.println(table + ": " + rs.getString("count(*)"));
          }
          break;
        case "analyze":
          System.out.print("analyze table " + table + " ... ");
          long startTime = System.currentTimeMillis();
          sm.execute("analyze table " + table);
          double duration = Util.mills2Second(startTime);
          System.out.println("complete | " + duration + "s");
          break;
      }
    }
  }
}
