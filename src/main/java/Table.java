import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Table implements Run {

  @SneakyThrows
  public void run(Source source, Map<String, String> properties) {
    String url = source.JDBCUrl();
    Connection conn = DriverManager.getConnection(url, source.getUser(), source.getPassword());
    Statement sm = conn.createStatement();
    ResultSet rs = sm.executeQuery("show tables");
    List<String> tables = new ArrayList<>();
    while (rs.next()) {
      tables.add(rs.getString("Tables_in_" + source.getDb()));
    }
    for (String table : tables) {
      switch (properties.get("task")) {
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
