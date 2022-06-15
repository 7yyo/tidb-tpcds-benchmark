package main;

import lombok.Data;

import java.util.Map;

@Data
public class Source {

  private String job;

  private String host;
  private String user;
  private String password;
  private String db;
  private String variables;
  private String folderPath;
  private String pd;
  private String master;
  private String table;
  private String partitionKey;

  public Source(Map<String, String> properties) {

    String h = properties.get("host");
    String u = properties.get("user");
    String p = properties.get("password");
    String db = properties.get("db");
    String v = properties.get("variables");
    String f = properties.get("file");
    String pd = properties.get("pd");
    String master = properties.get("master");
    String job = properties.get("job");
    String table = properties.get("table");
    String partitionKey = properties.get("partitionKey");
    this.job = job;

    switch (job) {
      case "tidb":
      case "explain_analyze":
        if (h == null || u == null || p == null || db == null || v == null || f == null) {
          System.out.println("`host`, `user`, `password`, `db`, `variables`, `file` must be set");
          System.exit(1);
        }
        host = h;
        user = u;
        password = p;
        this.db = db;
        variables = v;
        folderPath = f;
        break;
      case "row":
      case "analyze":
        if (h == null || u == null || p == null || db == null) {
          System.out.println("host`, `user`, `password`, `db` must be set");
          System.exit(1);
        }
        host = h;
        user = u;
        password = p;
        this.db = db;
        break;
      case "tispark":
        if (pd == null || u == null || p == null || db == null || master == null || f == null) {
          System.out.println("`pd`, `user`, `password`, `db`, `master` must be set");
          System.exit(1);
        }
        this.pd = pd;
        user = u;
        password = p;
        this.db = db;
        folderPath = f;
        this.master = master;
        break;
      case "replace":
        if (f == null || db == null) {
          System.out.println("`file`, `db` must be set");
          System.exit(1);
        }
        folderPath = f;
        this.db = db;
        break;
      case "partition":
        if (h == null
            || db == null
            || u == null
            || p == null
            || table == null
            || partitionKey == null) {
          System.out.println("`db`, `user`, `password`, `table`, `partitionKey` must be set");
          System.exit(1);
        }
        host = h;
        this.db = db;
        user = u;
        password = p;
        this.table = table;
        this.partitionKey = partitionKey;
        break;
      default:
        System.out.println("unexpected task: " + job);
        System.exit(1);
    }
  }

  public String JDBCUrl() {
    return "jdbc:mysql://" + this.host + "/" + this.db;
  }
}
