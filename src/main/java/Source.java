import lombok.Data;

import java.util.Map;

@Data
public class Source {

    private String k;

    private String host;
    private String user;
    private String password;
    private String db;
    private String variables;
    private String folderPath;
    private String pd;
    private String master;

    public Source(Map<String, String> properties) {
        String h = properties.get("host");
        String u = properties.get("user");
        String p = properties.get("password");
        String db = properties.get("db");
        String v = properties.get("variables");
        String f = properties.get("file");
        String pd = properties.get("pd");
        String master = properties.get("master");
        String task = properties.get("task");

        switch (task) {
            case "tidb":
            case "explain":
            case "explain_analyze":
                if (h == null || u == null || p == null || db == null || v == null || f == null) {
                    System.out.println("`host`, `user`, `password`, `db`, `variables`, `file` must be set");
                    System.exit(1);
                }
                this.host = h;
                this.user = u;
                this.password = p;
                this.db = db;
                this.variables = v;
                this.folderPath = f;
                break;
            case "row":
            case "analyze":
                if (h == null || u == null || p == null || db == null) {
                    System.out.println("host`, `user`, `password`, `db` must be set");
                    System.exit(1);
                }
                this.host = h;
                this.user = u;
                this.password = p;
                this.db = db;
                break;
            case "tispark":
                if (pd == null || u == null || p == null || db == null || master == null || f == null) {
                    System.out.println("`pd`, `user`, `password`, `db`, `master` must be set");
                    System.exit(1);
                }
                this.pd = pd;
                this.user = u;
                this.password = p;
                this.db = db;
                this.folderPath = f;
                this.master = master;
                break;
            case "replace":
                if (f == null || db == null) {
                    System.out.println("`file`, `db` must be set");
                    System.exit(1);
                }
                this.folderPath = f;
                this.db = db;
                break;
            default:
                System.out.println("unexpected task: " + k);
                System.exit(1);
        }

    }

    public String JDBCUrl() {
        return "jdbc:mysql://" + this.getHost() + "/" + this.getDb();
    }

}
