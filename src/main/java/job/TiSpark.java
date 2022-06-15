package job;

import lombok.Data;
import lombok.SneakyThrows;
import main.Source;

@Data
public class TiSpark implements Run {

  private String master;
  private String pd;
  private String db;

  private String folderPath;
  private StringBuilder sql = new StringBuilder();

  public TiSpark(Source source) {
    master = source.getMaster();
    pd = source.getPd();
    db = source.getDb();
    folderPath = source.getFolderPath();
  }

  @SneakyThrows
  @Override
  public void run() {

//    SparkConf sc =
//        new SparkConf()
//            .set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
//            .set("spark.tispark.pd.addresses", pd)
//            .setMaster("spark://" + master);
//
//    SparkSession ss = SparkSession.builder().config(sc).getOrCreate();
//    ss.sql("use " + db);
//
//    File[] files = Util.sortFolder(folderPath);
//    for (File f : Objects.requireNonNull(files)) {
//
//      try (Stream<String> ls = Files.lines(Paths.get(f.getAbsolutePath()))) {
//        ls.forEach(
//            s -> sql.append(s).append(" "));
//      } catch (Exception e) {
//        System.out.println(f.getName() + "|" + " load failed: " + e.getMessage());
//        continue;
//      }
//
//      long startTime = System.currentTimeMillis();
//      try {
//        System.out.println("start execute query: " + f.getAbsolutePath());
//        ss.sql(sql.toString()).show();
//        System.out.println("duration = " + (System.currentTimeMillis() - startTime) + "ms");
//      } catch (Exception e) {
//        System.out.println(f.getAbsolutePath() + " execute failed, error = " + e);
//      }
//      System.out.println();
//      sql.delete(0, sql.length());
//    }
  }
}
