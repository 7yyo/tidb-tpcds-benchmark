import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class TiSpark implements Run {
  @Override
  public void run() {}

  //  @SneakyThrows
  //  @Override
  //  public void run(Source source, Map<String, String> properties) {
  //
  //    String master = source.getMaster();
  //    String pd = source.getPd();
  //
  //    SparkConf sc =
  //        new SparkConf()
  //            .set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
  //            .set("spark.tispark.pd.addresses", pd)
  //            .setMaster("spark://" + master);
  //
  //    SparkSession ss = SparkSession.builder().config(sc).getOrCreate();
  //    ss.sql("use " + source.getDb());
  //
  //    File folder = new File(source.getFolderPath());
  //    File[] files = folder.listFiles();
  //    Arrays.sort(Objects.requireNonNull(files), new CompareByNo());
  //
  //    StringBuffer sql = new StringBuffer();
  //    for (File f : Objects.requireNonNull(files)) {
  //
  //      try (Stream<String> ls = Files.lines(Paths.get(f.getAbsolutePath()))) {
  //        ls.forEach(
  //            s -> {
  //              if (!s.startsWith("--")) {
  //                sql.append(s).append(" ");
  //              }
  //            });
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
  //  }
}
