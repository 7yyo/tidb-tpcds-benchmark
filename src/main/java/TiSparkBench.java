import java.util.Map;

public class TiSparkBench implements Run {
    @Override
    public void run(Source source, Map<String, String> properties) {

    }

//    @SneakyThrows
//    public void run(Source source, Map<String, String> properties) {
//
//        String pd = source.getPd();
//        String master = source.getMaster();
//
//        String folderPath = source.getFolderPath();
//        File folder = new File(folderPath);
//
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("tpc-ds");
//        sparkConf.setMaster("spark://" + master);
//        sparkConf.setIfMissing("spark.tispark.pd.addresses", pd);
//        sparkConf.setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions");
//        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
//        sparkSession.sql("use test");
//
//        StringBuffer sql = new StringBuffer();
//        for (File f : Objects.requireNonNull(folder.listFiles())) {
//            Stream<String> lines = Files.lines(Paths.get(f.getAbsolutePath()));
//            lines.forEach(s -> {
//                if (!s.startsWith("--")) {
//                    sql.append(s).append(" ");
//                }
//            });
//            long startTime = System.currentTimeMillis();
////            String s = sql.toString().replaceAll(";", "");
//            try {
//                System.out.println("start execute query: " + f.getAbsolutePath());
//                Dataset<Table> dataSet = sparkSession.sql(sql.toString());
//                dataSet.show();
//                System.out.println("duration = " + (System.currentTimeMillis() - startTime) + "ms");
//            } catch (Exception e) {
//                System.out.println(f.getAbsolutePath() + " execute failed, error = " + e);
//            }
//            System.out.println();
//            sql.delete(0, sql.length());
//        }
//    }

}
