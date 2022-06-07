import com.jakewharton.fliptables.FlipTable;

public class Util {

    public static double mills2Second(long startTime) {
        return Math.round(System.currentTimeMillis() - startTime) / 1000.0;
    }

    public static String explainAnalyzeTable(String[][] rows) {
        String[] header = new String[]{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"};
        return FlipTable.of(header, rows);
    }

}
