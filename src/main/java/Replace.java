import java.io.File;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;

/**
 * Convert the data files generated by tpc-ds into data file names that tidb-lightning can recognize
 */
public class Replace implements Run {

    public void run(Source source, Map<String, String> properties) {

        File folder = new File(source.getFolderPath());

        int success = 0;
        int fail = 0;
        for (File file : Objects.requireNonNull(folder.listFiles())) {

            if (file.getName().split("\\.").length != 2) {
                System.out.println("unexpected file name: " + file.getAbsolutePath() + ", skip");
                fail++;
                continue;
            }

            String processed = file.getName().split("\\.")[0].replaceAll("_", "."); // The part that needs to be processed, like `customer.1.3`
            String[] parts = processed.split("\\.");
            String newCSVName = "";

            switch (parts.length) {
                case 3:
                    newCSVName = source.getDb() + "." + parts[0] + "." + numberFormat(parts[1]) + ".csv";
                    break;
                case 4:
                    newCSVName = source.getDb() + "." + parts[0] + "_" + parts[1] + "." + numberFormat(parts[2]) + ".csv";
                    break;
                default:
                    System.out.println("unexpected file name: " + file.getAbsolutePath() + ", skip");
                    fail++;
                    continue;
            }
            renameFile(file, newCSVName);
            success++;
        }
        System.out.println("replace CSV name complete, success:" + success + ", failed:" + fail);
    }

    public static String numberFormat(String i) {
        NumberFormat formatter = NumberFormat.getNumberInstance();
        formatter.setMinimumIntegerDigits(8);
        formatter.setGroupingUsed(false);
        return formatter.format(Integer.parseInt(i));
    }

    public static void renameFile(File oldFile, String newFileName) {
        File newFile = new File(oldFile.getParent() + "/" + newFileName);
        if (!oldFile.renameTo(newFile)) {
            System.out.println("Rename file failed, " + oldFile.getName() + " -> " + newFile.getName());
        }
    }

}