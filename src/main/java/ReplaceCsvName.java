import java.io.File;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Convert the data files generated by tpc-ds into data file names that tidb-lightning can recognize
 */
public class ReplaceCsvName {


    public static void main(String[] args) {

        String folderPath = System.getProperty("f");

        File folder = new File(folderPath);
        File newFile;

        for (File f : Objects.requireNonNull(folder.listFiles())) {

            String db = f.getName().split("\\.")[0]; // database name
            String mid = f.getName().split("\\.")[1]; // process part
            String csv = f.getName().split("\\.")[2]; // csv


            String[] parts = mid.split("_");

            // store_1_4 -> store.1
            if (parts.length == 3) {
                // store.1.4
                mid = mid.replaceAll("_", ".");
                String[] m = mid.split("\\.");
                // test.store.1.csv
                String newFileName = db + "." + m[0] + "." + numberFormat(m[1]) + "." + csv;
                newFile = new File(f.getParent() + "/" + newFileName);
                boolean success = f.renameTo(newFile);
                if (!success) {
                    System.out.println("Rename file failed, " + f.getName() + " -> " + newFile.getName());
                }
            }

            // web_returns_1_4 -> web_returns.1
            else if (parts.length == 4) {
                // web.returns.1.4
                mid = mid.replaceAll("_", ".");
                // web_returns.1.4
                mid = mid.replace(".", "_");
                String[] m = mid.split("_");
                String newFileName = db + "." + m[0] + "_" + m[1] + "." + numberFormat(m[2]) + "." + csv;
                newFile = new File(f.getParent() + "/" + newFileName);
                boolean success = f.renameTo(newFile);
                if (!success) {
                    System.out.println("Rename file failed, " + f.getName() + " -> " + newFile.getName());
                }
            } else {
                System.out.println("unexpected filename: " + f.getName());
                System.exit(1);
            }
        }

    }

    public static String numberFormat(String i) {
        NumberFormat formatter = NumberFormat.getNumberInstance();
        formatter.setMinimumIntegerDigits(8);
        formatter.setGroupingUsed(false);
        return formatter.format(Integer.parseInt(i));
    }

}
