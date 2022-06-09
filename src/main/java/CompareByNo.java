import java.io.File;
import java.util.Comparator;

public class CompareByNo implements Comparator<File> {
  @Override
  public int compare(File o1, File o2) {
    if (o1.isDirectory() || o2.isDirectory()) {
      return 0;
    }
    int a = 0;
    int b = 0;
    try {
      a = Integer.parseInt(o1.getName().split("_")[1].split("\\.")[0]);
      b = Integer.parseInt(o2.getName().split("_")[1].split("\\.")[0]);
    } catch (Exception e) {
      System.out.println("error file name: " + o1 + "," + o2);
      System.exit(1);
    }
    return Integer.compare(a, b);
  }
}
