import com.jakewharton.fliptables.FlipTable;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

public class Util {

  public static double mills2Second(long startTime) {
    return Math.round(System.currentTimeMillis() - startTime) / 1000.0;
  }

  public static File createFolder(String folderPath) {
    File folder = new File(folderPath);
    if (!folder.mkdir()) {
      System.out.println("create folder " + folder.getAbsolutePath() + " failed");
      System.exit(1);
    }
    return folder;
  }

  public static void deleteFolder(String folderPath) {
    File deleteFolder = new File(folderPath);
    File[] fileList = deleteFolder.listFiles();
    if (fileList == null) {
      return;
    }
    for (File file : fileList) {
      if (!file.isDirectory()) {
        if (!file.delete()) {
          System.out.println("delete file " + file.getAbsolutePath() + " failed");
        }
      } else {
        deleteFolder(file.getAbsolutePath());
      }
    }
    if (!deleteFolder.delete()) {
      System.out.println("delete folder " + deleteFolder.getAbsolutePath() + " failed");
    }
  }

  public static File[] sortFolder(String folderPath) {
    File folder = new File(folderPath);
    File[] files = folder.listFiles();
    Arrays.sort(Objects.requireNonNull(files), new CompareByNo());
    return files;
  }
}
