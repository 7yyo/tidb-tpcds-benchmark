package util;

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

  private static final int YEAR = 0;
  private static final int MONTH = 1;
  private static final int DAY = 2;
  private static final int HOURS = 3;
  private static final int MINUTES = 4;
  private static final int SECONDS = 5;
  private static final int MILLIS = 6;

  public static int[] toTimestamp(double mjd) {

    int ymd_hms[] = {-1, -1, -1, -1, -1, -1, -1};
    int a, b, c, d, e, z;

    double jd = mjd + 2400000.5 + 0.5; // if a JDN is passed as argument,
    // omit the 2400000.5 term
    double f, x;

    z = (int) Math.floor(jd);
    f = jd - z;

    if (z >= 2299161) {
      int alpha = (int) Math.floor((z - 1867216.25) / 36524.25);
      a = z + 1 + alpha - (int) Math.floor(alpha / 4);
    } else {
      a = z;
    }

    b = a + 1524;
    c = (int) Math.floor((b - 122.1) / 365.25);
    d = (int) Math.floor(365.25 * c);
    e = (int) Math.floor((b - d) / 30.6001);

    ymd_hms[DAY] = b - d - (int) Math.floor(30.6001 * e);
    ymd_hms[MONTH] = (e < 14) ? (e - 1) : (e - 13);
    ymd_hms[YEAR] = (ymd_hms[MONTH] > 2) ? (c - 4716) : (c - 4715);

    for (int i = HOURS; i <= MILLIS; i++) {
      switch (i) {
        case HOURS:
          f = f * 24.0;
          break;
        case MINUTES:
        case SECONDS:
          f = f * 60.0;
          break;
        case MILLIS:
          f = f * 1000.0;
          break;
      }
      x = Math.floor(f);
      ymd_hms[i] = (int) x;
      f = f - x;
    }

    return ymd_hms;
  }
}
