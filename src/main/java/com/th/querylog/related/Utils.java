package com.th.querylog.related;

public class Utils {
  public static int computeLevenshteinDistance(final String[] words1,
      final String[] words2) {
    int[][] matrix = new int[words1.length][words2.length];

    // The distance of any first string to an empty second string
    for (int i = 0; i < words1.length; i++) {
      matrix[i][0] = i;
    }

    // The distance of any second string to an empty first string
    for (int j = 0; j < words2.length; j++) {
      matrix[0][j] = j;
    }

    for (int j = 1; j < words2.length; j++) {
      for (int i = 1; i < words1.length; i++) {
        if (words1[i].equals(words2[j])) {
          // no operation required
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          int deletion = matrix[i - 1][j] + 1;
          int insertion = matrix[i][j - 1] + 1;
          int substitution = matrix[i - 1][j - 1] + 1;
          matrix[i][j] = Math.min(Math.min(deletion, insertion), substitution);
        }
      }
    }

    return matrix[words1.length - 1][words2.length - 1];
  }

  public static double computeLevenshteinDistanceSimilarity(final String str1,
      final String str2) {
    String[] words1 = str1.split("\\s");
    String[] words2 = str2.split("\\s");

    return 1 - ((double) computeLevenshteinDistance(words1, words2) / Math.max(
        words1.length, words2.length));
  }
}
