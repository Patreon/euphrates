package com.patreon.euphrates;

public class Util {
  public static String tempTable(Config.Table table) {
    return String.format("_%s_new", table.name);
  }

  public static String formatKey(Config.Table table) {
    return String.format("format/%s", table.name);
  }
}
