package com.patreon.euphrates;

import java.util.ArrayList;
import java.util.Map;

public class Schema {
  String redshiftSchema;
  Config.Table table;

  public Schema(String redshiftSchema, Config.Table table) {
    this.redshiftSchema = redshiftSchema;
    this.table = table;
  }

  public Config.Table getTable() {
    return table;
  }

  public String generate() {
    String createTablePrefix =
      String.format("CREATE TABLE %s.%s (\n", redshiftSchema, Util.tempTable(table));
    String createTablePostfix = String.format("\n) %s", table.extra);
    ArrayList<String> columns = new ArrayList<>();
    for (Map.Entry<String, String> entry : table.columns.entrySet()) {
      String name = entry.getKey();
      String definition = entry.getValue();
      columns.add(String.format("  %s %s", name, definition));
    }

    return createTablePrefix + String.join(",\n", columns) + createTablePostfix;
  }
}
