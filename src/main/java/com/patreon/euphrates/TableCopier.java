package com.patreon.euphrates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

class TableCopier implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TableCopier.class);

  Replicator replicator;
  Config.Mysql mysql;
  List<String> tableNames;

  public TableCopier(Replicator replicator, Config.Mysql mysql, List<String> tableNames) {
    this.replicator = replicator;
    this.mysql = mysql;
    this.tableNames = tableNames;
  }

  @Override
  public void run() {
    try {
      if (tableNames.isEmpty()) return;

      LOG.info(String.format("Running table copier for %s", tableNames));
      for (String tableName : tableNames) {
        Schema schema = new Schema(replicator.getConfig().redshift.schema, replicator.getTable(tableName));
        LOG.debug(String.format("schema is %s", schema.generate()));
        replicator.getRedshift().generateTempTable(schema);
      }
      LOG.info(String.format("Done generating schema for %s", tableNames));

      long startTime = Clock.systemUTC().millis();

      List<String> dumpargs = new ArrayList<String>();
      dumpargs.add("mysqldump");
      dumpargs.add("--compress");
      dumpargs.add("-h");
      dumpargs.add(mysql.host);
      dumpargs.add("-u");
      dumpargs.add(mysql.user);
      dumpargs.add("-p" + mysql.password);
      dumpargs.add("-P" + mysql.port);
      dumpargs.add("--protocol=tcp");
      // dump out timezones
      dumpargs.add("--tz-utc");
      // dump as a single transaction
      dumpargs.add("--single-transaction");
      dumpargs.add("--quick");
      dumpargs.add("--xml");
      dumpargs.add("--max_allowed_packet=512M");

      dumpargs.add(mysql.database);
      dumpargs.addAll(tableNames);

      LOG.info(String.format("Running a mysqldump for %s", dumpargs));

      Process dumpProcess =
        new ProcessBuilder(dumpargs)
          .redirectError(ProcessBuilder.Redirect.INHERIT)
          .redirectOutput(ProcessBuilder.Redirect.PIPE)
          .start();

      StreamParser parser = new StreamParser(replicator);
      parser.parse(new ScrubbingInputStream(dumpProcess.getInputStream()));
      long elapsed = Clock.systemUTC().millis() - startTime;
      LOG.info(String.format("Done copying tables %s in %s seconds", tableNames, elapsed / 1000));
      dumpProcess.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
