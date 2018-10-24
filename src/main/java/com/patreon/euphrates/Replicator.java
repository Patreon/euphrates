package com.patreon.euphrates;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class Replicator {

  private static final Logger LOG = LogManager.getLogger(Replicator.class);

  Config config;
  S3Writer s3Writer;
  Redshift redshift;

  public Replicator(Config config) {
    this.config = config;
    this.s3Writer = new S3Writer(this);
    this.redshift = new Redshift(this);
  }

  public Config getConfig() {
    return config;
  }

  public S3Writer getS3Writer() {
    return s3Writer;
  }

  public Redshift getRedshift() {
    return redshift;
  }

  public void start() {
    doFullDump();
  }

  public void shutdown() {
    s3Writer.shutdown();
    redshift.shutdown();
  }

  public Config.Table getTable(String tablename) {
    return config.tables.stream()
             .filter(t -> t.name.equals(tablename))
             .findFirst()
             .get();
  }

  private void doFullDump() {
    ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(config.mysql.maxConnections);

    // create list of lists
    List<TableGroup> groups = new ArrayList<>();
    for (int i = 0; i != config.mysql.maxConnections; i++) {
      groups.add(new TableGroup());
    }

    // populate lists by using sizes
    config.tables.stream()
      .sorted((t1, t2) -> getTableSize(t2).compareTo(getTableSize(t1)))
      .forEach(t -> {
        TableGroup smallestGroup = groups.stream()
                                     .sorted((g1, g2) -> g1.getSizeInMb().compareTo(g2.getSizeInMb()))
                                     .findFirst()
                                     .get();
        smallestGroup.add(t.name);
        smallestGroup.incrementSizeInMb(getTableSize(t));
      });

    LOG.debug(String.format("Groups are %s", groups));

    try {
      // create a table copier per group
      List<Future> futures = groups.stream()
                               .map(group -> threadPoolExecutor.submit(new TableCopier(this, config.mysql, group)))
                               .collect(Collectors.toList());
      futures.stream()
        .forEach(f -> {
          try {
            f.get();
          } catch (java.util.concurrent.ExecutionException ee) {
            throw new RuntimeException(ee);
          } catch (InterruptedException ie) {
            // do nothing
          }
        });
      LOG.info("Swaping redshift tables");
      redshift.swapRedshiftTables();
      LOG.info("Done swaping redshift tables");
    } catch (Exception e) {
      App.fatal(e);
    } finally {
      threadPoolExecutor.shutdownNow();
    }
  }

  private Long getTableSize(Config.Table table) {
    return redshift.getTableSize(table.name);
  }
}
