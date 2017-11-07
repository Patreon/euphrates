package com.patreon.euphrates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class App {

  private static final Logger LOG = LoggerFactory.getLogger(App.class);
  public static final StatsDClient statsd =
      new NonBlockingStatsDClient(
          "euphrates", /* prefix to any stats; may be null or empty string */
          "localhost", /* common case: localhost */
          8125);

  public static void main(String[] args) {
    Config config = null;
    try {
      config = Config.load("config.json");
    } catch (java.io.IOException ioe) {
      fatal(ioe);
    }

    Replicator replicator = new Replicator(config);
    try {
      replicator.start();
      LOG.info("Replication complete!");
    } finally {
      replicator.shutdown();
    }
  }

  public static void fatal(Exception e) {
    LOG.error(String.format("FATAL ERROR, exiting!, %s", e.getMessage()), e);
    System.exit(1);
  }
}
