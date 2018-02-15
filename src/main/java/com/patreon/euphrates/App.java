package com.patreon.euphrates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    Config config = null;
    try {
      config = Config.load(args[0]);
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
