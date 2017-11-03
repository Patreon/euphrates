package com.patreon.euphrates;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class Redshift {

  private static final Logger LOG = LoggerFactory.getLogger(Redshift.class);

  BasicDataSource connectionPool;
  Replicator replicator;
  Config config;
  HashMap<String, Integer> tableSizes = new HashMap<>();

  public Redshift(Replicator replicator) {
    this.replicator = replicator;
    this.config = replicator.getConfig();
    createRedshiftPool();
    populateTableSizes();
  }

  public Integer getTableSize(String name) {
    // make default 1 so table is at least weighted
    return tableSizes.getOrDefault(name.toLowerCase(), new Integer(1)).intValue();
  }

  public void generateTempTable(Schema schema) {
    try (Connection connection = connectionPool.getConnection()) {
      connection
        .createStatement()
        .execute(
          String.format(
            "drop table if exists %s.%s CASCADE",
            config.redshift.schema,
            Util.tempTable(schema.getTable())));
      connection.createStatement().execute(schema.generate());
      connection.commit();
    } catch (SQLException se) {
      throw new RuntimeException(se);
    }
  }

  public void copyManifestPath(Config.Table table, String manifestPath) {
    try (Connection connection = connectionPool.getConnection()) {
      String copyStatement =
        String.format("COPY %s.%s\n", config.redshift.schema, Util.tempTable(table))
          + String.format("FROM 's3://%s/%s'\n", config.s3.bucket, manifestPath)
          + String.format(
          "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'\n",
          config.s3.accessKey,
          config.s3.secretKey)
          + String.format(
          "json 's3://%s/%s' gzip TIMEFORMAT AS 'auto' ACCEPTANYDATE TRUNCATECOLUMNS MANIFEST COMPUPDATE OFF",
          config.s3.bucket,
          Util.formatKey(table));

      LOG.info(String.format("Running %s", copyStatement));

      connection.createStatement().execute(copyStatement);
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void swapRedshiftTables() {
    try (Connection connection = connectionPool.getConnection()) {
      for (Config.Table table : config.tables) {
        connection
          .createStatement()
          .execute(
            String.format(
              "drop table if exists %s.%s CASCADE", config.redshift.schema, table.name));
        connection
          .createStatement()
          .execute(
            String.format(
              "alter table %s.%s rename to %s",
              config.redshift.schema,
              Util.tempTable(table),
              table.name));
        connection.commit();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    try {
      connectionPool.close();
    } catch (SQLException e) {
      // do nothing
    }
  }

  private void createRedshiftPool() {
    String dbUrl =
      String.format(
        "jdbc:redshift://%s:%s/%s",
        config.redshift.host,
        config.redshift.port,
        config.redshift.database);
    this.connectionPool = new BasicDataSource();
    connectionPool.setUsername(config.redshift.user);
    connectionPool.setPassword(config.redshift.password);
    connectionPool.setUrl(dbUrl);
    connectionPool.setInitialSize(1);
    connectionPool.setDefaultAutoCommit(false);
    connectionPool.setMaxTotal(config.redshift.maxConnections);
  }

  private void populateTableSizes() {
    try (Connection connection = connectionPool.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement("select \"table\", size from svv_table_info where schema = ?")) {
        statement.setString(1, config.redshift.schema);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
          tableSizes.put(rs.getString(1).toLowerCase(), rs.getInt(2));
        }
        LOG.info(String.format("tableSizes is %s", tableSizes));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

