package com.patreon.euphrates;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Redshift {

  private static final Logger LOG = LogManager.getLogger(Redshift.class);

  BasicDataSource connectionPool;
  Replicator replicator;
  Config config;
  HashMap<String, Long> tableSizes = new HashMap<>();
  HashMap<String, Long> newTableSizes = new HashMap<>();

  public Redshift(Replicator replicator) {
    this.replicator = replicator;
    this.config = replicator.getConfig();
    createRedshiftPool();
    populateTableSizes();
  }

  public Long getTableSize(String name) {
    // make default 1 so table is at least weighted
    return tableSizes.getOrDefault(name.toLowerCase(), new Long(1)).longValue();
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
          "IAM_ROLE '%s'\n",
          config.s3.iamRole)
          + String.format(
          "json 's3://%s/%s' gzip TIMEFORMAT AS 'auto' ACCEPTANYDATE TRUNCATECOLUMNS MANIFEST",
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
    persistTableSizes();
    try {
      connectionPool.close();
    } catch (SQLException e) {
      // do nothing
    }
  }

  public void recordTableSize(String table, long secondsTook) {
    newTableSizes.put(table, secondsTook);
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
      connection.createStatement().execute("create table if not exists euphrates_table_timings (tablename varchar(255), secondsTook int)");
      try (ResultSet rs = connection.createStatement().executeQuery("select tablename, secondsTook from euphrates_table_timings")) {
        while (rs.next()) {
          tableSizes.put(rs.getString(1).toLowerCase(), rs.getLong(2));
        }
        LOG.info(String.format("table times is %s", tableSizes));
      }
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void persistTableSizes() {
    try {
      try (Connection connection = connectionPool.getConnection()) {
        PreparedStatement updateStatement = connection.prepareStatement("update euphrates_table_timings set secondsTook = ? where tablename = ?");
        PreparedStatement insertStatement = connection.prepareStatement("insert into euphrates_table_timings (secondsTook, tablename) values (?, ?)");

        LOG.info(String.format("new table times is %s", newTableSizes));

        for (Map.Entry<String, Long> entry : newTableSizes.entrySet()) {
          PreparedStatement statement = tableSizes.containsKey(entry.getKey()) ? updateStatement : insertStatement;
          statement.setLong(1, entry.getValue());
          statement.setString(2, entry.getKey());
          statement.execute();
        }
        connection.commit();
      }
    } catch(SQLException e) {
      throw new RuntimeException(e);
    }

  }
}

