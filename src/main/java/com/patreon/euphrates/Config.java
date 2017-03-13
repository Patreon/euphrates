package com.patreon.euphrates;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Config {
  public static Config load(String path) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    System.out.println("loading config " + path);
    Config config = mapper.readValue(new File(path), Config.class);
    return config;
  }

  public final Mysql mysql;
  public final Redshift redshift;
  public final List<Table> tables;
  public final S3 s3;

  @JsonCreator
  public Config(
                 @JsonProperty("mysql") Mysql mysql,
                 @JsonProperty("redshift") Redshift redshift,
                 @JsonProperty("tables") List<Table> tables,
                 @JsonProperty("s3") S3 s3) {
    this.mysql = mysql;
    this.redshift = redshift;
    this.tables = tables;
    this.s3 = s3;
  }

  public static class Mysql {
    final public String user;
    final public String password;
    final public String host;
    final public int port;
    final public String database;
    final public int maxConnections;

    @JsonCreator
    public Mysql(
                  @JsonProperty("user") String user,
                  @JsonProperty("password") String password,
                  @JsonProperty("host") String host,
                  @JsonProperty("port") int port,
                  @JsonProperty("database") String database,
                  @JsonProperty("maxConnections") int maxConnections) {
      this.user = user;
      this.password = password;
      this.host = host;
      this.port = port;
      this.database = database;
      this.maxConnections = maxConnections;
    }
  }

  public static class Redshift {
    final public String user;
    final public String password;
    final public String host;
    final public int port;
    final public String database;
    final public int maxConnections;
    final public String schema;

    @JsonCreator
    public Redshift(
                     @JsonProperty("user") String user,
                     @JsonProperty("password") String password,
                     @JsonProperty("host") String host,
                     @JsonProperty("port") int port,
                     @JsonProperty("database") String database,
                     @JsonProperty("maxConnections") int maxConnections,
                     @JsonProperty("schema") String schema) {
      this.user = user;
      this.password = password;
      this.host = host;
      this.port = port;
      this.database = database;
      this.maxConnections = maxConnections;
      this.schema = schema;
    }
  }

  public static class Table {
    final public String name;
    final public String extra;
    final public Map<String, String> columns;

    @JsonCreator
    public Table(
                  @JsonProperty("name") String name,
                  @JsonProperty("extra") String extra,
                  @JsonProperty("columns") Map<String, String> columns) {
      this.name = name;
      this.extra = extra;
      this.columns = columns;
    }
  }

  public static class S3 {
    final public String bucket;
    final public String region;
    final public String accessKey;
    final public String secretKey;
    final public int minimumSegmentSize;

    @JsonCreator
    public S3(
               @JsonProperty("bucket") String bucket,
               @JsonProperty("region") String region,
               @JsonProperty("accessKey") String accessKey,
               @JsonProperty("secretKey") String secretKey,
               @JsonProperty("minimumSegmentSize") int minimumSegmentSize) {
      this.bucket = bucket;
      this.region = region;
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      this.minimumSegmentSize = minimumSegmentSize;
    }
  }
}
