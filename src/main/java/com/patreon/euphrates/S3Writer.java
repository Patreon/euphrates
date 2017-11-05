package com.patreon.euphrates;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafixes.concurrency.ReusableCountLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

public class S3Writer {

  private static final Logger LOG = LoggerFactory.getLogger(S3Writer.class);
  private static final int TABLE_QUEUE_SIZE = 20000;
  AmazonS3 client;
  Replicator replicator;
  ThreadPoolExecutor uploader;
  ExecutorService copier;
  ConcurrentHashMap<String, BlockingQueue<CopyJob>> copyQueues = new ConcurrentHashMap<>();
  ConcurrentHashMap<String, ReentrantLock> tableCopyLocks = new ConcurrentHashMap<>();
  int queueSum;

  public S3Writer(Replicator replicator) {
    this.replicator = replicator;
    this.client =
      AmazonS3ClientBuilder.standard().withRegion(replicator.getConfig().s3.region).build();
    this.copier = Executors.newFixedThreadPool(replicator.getConfig().redshift.maxConnections);
    int queueSize = TABLE_QUEUE_SIZE / replicator.getConfig().tables.size();
    queueSum = queueSize * replicator.getConfig().tables.size();

    for (Config.Table table : replicator.getConfig().tables) {
      copyQueues.put(table.name, new LinkedBlockingQueue<>(queueSize));

      // We only allow 1 copy worker at a time to be copying to a given table, so we maintain this mapping.
      tableCopyLocks.put(table.name, new ReentrantLock());
      uploadFormat(table);
    }

    for (int i = 0; i != replicator.getConfig().redshift.maxConnections; i++) {
      this.copier.submit(new CopyWorker(this, copyQueues, tableCopyLocks));
    }
    this.uploader =
      new ThreadPoolExecutor(1, 100, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20));
  }

  public void shutdown() {
    uploader.shutdownNow();
    copier.shutdownNow();
  }

  public void enqueueRows(
                           Config.Table table, List<List<String>> rows, ReusableCountLatch finished) {
    LOG.info(
      String.format(
        "enqueueRows %s currently at %s/%s",
        table.name,
        this.uploader.getQueue().remainingCapacity(),
        getCopyQueueSum()));
    this.uploader.execute(new UploadJob(this, table, rows, finished));
  }

  private void uploadFormat(Config.Table table) {
    try {
      int position = 0;
      Map<String, List<String>> jsonpaths = new HashMap<>();
      List<String> paths = new ArrayList<>();
      jsonpaths.put("jsonpaths", paths);
      for (Map.Entry<String, String> column : table.columns.entrySet()) {
        paths.add(String.format("$[%s]", position));
        position++;
      }
      String formatPath = Util.formatKey(table);
      ObjectMapper mapper = new ObjectMapper();
      client.putObject(
        replicator.getConfig().s3.bucket, formatPath, mapper.writeValueAsString(jsonpaths));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void enqueueKey(Config.Table table, String key, ReusableCountLatch finished) {
    CopyJob job = new CopyJob(table, key, finished);
    this.copyQueues.get(table.name).add(job);
  }

  protected int getCopyQueueSum() {
    return queueSum - Collections.list(copyQueues.elements())
             .stream()
             .mapToInt(v -> v.remainingCapacity())
             .sum();
  }

  protected Replicator getReplicator() {
    return replicator;
  }

  protected AmazonS3 getClient() {
    return client;
  }

  class CopyJob {
    Config.Table table;
    String key;
    ReusableCountLatch finished;

    CopyJob(Config.Table table, String key, ReusableCountLatch finished) {
      this.table = table;
      this.key = key;
      this.finished = finished;
    }

    Config.Table getTable() {
      return table;
    }

    String getKey() {
      return key;
    }

    ReusableCountLatch getFinished() {
      return finished;
    }
  }

  class CopyWorker implements Runnable {

    S3Writer s3Writer;
    Replicator replicator;
    ConcurrentHashMap<String, BlockingQueue<CopyJob>> queues;
    ConcurrentHashMap<String, ReentrantLock> tableCopyLocks;

    CopyWorker(S3Writer s3Writer, ConcurrentHashMap<String, BlockingQueue<CopyJob>> queues, ConcurrentHashMap<String, ReentrantLock> tableCopyLocks) {
      this.s3Writer = s3Writer;
      this.replicator = s3Writer.getReplicator();
      this.queues = queues;
      this.tableCopyLocks = tableCopyLocks;
    }

    public void run() {
      while (true) {
        for (ConcurrentHashMap.Entry<String, BlockingQueue<CopyJob>> copyEntry : queues.entrySet()) {
          try {
            String currentTableName = copyEntry.getKey();
            BlockingQueue<CopyJob> queue = copyEntry.getValue();

            ReentrantLock currentTableLock = tableCopyLocks.get(currentTableName);
            if (!currentTableLock.tryLock()) {
              continue;
            }

            try {
              // wait a second to take an item
              CopyJob firstJob = queue.poll(1L, TimeUnit.SECONDS);
              // when there is no item, go to the next entry in the loop
              if (firstJob == null) continue;

              Config.Table table = firstJob.getTable();
              ArrayList<CopyJob> jobs = new ArrayList<>();
              jobs.add(firstJob);
              // drain up to 9 more for 10 in total
              queue.drainTo(jobs, 9);

              processJobs(table, jobs);
            } finally {
              currentTableLock.unlock();
            }
          } catch (InterruptedException ie) {
            return; // do nothing and return
          } catch (Exception e) {
            App.fatal(e);
          }
        }
      }
    }

    private void processJobs(Config.Table table, ArrayList<CopyJob> jobs) throws JsonProcessingException{
      String manifestId = UUID.randomUUID().toString();
      String manifestPath = String.format("%s/manifest-%s.json", table.name, manifestId);
      HashMap<String, ArrayList<HashMap<String, Object>>> manifest = new HashMap<>();
      ArrayList<HashMap<String, Object>> entries = new ArrayList<>();
      for (CopyJob job : jobs) {
        HashMap<String, Object> entry = new HashMap<>();
        entry.put("mandatory", new Boolean(true));
        entry.put(
                "url",
                String.format("s3://%s/%s", replicator.getConfig().s3.bucket, job.getKey()));
        entries.add(entry);
      }
      manifest.put("entries", entries);
      ObjectMapper mapper = new ObjectMapper();
      s3Writer
              .getClient()
              .putObject(
                      replicator.getConfig().s3.bucket,
                      manifestPath,
                      mapper.writeValueAsString(manifest));

      LOG.info(String.format("copying to %s with %s segments", table.name, jobs.size()));
      replicator.getRedshift().copyManifestPath(table, manifestPath);
      for (CopyJob job : jobs) {
        job.getFinished().decrement();
        s3Writer
                .getClient()
                .deleteObject(
                        new DeleteObjectRequest(replicator.getConfig().s3.bucket, job.getKey()));
      }
      s3Writer
              .getClient()
              .deleteObject(
                      new DeleteObjectRequest(replicator.getConfig().s3.bucket, manifestPath));
    }
  }

  class UploadJob implements Runnable {

    String id;
    Replicator replicator;
    S3Writer s3Writer;
    Config.Table table;
    List<List<String>> rows;
    ReusableCountLatch finished;

    public UploadJob(
                      S3Writer s3Writer,
                      Config.Table table,
                      List<List<String>> rows,
                      ReusableCountLatch finished) {
      this.s3Writer = s3Writer;
      this.replicator = s3Writer.getReplicator();
      this.table = table;
      this.rows = rows;
      this.finished = finished;
      this.id = UUID.randomUUID().toString();
    }

    public void run() {
      File file = new File("/tmp/" + id + ".json.gz");
      try {
        LOG.debug(String.format("writing %s", file));
        file.deleteOnExit();
        Writer writer =
          new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), "UTF-8");
        ObjectMapper mapper = new ObjectMapper();
        for (List<String> row : rows) {
          writer.write(mapper.writeValueAsString(row));
          writer.write("\n");
        }
        writer.flush();
        writer.close();
        rows.clear();
        LOG.debug(String.format("closing %s", file));
        String key = String.format("%s/%s.json.gz", table.name, id);
        s3Writer.getClient().putObject(replicator.getConfig().s3.bucket, key, file);
        LOG.debug(String.format("done saving %s", key));
        s3Writer.enqueueKey(table, key, finished);
      } catch (Exception e) {
        App.fatal(e);
      } finally {
        file.delete();
      }
    }
  }
}
