package com.patreon.euphrates;

import javafixes.concurrency.ReusableCountLatch;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class StreamParser {

  private static final Logger LOG = LoggerFactory.getLogger(StreamParser.class);

  Replicator replicator;
  Config.Table table;
  Map<String, Integer> columnMap = new HashMap<>();
  CountDownLatch started = new CountDownLatch(1);
  ReusableCountLatch finished = new ReusableCountLatch();
  List<List<String>> rows = new ArrayList<>();
  int rowIndex = 0;

  public StreamParser(Replicator replicator) {
    this.replicator = replicator;
  }

  public void parse(InputStream is) {
    try {
      XMLInputFactory2 inputFactory = XMLInputFactory2.class.cast(XMLInputFactory.newInstance());
      XMLStreamReader2 reader =
        XMLStreamReader2.class.cast(inputFactory.createXMLStreamReader(is, "UTF8"));

      List<String> currentRow = null;

      int columnIndex = -1;
      int valueCount = -1;
      int size = 0;
      StringBuilder currentValue = new StringBuilder();
      while (reader.hasNext()) {
        int type = reader.next();
        switch (type) {
          case XMLStreamReader.START_ELEMENT:
            switch (reader.getLocalName()) {
              case "table_data":
                String tableName = reader.getAttributeValue(null, "name");
                table = replicator.getTable(tableName);
                populateColumnMap();
                break;
              case "field":
                String fieldName = reader.getAttributeValue(null, "name");
                columnIndex = columnMap.getOrDefault(fieldName, new Integer(-1));
                break;
              case "row":
                currentRow = new ArrayList<>();
                for (int i = 0, len = columnMap.size(); i != len; i++) {
                  currentRow.add(null);
                }
                rows.add(currentRow);
                rowIndex++;
                valueCount = columnMap.size();
                break;
            }
            break;
          case XMLStreamReader.END_ELEMENT:
            switch (reader.getLocalName()) {
              case "table_data":
                enqueueCurrentRows();
                table = null;
                break;
              case "row":
                if (valueCount != 0)
                  throw new RuntimeException(
                                              String.format(
                                                "value count was expected to be 0, was instead %s for table %s", valueCount, table.name));
                size += currentRow.stream().mapToInt(v -> v == null ? 4 : v.length()).sum();
                if (size > replicator.getConfig().s3.minimumSegmentSize) {
                  enqueueCurrentRows();
                  size = 0;
                }
                break;
              case "field":
                if (columnIndex != -1) {
                  currentRow.set(columnIndex, currentValue.toString());
                  currentValue = new StringBuilder();
                  valueCount--;
                  columnIndex = -1;
                }
                break;
            }
            break;
          case XMLStreamReader.CHARACTERS:
            if (columnIndex != -1) {
              currentValue.append(reader.getText());
            }
            break;
        }
      }
      flush();
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }
  }

  private void flush() {
    try {
      // started latch must be tripped
      started.await();
      // finished phase must be 0
      finished.waitTillZero();
    } catch (InterruptedException e) {
      // do nothing
      LOG.error(String.format("interruptted! %s", e));
    }
  }

  private void enqueueCurrentRows() {
    LOG.info(String.format("enqueuing %s at index %s", table.name, rowIndex));
    replicator.getS3Writer().enqueueRows(table, rows, finished);
    rows = new ArrayList<>();
    // register before counting down the started latch
    finished.increment();
    started.countDown();
  }

  private void populateColumnMap() {
    int position = 0;
    columnMap.clear();
    for (Map.Entry<String, String> column : table.columns.entrySet()) {
      columnMap.put(column.getKey(), position);
      position++;
    }
  }
}
