package com.patreon.euphrates;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.*;

public class ScrubbingInputStreamTest extends TestCase {

  public ScrubbingInputStreamTest(String testName) {
    super(testName);
  }

  public static Test suite() {
    return new TestSuite(ScrubbingInputStreamTest.class);
  }

  public void testScrubbingBytes() throws Exception {
    byte[] in = {123, 0x10, 123, 0, 0xa, 0xb, 0xc, 123, 122};
    ByteArrayInputStream bin = new ByteArrayInputStream(in);
    ScrubbingInputStream sis = new ScrubbingInputStream(bin);
    byte[] output = IOUtils.toByteArray(sis);
    assertArrayEquals(new byte[]{123, 123, 0xa, 123, 122}, output);
  }

  public void testScrubbingMultibytes() throws Exception {
    byte[] in = {123, -17, -65, -65, 123, 123};
    ByteArrayInputStream bin = new ByteArrayInputStream(in);
    ScrubbingInputStream sis = new ScrubbingInputStream(bin);
    byte[] output = IOUtils.toByteArray(sis);
    assertArrayEquals(new byte[]{123, 123, 123}, output);
  }

  public void testScrubbingLowPrintableChars() throws Exception {
    byte[] in = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
      22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
    ByteArrayInputStream bin = new ByteArrayInputStream(in);
    ScrubbingInputStream sis = new ScrubbingInputStream(bin);
    byte[] output = IOUtils.toByteArray(sis);
    assertArrayEquals(new byte[]{9, 10, 13}, output);
  }

  public void testLargeInputScrubbing() throws Exception {
    byte[] in = new byte[1024 * 1024 * 3];
    for (int i = 0; i != in.length; i+=6) {
      System.arraycopy(new byte[]{123, -17, -65, -65, 123, 123}, 0, in, i, 6);
    }

    ByteArrayInputStream bin = new ByteArrayInputStream(in);
    ScrubbingInputStream sis = new ScrubbingInputStream(bin);
    byte[] output = IOUtils.toByteArray(sis);
    assertEquals(1_572_864, output.length);
  }

}
