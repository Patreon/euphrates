package com.patreon.euphrates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ScrubbingInputStream extends FilterInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);

  public ScrubbingInputStream(InputStream in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    int b = in.read();
    return charValid(b) ? b : read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int r = in.read(b, off, len);
    if (r > 0) {
      int lastChar = off;
      // scan all chars
      for (int pos = off; pos < off + r; pos++) {
        // if invalid
        if (!charValid(b[pos])) {
          // find the next valid one
          for (int nextValid = pos + 1; nextValid < off + r; nextValid++) {
            // and swapem!
            if (charValid(b[nextValid])) {
              byte cc = b[nextValid];
              b[nextValid] = b[pos];
              b[pos] = cc;
              lastChar = pos;
              break;
            }
          }
        } else {
          lastChar = pos;
        }
      }
      r += lastChar - (off + r - 1);
    }
    return r;
  }

  private boolean charValid(int c) {
    switch (c) {
      // these characters are forbidden by xml 1.0 which is what mysqldump produces,
      // however, stream itself is not xml 1.0 compliant, thus, remove control characters
      // except for the printable ones
      case 0x0:
      case 0x1:
      case 0x2:
      case 0x3:
      case 0x4:
      case 0x5:
      case 0x6:
      case 0x7:
      case 0x8:
        //case 0x9: tab      \t
        //case 0xa: new line \n
      case 0xb:
      case 0xc:
        // case 0xd: cr      \r
      case 0xe:
      case 0xf:
      case 0x10:
      case 0x11:
      case 0x12:
      case 0x13:
      case 0x14:
      case 0x15:
      case 0x16:
      case 0x17:
      case 0x18:
      case 0x19:
      case 0x1a:
      case 0x1b:
      case 0x1c:
      case 0x1d:
      case 0x1e:
      case 0x1f:
        return false;
      default:
        return true;
    }
  }
}
