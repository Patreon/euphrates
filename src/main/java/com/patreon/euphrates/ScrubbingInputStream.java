package com.patreon.euphrates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

public class ScrubbingInputStream extends FilterInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(ScrubbingInputStream.class);
  CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
  CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
  byte[] readBuffer = new byte[4096];
  CharBuffer charBuffer, goodCharBuffer;
  ByteBuffer byteBuffer;

  public ScrubbingInputStream(InputStream in) {
    super(in);
    decoder.onMalformedInput(CodingErrorAction.IGNORE);
    byteBuffer = ByteBuffer.allocate(4096);
    charBuffer = CharBuffer.allocate(4096);
    goodCharBuffer = CharBuffer.allocate(4096);
    byteBuffer.limit(0);
  }

  @Override
  public int read() throws IOException {
    fillBuffer();

    if (!byteBuffer.hasRemaining()) return -1;
    int b = (int)byteBuffer.get();
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    fillBuffer();
    if (!byteBuffer.hasRemaining()) {
      return -1;
    }

    int maxAvailableLength = Math.min(byteBuffer.remaining(), len);
    byteBuffer.get(b, off, maxAvailableLength);
    return maxAvailableLength;
  }

  private void fillBuffer() throws IOException {
    if (byteBuffer.hasRemaining()) {
      return;
    }

    int bytesRead = in.read(readBuffer);
    boolean eof = bytesRead == -1;

    ByteBuffer bytesIn = ByteBuffer.wrap(readBuffer, 0, eof ? 0 : bytesRead);
    decoder.decode(bytesIn, charBuffer, eof);
    if (eof) decoder.flush(charBuffer);

    charBuffer.flip();

    while (charBuffer.hasRemaining()) {
      char c = charBuffer.get();
      if (isPrintableChar(c)) {
        goodCharBuffer.put(c);
      }
    }
    charBuffer.compact();

    goodCharBuffer.flip();
    byteBuffer.clear();
    encoder.encode(goodCharBuffer, byteBuffer, eof);
    if (eof) encoder.flush(byteBuffer);

    byteBuffer.flip();
    goodCharBuffer.compact();
  }

  public boolean isPrintableChar( char c ) {
    Character.UnicodeBlock block = Character.UnicodeBlock.of( c );
    return (!Character.isISOControl(c) || c == '\n' || c == '\t' || c == '\r') &&
             block != null &&
             block != Character.UnicodeBlock.SPECIALS;
  }

}
