package com.patreon.euphrates;

import java.util.ArrayList;

public class TableGroup extends ArrayList<String> {

  private long sizeInMb = 0;

  public void incrementSizeInMb(long size) {
    sizeInMb += size;
  }

  public Long getSizeInMb() {
    return sizeInMb;
  }
}