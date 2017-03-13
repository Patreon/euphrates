package com.patreon.euphrates;

import java.util.ArrayList;

public class TableGroup extends ArrayList<String> {

  private int sizeInMb = 0;

  public void incrementSizeInMb(int size) {
    sizeInMb += size;
  }

  public Integer getSizeInMb() {
    return sizeInMb;
  }
}