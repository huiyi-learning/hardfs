package edu.sjtu;

import org.francis.util.bloomfilter.CountingBloomFilter;

public class StateManager {

  private static final double n = 10000.0;
  private static final double p = 0.00000005;
  private static final int c = 4;

  private CountingBloomFilter states;

  public StateManager() {
    states = new CountingBloomFilter(n, p, c);
  }

  public boolean insert(String src) {
    return states.add(src);
  }

  public void delete(String src) {
    states.remove(src);
  }

  public boolean exists(String src) {
    return states.contains(src);
  }
}