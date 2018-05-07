package io.svectors.hbase.util;

public interface TrackHbaseWrite {
 public long getRecentTransactionTime();
 public String getLastWrittenUUid();
}
