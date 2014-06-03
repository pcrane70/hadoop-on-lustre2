package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.fs.Path;

public class CompressAwarePath extends Path {
    private long rawDataLength;
    private long compressedSize;
    private long offset = 0L;

    public CompressAwarePath(Path path, long rawDataLength, long compressSize) {
      super(path.toUri());
      this.rawDataLength = rawDataLength;
      this.compressedSize = compressSize;
    }
    
    public CompressAwarePath(Path path, long rawDataLength, long compressSize, long offset) {
        super(path.toUri());
        this.rawDataLength = rawDataLength;
        this.compressedSize = compressSize;
        this.offset = offset;
      }

    public long getRawDataLength() {
      return rawDataLength;
    }

    public long getCompressedSize() {
      return compressedSize;
    }
    
    public long getOffset() {
    	return offset;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public int compareTo(Object obj) {
      if(obj instanceof CompressAwarePath) {
        CompressAwarePath compPath = (CompressAwarePath) obj;
        if(this.compressedSize < compPath.getCompressedSize()) {
          return -1;
        } else if (this.getCompressedSize() > compPath.getCompressedSize()) {
          return 1;
        }
        // Not returning 0 here so that objects with the same size (but
        // different paths) are still added to the TreeSet.
      }
      return super.compareTo(obj);
    }
  }