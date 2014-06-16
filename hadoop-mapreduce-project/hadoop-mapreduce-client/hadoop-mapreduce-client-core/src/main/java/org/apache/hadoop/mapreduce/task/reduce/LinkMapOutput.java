package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LinkMapOutput<K, V> extends MapOutput<K, V> {
	private static final Log LOG = LogFactory.getLog(OnDiskMapOutput.class);
	private final FileSystem fs;
	private final Path tmpOutputPath;
	private final Path outputPath;
	private final MergeManagerImpl<K, V> merger;
	private long compressedLength;
	private long decompressedLength;
	private long offset;
	private final TaskAttemptID reduceId;
	private final JobConf conf;

	public LinkMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
			MergeManagerImpl<K,V> merger, long size,
			JobConf conf,
			MapOutputFile mapOutputFile,
			int fetcher, boolean primaryMapOutput)
					throws IOException {
		this(mapId, reduceId, merger, size, conf, mapOutputFile, fetcher,
				primaryMapOutput, FileSystem.getLocal(conf),
				mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
	}

	@VisibleForTesting
	LinkMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
			MergeManagerImpl<K,V> merger, long size,
			JobConf conf,
			MapOutputFile mapOutputFile,
			int fetcher, boolean primaryMapOutput,
			FileSystem fs, Path outputPath) throws IOException {
		super(mapId, size, primaryMapOutput);
		this.fs = fs;
		this.merger = merger;
		this.outputPath = outputPath;
		tmpOutputPath = getTempPath(outputPath, fetcher);
		this.reduceId = reduceId;
		this.conf = conf;
	}

	@VisibleForTesting
	static Path getTempPath(Path outPath, int fetcher) {
		return outPath.suffix(String.valueOf(fetcher));
	}

	@Override
	public void shuffle(MapHost host, InputStream input,
			long compressedLength, long decompressedLength,
			ShuffleClientMetrics metrics,
			Reporter reporter) throws IOException {
		
		this.compressedLength = compressedLength;
		this.decompressedLength = decompressedLength;
		
		String mapredLocalDir = conf.get(MRConfig.LOCAL_DIR);
		String user = conf.getUser();

		String src = mapredLocalDir +  "/output/" + getMapId() + "/file.out";
		String src_idx = mapredLocalDir + "/output/" + getMapId() + "/file.out.index";
        
        DataInputStream in = new DataInputStream(new FileInputStream(src_idx));

		try {
			in.skipBytes(8*3*reduceId.getTaskID().getId());
			offset = in.readLong();
			this.compressedLength = in.readLong();
			this.decompressedLength = in.readLong();
		} catch (Exception e) {
			e.printStackTrace();
		}

		setSize(this.decompressedLength);
		in.close();
		

		File f = new File(src);
		if(f.exists()) { 
			LOG.debug("shuffleToLink: the src " + src + " EXIST!") ; 
		}

		String lnCmd = conf.get("hadoop.ln.cmd");
		String command = lnCmd + " " + src + " " + tmpOutputPath;
	
		try {
			LOG.debug("shuffleToLink: Command used for hardlink " + command);
			Runtime.getRuntime().exec(command).waitFor();
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void commit() throws IOException {
		fs.rename(tmpOutputPath, outputPath);
		// Still need to get the offset right here and add it to compressAwarePath
		CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
				getSize(), this.compressedLength, this.offset);
		merger.closeOnDiskFile(compressAwarePath);
	}

	@Override
	public void abort() {
		try {
			fs.delete(tmpOutputPath, false);
		} catch (IOException ie) {
			LOG.info("failure to clean up " + tmpOutputPath, ie);
		}
	}

	@Override
	public String getDescription() {
		return "LINK";
	}
}
