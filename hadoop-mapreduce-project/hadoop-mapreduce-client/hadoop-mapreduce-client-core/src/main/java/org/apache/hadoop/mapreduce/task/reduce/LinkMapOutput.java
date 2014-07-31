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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LinkMapOutput<K, V> extends MapOutput<K, V> {
	private static final Log LOG = LogFactory.getLog(LinkMapOutput.class);
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
	
		System.out.println("tnstansbury: " + conf.get(MRConfig.LOCAL_DIR + "tim"));	
		System.out.println(MRConfig.LOCAL_DIR);
		System.out.println(conf.get(MRConfig.TEMP_DIR));
		String mapredLocalDir = conf.get("lustre.dir");
		//String mapredLocalDir = conf.get(MRConfig.TEMP_DIR);
		String user = conf.getUser();
		mapredLocalDir += "/usercache/" + user + "/appcache/" + conf.get(JobContext.APPLICATION_ATTEMPT_ID);

		String src = mapredLocalDir +  "/output/" + getMapId() + "/file.out";
        fs.deleteOnExit(new Path(src));

		File f = new File(src);
		if(f.exists()) { 
			LOG.debug("shuffleToLink: the src " + src + " EXIST!") ; 
		}

		String lnCmd = conf.get("hadoop.ln.cmd");
		String command = lnCmd + " " + src + " " + tmpOutputPath;
		System.out.println("Executing command: " + command);	
		try {
			
			String mkdirCmd = "mkdir -p ";
			String dir = tmpOutputPath.getParent().toString();
			System.out.println("Executing mkdir command: " + mkdirCmd + dir);
			
			Runtime.getRuntime().exec(mkdirCmd + dir).waitFor();
			
			LOG.debug("shuffleToLink: Command used for hardlink " + command);
			Runtime.getRuntime().exec(command).waitFor();
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
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
