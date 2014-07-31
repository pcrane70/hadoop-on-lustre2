/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryLinkMapOutput<K, V> extends InMemoryMapOutput<K, V> {
	private static final Log LOG = LogFactory
			.getLog(InMemoryLinkMapOutput.class);
	private JobConf conf;
	private final MergeManagerImpl<K, V> merger;
	private final byte[] memory;
	private BoundedByteArrayOutputStream byteStream;
	// Decompression of map-outputs
	private final CompressionCodec codec;
	private final Decompressor decompressor;

	private long compressedLength;
	private long decompressedLength;
	private long offset;

	public InMemoryLinkMapOutput(JobConf conf, TaskAttemptID mapId,
			MergeManagerImpl<K, V> merger, int size, CompressionCodec codec,
			boolean primaryMapOutput) {
		super(conf, mapId, merger, size, codec, primaryMapOutput);
		// super(mapId, (long) size, primaryMapOutput);
		this.conf = conf;
		this.merger = merger;
		this.codec = codec;
		byteStream = new BoundedByteArrayOutputStream(size);
		memory = byteStream.getBuffer();
		if (codec != null) {
			decompressor = CodecPool.getDecompressor(codec);
		} else {
			decompressor = null;
		}
	}

	public byte[] getMemory() {
		return memory;
	}

	public BoundedByteArrayOutputStream getArrayStream() {
		return byteStream;
	}

	@Override
	public void shuffle(MapHost host, InputStream input, long compressedLength,
			long decompressedLength, ShuffleClientMetrics metrics,
			Reporter reporter) throws IOException {

		String mapredLocalDir = conf.get("lustre.dir");
		// String mapredLocalDir = conf.get(MRConfig.TEMP_DIR);
		String user = conf.getUser();
		mapredLocalDir += "/usercache/" + user + "/appcache/"
				+ conf.get(JobContext.APPLICATION_ATTEMPT_ID);

		String src = mapredLocalDir + "/output/" + getMapId() + "/file.out";

		// TO-DO: Do we need to delete the file on exit?
		FileSystem fs = FileSystem.getLocal(conf);
		fs.deleteOnExit(new Path(src));

		File f = new File(src);
		if (f.exists()) {
			LOG.debug("shuffleToLink: the src " + src + " EXIST!");
		}

		LOG.debug("src file size: "+f.length());
		
		input = new FileInputStream(src);
		IFileInputStream checksumIn = new IFileInputStream(input,
				compressedLength, conf);

		input = checksumIn;

		// Are map-outputs compressed?
		if (codec != null) {
			decompressor.reset();
			input = codec.createInputStream(input, decompressor);
		}

		try {
			LOG.debug("offset: " + offset);
			LOG.debug("memory.length: " + memory.length);
			LOG.debug("compressedLength: " + compressedLength);
			LOG.debug("decompressedLength: " + decompressedLength);

			// TO-DO: would offset and length be OK to be int?
			IOUtils.readFully(input, memory, (int) offset, memory.length);
			metrics.inputBytes((int) memory.length);
			reporter.progress();
			LOG.info("Read " + memory.length + " bytes from map-output for "
					+ getMapId());
			LOG.info("Read " + memory.length + " bytes from map-output for "
					+ getMapId());

			/**
			 * We've gotten the amount of data we were expecting. Verify the
			 * decompressor has nothing more to offer. This action also forces
			 * the decompressor to read any trailing bytes that weren't critical
			 * for decompression, which is necessary to keep the stream in sync.
			 */
			if (input.read() >= 0) {
				throw new IOException(
						"Unexpected extra bytes from input stream for "
								+ getMapId());
			}

		} catch (IOException ioe) {
			// Close the streams
			IOUtils.cleanup(LOG, input);

			// Re-throw
			throw ioe;
		} finally {
			CodecPool.returnDecompressor(decompressor);
		}
	}

	@Override
	public void commit() throws IOException {
		merger.closeInMemoryFile(this);
	}

	@Override
	public void abort() {
		merger.unreserve(memory.length);
	}

	@Override
	public String getDescription() {
		return "LINK MEMORY";
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}
