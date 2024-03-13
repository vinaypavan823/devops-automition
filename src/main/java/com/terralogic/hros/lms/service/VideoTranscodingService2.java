package com.terralogic.hros.lms.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import com.terralogic.hros.lms.exceptionHandling.NoResourceFound;
import com.terralogic.hros.lms.exceptionHandling.TranscodingException;
import com.terralogic.hros.lms.utility.BackBlazeService;

import io.lindstrom.m3u8.model.AlternativeRendition;
import io.lindstrom.m3u8.model.MasterPlaylist;
import io.lindstrom.m3u8.model.MediaType;
import io.lindstrom.m3u8.model.Variant;
import io.lindstrom.m3u8.parser.MasterPlaylistParser;

@Service
public class VideoTranscodingService2 {
	@Value("${ffmpeg.path}")
	private String ffmpegPath;

	@Autowired
	BackBlazeService s;
	
	@Autowired
	 private B2StorageClient client;

	private final Logger logger = LoggerFactory.getLogger(VideoTranscodingService.class);

	@Autowired
	private VideoService videoService;

	public void transcodeAndStoreVideos(byte[] videoData, String videoName, String bucketName)
			throws Exception {

		Path tempDir = Files.createTempDirectory("video_transcoding");;
		try {
			// Create a temporary directory to store the generated files
			

			// Define the temporary video file path
			Path tempVideoFile = tempDir.resolve("temp_video.mp4");

			// Save the video data to the temporary video file
			Files.write(tempVideoFile, videoData);

			// Create an executor service with a fixed number of threads
			int numThreads = 5; // Adjust this based on your system's capabilities
			ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
			// HLS transcoding commands
			String[] hlsCommands = {
					ffmpegPath + " -i " + tempVideoFile +
					" -vf \"scale=256:144\" -c:v h264 -b:v 400k -c:a aac -b:a 128k -hls_time 4 -hls_list_size 0 " +
					"-hls_segment_filename " + tempDir.resolve("output_144p_%03d.ts") + " " +    
					//       "-hls_flags append_list " + tempDir.resolve("master.m3u8") + " " +
					tempDir.resolve("output_144p.m3u8"),


					ffmpegPath + " -i " + tempVideoFile +
					" -vf \"scale=640:360\" -c:v h264 -b:v 800k -c:a aac -b:a 128k -hls_time 4 -hls_list_size 0 " +
					"-hls_segment_filename " + tempDir.resolve("output_360p_%03d.ts") + " " +
					//     "-hls_flags append_list " + tempDir.resolve("master.m3u8") + " " +
					tempDir.resolve("output_360p.m3u8"),

					ffmpegPath + " -i " + tempVideoFile +
					" -vf \"scale=1280:720\" -c:v h264 -b:v 1500k -c:a aac -b:a 192k -hls_time 4 -hls_list_size 0 " +
					"-hls_segment_filename " + tempDir.resolve("output_720p_%03d.ts") + " " +
					//         "-master_pl_name master.m3u8 " + tempDir.resolve("master.m3u8") + " " +
					tempDir.resolve("output_720p.m3u8"),
//
//					ffmpegPath + " -i " + tempVideoFile +
//					" -vf \"scale=1920:1080\" -c:v h264 -b:v 3000k -c:a aac -b:a 192k -hls_time 4 -hls_list_size 0 " +
//					"-hls_segment_filename " + tempDir.resolve("output_1080p_%03d.ts") + " " +
//					//       "-master_pl_name master.m3u8 " + tempDir.resolve("master.m3u8") + " " +
//					tempDir.resolve("output_1080p.m3u8")    



			};

			// Execute HLS commands concurrently
			for (String command : hlsCommands) {
				executorService.execute(() -> {
					try {
						videoService.executeFFmpegCommand(command);
					} catch (IOException | InterruptedException | TranscodingException e) {
						e.printStackTrace();
					}
				});
			}

			// Shutdown the executor and wait for all threads to complete
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			//   MasterPlaylistParser parser = new MasterPlaylistParser();
			MasterPlaylist playlist = MasterPlaylist.builder()
					.version(4)
					.independentSegments(true)
					.addAlternativeRenditions(AlternativeRendition.builder()
							.type(MediaType.AUDIO)
							.name("Default audio")
							.groupId("AUDIO")
							.build())
					.addVariants(
							Variant.builder()
							.addCodecs("avc1.64001f", "mp4a.40.2")            	            
							.bandwidth(1400000)
							.uri("output_360p.m3u8")
							.resolution(640,360)
							.build(),
							Variant.builder()
							.addCodecs("avc1.640028", "mp4a.40.2")
							.bandwidth(580800)
							.uri("output_144p.m3u8")
							.resolution(256, 144)
							.build(),
							Variant.builder()
							.addCodecs("avc1.640029", "mp4a.40.2")
							.bandwidth(3500000)
							.uri("output_720p.m3u8")
							.resolution(1280, 720)
							.build(),
							Variant.builder()
							.addCodecs("avc1.640032", "mp4a.40.2")
							.bandwidth(6500000)
							.uri("output_1080p.m3u8")
							.resolution(1920, 1080)
							.build())


					.build();

			MasterPlaylistParser parser = new MasterPlaylistParser();
			System.out.println(parser.writePlaylistAsString(playlist));

			String masterPlaylist = parser.writePlaylistAsString(playlist);

			// Create a temporary file
			Path tempFile = Files.createTempFile("master_playlist", ".m3u8");

			// Write the master playlist content to the temporary file
			Files.write(tempFile, masterPlaylist.getBytes(), StandardOpenOption.CREATE);

			ExecutorService executor1 = Executors.newFixedThreadPool(10);
			String objectNamePrefix = "videos/" + videoName;

			// Upload HLS M3U8 playlist files and segment files with content type set
		//	uploadFileWithContentType(bucketName, objectNamePrefix + "/master.m3u8", tempFile.toFile(), "application/x-mpegURL");

			@SuppressWarnings("unchecked")
			CompletableFuture<Void>[] futures = new CompletableFuture[] {
					uploadFileWithContentTypeAsync(bucketName, objectNamePrefix + "/master.m3u8", tempFile.toFile(), "application/x-mpegURL",executor1),
					uploadFileWithContentTypeAsync(bucketName, objectNamePrefix + "/" + tempDir.resolve("output_144p.m3u8").getFileName(), tempDir.resolve("output_144p.m3u8").toFile(), "application/x-mpegURL",executor1),
					uploadFileWithContentTypeAsync(bucketName, objectNamePrefix + "/" + tempDir.resolve("output_360p.m3u8").getFileName(), tempDir.resolve("output_360p.m3u8").toFile(), "application/x-mpegURL",executor1),
					uploadFileWithContentTypeAsync(bucketName, objectNamePrefix + "/" + tempDir.resolve("output_720p.m3u8").getFileName(), tempDir.resolve("output_720p.m3u8").toFile(), "application/x-mpegURL",executor1),
					uploadFileWithContentTypeAsync(bucketName, objectNamePrefix + "/" + tempDir.resolve("output_1080p.m3u8").getFileName(), tempDir.resolve("output_1080p.m3u8").toFile(), "application/x-mpegURL",executor1)

			};
			// Upload HLS video segment files with content type set
			Files.list(tempDir)
			.filter(file -> Files.isRegularFile(file) && file.getFileName().toString().endsWith(".ts"))
			.forEach(segmentFile -> {
				executor1.execute(() -> {
				try {
					uploadFileWithContentType(bucketName, objectNamePrefix + "/" + segmentFile.getFileName().toString(), segmentFile.toFile(), "video/MP2T");
				} catch (Exception e) {
					e.printStackTrace();
				}
				});
			});
			CompletableFuture.allOf(futures).join();

	        // Shutdown the executor
	        executor1.shutdown();
	        try {
	            // Wait for the executor to terminate
	            executor1.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }

			logger.info("HLS transcoding and upload completed successfully.");
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		finally {
			try {
				Files.walk(tempDir.toAbsolutePath())
				.sorted(Comparator.reverseOrder())  // Delete in reverse order (subdirectories first)
				.map(Path::toFile)
				.forEach(File::delete);
			} catch (IOException e) {
				logger.error("Error cleaning up temporary files:", e);
			}
		}
	}

			 private CompletableFuture<Void> uploadFileWithContentTypeAsync(String bucketName, String objectName, File file, String contentType, ExecutorService executor) {
		        return CompletableFuture.runAsync(() -> {
					try {
						uploadFileWithContentType(bucketName, objectName, file, contentType);
					} catch (InvalidKeyException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (B2Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NoResourceFound e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}, executor);
		    }

	
	private void uploadFileWithContentType(String bucketName, String objectName, File file, String contentType)
			throws IOException,  InvalidKeyException, NoSuchAlgorithmException, B2Exception, NoResourceFound {
		// Set the content type for the object before uploading
	//	videoService.uploadVideo(bucketName, objectName, file, contentType);
		B2ContentSource source = B2FileContentSource.build(file);
		 B2UploadFileRequest request = B2UploadFileRequest.builder(bucketName, objectName, contentType, source).build();
	        logger.info("started");
	        client.uploadSmallFile(request);
	}
}

