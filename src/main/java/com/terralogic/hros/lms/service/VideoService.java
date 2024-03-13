package com.terralogic.hros.lms.service;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import com.terralogic.hros.lms.exceptionHandling.NoResourceFound;
import com.terralogic.hros.lms.exceptionHandling.TranscodingException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VideoService {
	
	 @Autowired
	 private B2StorageClient client;
    
	public static org.apache.logging.log4j.Logger logger = LogManager.getLogger(VideoService.class);
    
 
    
    String contentType = "mp4";
  
	public void uploadVideo(String bucketName, String objectName, File file, String contentType) throws B2Exception, NoResourceFound {
		
		try {
		B2ContentSource source = B2FileContentSource.build(file);
		 B2UploadFileRequest request = B2UploadFileRequest.builder(bucketName, objectName, contentType, source).build();
	        logger.info("started");
	        client.uploadSmallFile(request);
		
	}
		catch(Exception e){
			
		throw new NoResourceFound(e.getMessage());
	}
		
	}
	
	public void executeFFmpegCommand(String command) throws  TranscodingException, IOException, InterruptedException {
		ProcessBuilder processBuilder = new ProcessBuilder();
	//	processBuilder.command("cmd.exe", "/c", command);
	// processBuilder.command("cmd.exe", "/c", "start", "cmd.exe", "/c", command);
		processBuilder.command("sh", "-c", command);
		processBuilder.redirectErrorStream(true); // Redirect error stream to the input stream
		Process process = processBuilder.start();

		// Capture the output and error streams
		StringBuilder outputBuilder = new StringBuilder();
		StringBuilder errorBuilder = new StringBuilder();

		try (
				BufferedReader outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
				) {
			String line;
			while ((line = outputReader.readLine()) != null) {
				outputBuilder.append(line).append("\n");
			}
			while ((line = errorReader.readLine()) != null) {
				errorBuilder.append(line).append("\n");
			}
		}

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			// Log the FFmpeg error output
			logger.error("FFmpeg command error output:\n" + errorBuilder.toString());
			throw new TranscodingException("FFmpeg command exited with an error: " + exitCode);
		} else {
			// Log the FFmpeg normal output
			logger.info("FFmpeg command output:\n" + outputBuilder.toString());
		}

		// Close input streams
		process.getInputStream().close();
		process.getErrorStream().close();
	}

	public void executeMP4BoxCommand(String command) throws IOException, InterruptedException {
		ProcessBuilder processBuilder = new ProcessBuilder();
	//	processBuilder.command("cmd.exe", "/c", command);
		
	//	processBuilder.command("/usr/local/bin/MP4Box", "-version");
		processBuilder.command("sh", "-c", command);
		processBuilder.redirectErrorStream(true); // Redirect error stream to the input stream
		Process process = processBuilder.start();

		// Capture the output and error streams
		StringBuilder outputBuilder = new StringBuilder();
		StringBuilder errorBuilder = new StringBuilder();

		try (
				BufferedReader outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))
				) {
			String line;
			while ((line = outputReader.readLine()) != null) {
				outputBuilder.append(line).append("\n");
			}
			while ((line = errorReader.readLine()) != null) {
				errorBuilder.append(line).append("\n");
			}
		}

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			// Log the MP4Box error output
			logger.error("MP4Box command error output:\n" + errorBuilder.toString());
			throw new RuntimeException("MP4Box command exited with an error: " + exitCode);
		} else {
			// Log the MP4Box normal output
			logger.info("MP4Box command output:\n" + outputBuilder.toString());
		}

		// Close input streams
		process.getInputStream().close();
		process.getErrorStream().close();
	}
	
}
