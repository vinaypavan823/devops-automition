
package com.terralogic.hros.lms.controller;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.backblaze.b2.client.exceptions.B2Exception;
import com.terralogic.hros.lms.entity.RequestContent;
import com.terralogic.hros.lms.exceptionHandling.NoResourceFound;
import com.terralogic.hros.lms.exceptionHandling.TranscodingException;
import com.terralogic.hros.lms.repository.RequestContentRepo;
import com.terralogic.hros.lms.service.VideoTranscodingService;
import com.terralogic.hros.lms.service.VideoTranscodingService2;
import com.terralogic.hros.lms.utility.BackBlazeService;

@RestController
@RequestMapping("/api")

public class VideoController {
	
	@Autowired
	Environment environment;
	
	@Autowired
	RequestContentRepo r1;
	
	@Value("${b2.userAgent}")
	private String c;
	
	@Value("${secret1}")
	private String c1;
	
	@Value("${spring.data}")
	private String c2;

	@Autowired
	private VideoTranscodingService videoTranscodingService;

	@Autowired
	private VideoTranscodingService2 videoTranscodingService2;

	@Autowired
	BackBlazeService s;

	public static org.apache.logging.log4j.Logger logger = LogManager.getLogger(VideoController.class);


	 

	@PostMapping("/upload")
	public ResponseEntity<String> uploadVideo( @RequestParam String fileUrl ) throws Exception {

		LocalDateTime currentTime = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedTime = currentTime.format(formatter);
		String videoName = "video-" + formattedTime;

		try {
			String bucketName = s.getBucketName(fileUrl);
			logger.info(" the bucket name is " + bucketName);
			String videoUrl =s.getUrl(bucketName,fileUrl);	
			byte[] videoBytes = s.getBytes(videoUrl);


			logger.info("the video name url is " + videoUrl);
			logger.info("transcoding started");
			ExecutorService executorService = Executors.newFixedThreadPool(1);

			// Define CompletableFuture for each transcoding task
			CompletableFuture<Void> transcodingTask1 = CompletableFuture.runAsync(() -> {
				

					try {
						System.out.println("mpd");
						videoTranscodingService.transcodeAndStoreVideos(videoBytes, videoName, bucketName);
						System.out.println("mpd");
					} catch (Exception e) {
							//throw new TranscodingException("Transcoding failed for video " +  e);
						e.printStackTrace();
					}

					System.out.println("hiiiiiiii");
				
			}, executorService);

			CompletableFuture<Void> transcodingTask2 = CompletableFuture.runAsync(() -> {
				
					try {
						System.out.println("m3u8");
						videoTranscodingService2.transcodeAndStoreVideos(videoBytes, videoName, bucketName);
						System.out.println("m3u8");
					}  catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}, executorService);

			// Combine both tasks to wait for their completion
			CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(transcodingTask1, transcodingTask2);

			// Wait for both tasks to complete
			combinedFuture.join();

			// Shutdown the executor service
			executorService.shutdown();
			if (!transcodingTask1.isCompletedExceptionally() && !transcodingTask2.isCompletedExceptionally()) {
				RequestContent r = s.addUrl(bucketName, videoName, fileUrl);
				logger.info("request content is " + r);
				logger.info("transcoding ended");
				return new ResponseEntity<>("video uploaded succesfully",HttpStatus.OK);
			} else {
				// Handle the case where one or both tasks failed
				System.out.println("One or both transcoding tasks failed");
				throw new TranscodingException(" video upload failed due to Data base Exception");
				//  return new ResponseEntity<>("video upload failed",HttpStatus.BAD_REQUEST);
			}


		}catch(Exception e) {
			if(e instanceof NoResourceFound) {
				throw new NoResourceFound(e.getMessage());
				//	e.printStackTrace();
			}
			else if(e instanceof B2Exception) {
				throw new NoResourceFound(e.getMessage());	
			}
			else if(e instanceof TranscodingException) {
				throw new TranscodingException(e.getMessage());
			}
			else if(e instanceof RuntimeException) {
				throw new TranscodingException(e.getMessage());
			}
			else {
				throw new TranscodingException(e.getMessage());
			}
		}
		// 	return new ResponseEntity<>("video upload failed",HttpStatus.BAD_REQUEST);

	}
	
	@GetMapping("/cluster-info")
	public List<String> getName() {
		String v = environment.getProperty("b2.userAgent");
		String secret1Value = environment.getProperty("SECRET1");
		String secret2Value = environment.getProperty("SECRETV");
		String secret3Value = environment.getProperty("MONGO_URI");
		String s = environment.getProperty("MONGO-V");
		String s1 = environment.getProperty("SPRING_DATA");
		List<String> v1= new ArrayList<>();
		v1.add(secret1Value);
		v1.add(secret2Value);
		v1.add(secret3Value);
		v1.add(s);
		v1.add(s1);
		v1.add(v);

		return v1;
	}
	@GetMapping("/prop")
	public List<String> prop() {
		List<String> v1= new ArrayList<>();
		v1.add(c);
		v1.add(c1);
		v1.add(c2);
		return v1;
	}
	@PostMapping("/postdata")
	public RequestContent addC() {
		RequestContent r = new RequestContent();
		r.setAccessType(c);
		r.setCategory(c1);
		r.setCoverImage("new code ");
		r1.save(r);
		return r;
		
	}
	
	@GetMapping("/test")
	public String jenkins() {
		return "to check jenkins";
	}



}   



