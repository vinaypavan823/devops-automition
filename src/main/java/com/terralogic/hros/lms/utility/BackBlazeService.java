package com.terralogic.hros.lms.utility;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListBucketsResponse;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;
import com.terralogic.hros.lms.entity.RequestContent;
import com.terralogic.hros.lms.exceptionHandling.NoResourceFound;
import com.terralogic.hros.lms.repository.RequestContentRepo;
import com.terralogic.hros.lms.service.VideoService;



@Component
public class BackBlazeService {



	@Autowired
	private B2StorageClient client;
	
	@Autowired
	VideoService videoService;

	@Autowired
	RequestContentRepo r;

	String FILENAME_TO_SEARCH = "";


	public static org.apache.logging.log4j.Logger logger = LogManager.getLogger(BackBlazeService.class);


	public RequestContent addUrl(String bucketName, String videoName, String url) throws B2Exception {

		logger.info("the file name is " + videoName);


		logger.info("client details aree" + client);
		String fileName="videos/"+videoName+"/master.m3u8";
		String fileName1="videos/"+videoName+"/video.mpd";
		logger.info("the file path is" + fileName);
		List<B2Bucket> b2 = client.buckets();

		String a = b2.stream().filter(v->v.getBucketId().equalsIgnoreCase(bucketName)).map(v->v.getBucketName()).collect(Collectors.toList()).get(0);

		String m3u8 = client.getDownloadByNameUrl(a, fileName); 
		String mpd = client.getDownloadByNameUrl(a, fileName1);
		logger.info("the m3u8 uri is  " + m3u8);
		logger.info("the mpd uri is  " + mpd);
		//  String url1 = "contents/content_1702622902628/content_1702622902628.mp4"; 
	//	RequestContent u =  r.findByUrl(url);
		RequestContent u = new RequestContent();
		
		u.setM3u8(m3u8);
		u.setMpd(mpd);
		u.setStreamable(true);

		r.save(u);     
		return u;
	}
	public String getBucketId(String bucketName) throws B2Exception {

		logger.info("started "  +    bucketName);

		logger.info("endeddddddd");
		// List buckets associated with your account
		B2Bucket bucket= client.getBucketOrNullByName(bucketName);
		logger.info("endeddddddd11111");

		if(bucket!=null) {

			String bucketId = bucket.getBucketId();
			return bucketId;
		}else {
			return null;
		}

	}

	public byte[] getBytes(String fileUrl) throws Exception {

		URL url = new URL(fileUrl);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// Allocate a byte array to store the downloaded data
		byte[] videoBytes = new byte[connection.getContentLength()];

		// Initialize variables for tracking data read
		int bytesRead = 0;
		int offset = 0;

		try (InputStream inputStream = connection.getInputStream()) {
			// Read the downloaded content into the byte array
			while ((bytesRead = inputStream.read(videoBytes, offset, videoBytes.length - offset)) != -1) {
				offset += bytesRead;
			}
		} finally {
			connection.disconnect();
		}

		// Check if the entire content was downloaded
		if (offset != connection.getContentLength()) {
			// Handle download error or incomplete data
			System.err.println("Error downloading video: Incomplete data!");
		} else {
			// Successfully downloaded video bytes
			// Return the videoBytes array to another method
			return videoBytes;
		}
		return null;
	}






	public String getBucketName(String url) throws B2Exception, NoResourceFound {
         B2ListBucketsResponse list = null ;
		try {
		 list = client.listBuckets();
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new B2Exception(e.getMessage(), 0, null, "Back blaze b2 exception");
			
			
		}
		// Iterate through each bucket
		for (B2Bucket bucket : list.getBuckets()) {
			final B2ListFileNamesRequest request1 = B2ListFileNamesRequest
					.builder(bucket.getBucketId())
					.build();

			B2ListFilesIterable response1 = client.fileNames(request1);
			logger.info("the bucket name is " + bucket.getBucketName());
			for (B2FileVersion file : response1) {
				if (file.getFileName().equalsIgnoreCase(url)) {
					return bucket.getBucketId();  // Return the found bucket ID
				}
			}
		}

		// If no file found with the given URL, throw a NoResourceFound exception
		throw new NoResourceFound("no file exists with this url");
	}





	public String getUrl(String bucketName, String fileUrl) throws B2Exception, NoResourceFound {

		List<B2Bucket> b2 = client.buckets();

		String a = b2.stream().filter(v -> v.getBucketId().equalsIgnoreCase(bucketName))
				.map(v -> v.getBucketName()).collect(Collectors.toList()).get(0);

		return client.getDownloadByNameUrl(a, fileUrl);  // Potentially throws B2Exception

	}


}


