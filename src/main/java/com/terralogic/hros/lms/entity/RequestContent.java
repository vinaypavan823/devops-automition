package com.terralogic.hros.lms.entity;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.terralogic.hros.lms.enums.ApprovalStatus;

import lombok.Data;

@Data
@Document(collection = "contents")
public class RequestContent {

	@Id
	private String id;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTypeOfContent() {
		return typeOfContent;
	}
	public void setTypeOfContent(String typeOfContent) {
		this.typeOfContent = typeOfContent;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getFileType() {
		return fileType;
	}
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getYoutubeId() {
		return youtubeId;
	}
	public void setYoutubeId(String youtubeId) {
		this.youtubeId = youtubeId;
	}
	public String getCoverImage() {
		return coverImage;
	}
	public void setCoverImage(String coverImage) {
		this.coverImage = coverImage;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public boolean isStreamable() {
		return streamable;
	}
	public void setStreamable(boolean streamable) {
		this.streamable = streamable;
	}
	public String getM3u8() {
		return m3u8;
	}
	public void setM3u8(String m3u8) {
		this.m3u8 = m3u8;
	}
	public String getMpd() {
		return mpd;
	}
	public void setMpd(String mpd) {
		this.mpd = mpd;
	}
	public List<Integer> getApprovers() {
		return approvers;
	}
	public void setApprovers(List<Integer> approvers) {
		this.approvers = approvers;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getSubCategory() {
		return subCategory;
	}
	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}
	public String getAccessType() {
		return accessType;
	}
	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}
	public ApprovalStatus getApprovalStatus() {
		return approvalStatus;
	}
	public void setApprovalStatus(ApprovalStatus approvalStatus) {
		this.approvalStatus = approvalStatus;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public String getCreatedTimestamp() {
		return createdTimestamp;
	}
	public void setCreatedTimestamp(String createdTimestamp) {
		this.createdTimestamp = createdTimestamp;
	}
	private String typeOfContent;
	private String title;
	private String description;
	private String fileName;
	private String fileType;
	private String url;
	private String youtubeId;
	private String coverImage;
	private String filePath;
	private boolean streamable;
	private String m3u8;
	private String mpd;
	private List<Integer> approvers;
	private String category;
	private String subCategory;
	private String accessType;  
	private ApprovalStatus approvalStatus;  
	private List<String> tags;
	private String createdTimestamp;
	private String tenantId;
}
