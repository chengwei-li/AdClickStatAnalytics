package com.licw.sparkApps.sparkStreaming;

public class UserAdClick {
	private String timestamp;
    private String ip;
    private String userID;
    private String adID;
    private String province;
    private String city;
    private Integer clickedCount;
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getAdID() {
		return adID;
	}
	public void setAdID(String adID) {
		this.adID = adID;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Integer getClickedCount() {
		return clickedCount;
	}
	public void setClickedCount(Integer clickedCount) {
		this.clickedCount = clickedCount;
	}
    
}
