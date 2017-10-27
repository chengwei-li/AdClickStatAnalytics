package com.licw.sparkApps.sparkStreaming;

public class AdClicked {
	private String timestamp;
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
