package com.licw.sparkApps.sparkStreaming;

public class AdClickProvinceTopN {
	String timestamp;
	String adID;
	String province;
	Integer clickCount;
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
	public Integer getClickCount() {
		return clickCount;
	}
	public void setClickCount(Integer clickCount) {
		this.clickCount = clickCount;
	}
	
}
