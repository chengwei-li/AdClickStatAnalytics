package com.licw.sparkApps.sparkStreaming;

public class AdTrendStat {
	private String adID;
	private Integer clickcount;
	private String _date;
    private String _hour;
    private String _minute;
	public String getAdID() {
		return adID;
	}
	public void setAdID(String adID) {
		this.adID = adID;
	}
	public Integer getClickcount() {
		return clickcount;
	}
	public void setClickcount(Integer clickcount) {
		this.clickcount = clickcount;
	}
	public String get_date() {
		return _date;
	}
	public void set_date(String _date) {
		this._date = _date;
	}
	public String get_hour() {
		return _hour;
	}
	public void set_hour(String _hour) {
		this._hour = _hour;
	}
	public String get_minute() {
		return _minute;
	}
	public void set_minute(String _minute) {
		this._minute = _minute;
	}
    
}
