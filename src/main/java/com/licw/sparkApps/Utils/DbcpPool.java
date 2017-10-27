package com.licw.sparkApps.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;


public class DbcpPool {
	 private static Logger log = Logger.getLogger(DbcpPool.class);
	    private static BasicDataSource bs = null;
	 
	    /**
	     * 创建数据源
	     * @return
	     */
	    public static BasicDataSource getDataSource() throws Exception{
	        if(bs==null){
	            bs = new BasicDataSource();
	            bs.setDriverClassName("com.mysql.jdbc.Driver");
	            bs.setUrl("jdbc:mysql://10.0.40.4:3306/log");
	            bs.setUsername("log");
	            bs.setPassword("log@bigdata");
	            bs.setMaxActive(200);//设置最大并发数
	            bs.setInitialSize(30);//数据库初始化时，创建的连接个数
	            bs.setMinIdle(50);//最小空闲连接数
	            bs.setMaxIdle(200);//数据库最大连接数
	            bs.setMaxWait(1000);
	            bs.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
	            bs.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
	            bs.setTestOnBorrow(true);
	        }
	        return bs;
	    }
	 
	    /**
	     * 释放数据源
	     */
	    public static void shutDownDataSource() throws Exception{
	        if(bs!=null){
	            bs.close();
	        }
	    }
	 
	    /**
	     * 获取数据库连接
	     * @return
	     */
	    public static Connection getConnection(){
	        Connection con=null;
	        try {
	            if(bs!=null){
	                con=bs.getConnection();
	            }else{
	                con=getDataSource().getConnection();
	            }
	        } catch (Exception e) {
	            log.error(e.getMessage(), e);
	        }
	        return con;
	    }
	 
	    /**
	     * 关闭连接
	     */
	    public static void closeCon(ResultSet rs,PreparedStatement ps,Connection con){
	        if(rs!=null){
	            try {
	                rs.close();
	            } catch (Exception e) {
	                log.error("关闭结果集ResultSet异常！"+e.getMessage(), e);
	            }
	        }
	        if(ps!=null){
	            try {
	                ps.close();
	            } catch (Exception e) {
	                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
	            }
	        }
	        if(con!=null){
	            try {
	                con.close();
	            } catch (Exception e) {
	                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
	            }
	        }
	    }
	    public static void closeCon(PreparedStatement ps,Connection con){
	        
	        if(ps!=null){
	            try {
	                ps.close();
	            } catch (Exception e) {
	                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
	            }
	        }
	        if(con!=null){
	            try {
	                con.close();
	            } catch (Exception e) {
	                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
	            }
	        }
	    }
	    
	    public int[] doBatch(String sql ,List<Object[]> params){
			Connection conn = getConnection();
			PreparedStatement  ps = null;
			int[] result = null;
			try {
				conn.setAutoCommit(false); //取消自动提交数据
				ps = conn.prepareStatement(sql);
				//为sql添加需要的参数
				if(params != null && params.size() > 0) {
					for(Object[] param : params){
						for(int i =0 ;i<param.length;i++){
							ps.setObject(i+1, param[i]);
						}
						ps.addBatch();// 执行批处理
					}
				}
				result = ps.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				closeCon(ps, conn);
//				try {
//					shutDownDataSource();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
			}
			
			return result;
			
		}
		public void doQuery(String sql ,Object[] params,ExecuteCallBack callBack){
			Connection conn = getConnection();
			PreparedStatement  ps = null;
			ResultSet rs = null;
			try {
				ps = conn.prepareStatement(sql);
				if(params != null && params.length > 0) {
					for(int i =0 ;i<params.length;i++){
						ps.setObject(i+1, params[i]);
					}
				}
				rs = ps.executeQuery();
				callBack.resultCallBack(rs);
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				closeCon(rs, ps, conn);
//				try {
//					shutDownDataSource();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
			}
		}
		public static void main(String[] args) {
			
			List<Object[]> params = new ArrayList<Object[]>();
			params.add( new Object[]{77,"1509009951808","2","Zhejiang","Hangzhou"});
			
			DbcpPool dbcp = new DbcpPool();
			dbcp.doBatch("update adclickedcount set clickCount=? where times=? and adID=? and province=? and city=?", params);
			
		}
}
