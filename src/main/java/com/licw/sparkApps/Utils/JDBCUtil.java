package com.licw.sparkApps.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;



public class JDBCUtil {
	private static JDBCUtil jdbcInstance = null ;
	private static LinkedList<Connection> dbConnPool = 
				new LinkedList<Connection>();
	String url = "jdbc:mysql://10.0.40.4:3306/log";
	String username = "log";
	String passwd = "log@bigdata";
	public JDBCUtil() {
		//初始化时 创建10个链接用于连接池
		for(int i=0;i<100;i++){
			try {
				Connection conn = DriverManager.getConnection(url,username,passwd);
				dbConnPool.push(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 数据库连接实例,单例模式
	 * @return
	 */
	public static JDBCUtil getJdbcInstance() {
		if(jdbcInstance == null){
			synchronized (JDBCUtil.class) {
				if(jdbcInstance == null){
					jdbcInstance= new JDBCUtil();
				}
			}
		}
		return jdbcInstance;
	}
	/**
	 * 从连接池取Connection，考虑线程问题加入同步锁，让1个线程只获取一个实例
	 * @return
	 */
	public synchronized Connection getConnection(){
		while(dbConnPool.size() == 0){ //连接实例没有时 ，即死循环。保证后续有实例可用
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return dbConnPool.poll();
	}
	/**
	 * 获取实例后进行查看、插入、更新数据操作，且操作都是批量处理
	 * @param sql  sql语句
	 * @param params 批量操作的参数
	 * @param callBack 
	 * @return  操作成功的数组
	 */
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
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(conn!=null){
				try {
					dbConnPool.push(conn);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn!=null){
				try {
					dbConnPool.push(conn);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	 public int doUpdate(String sql, Object[] params) {
	      int rtn = 0;
	      Connection conn = null;
	      PreparedStatement pstmt = null;
	      
	      try {
	         conn = getConnection();
	         conn.setAutoCommit(false);  
	         
	         pstmt = conn.prepareStatement(sql);
	         
	         if(params != null && params.length > 0) {
	            for(int i = 0; i < params.length; i++) {
	               pstmt.setObject(i + 1, params[i]);  
	            }
	         }
	         
	         rtn = pstmt.executeUpdate();
	         
	         conn.commit();
	      } catch (Exception e) {
	         e.printStackTrace();  
	      } finally {
	         if(conn != null) {
	        	 dbConnPool.push(conn);  
	         }
	      }
	      
	      return rtn;
	   }
	public static void main(String[] args) {
		JDBCUtil jdbc = new JDBCUtil();
//		List<Object[]> insertp = new ArrayList<Object[]>();
//		insertp.add(new Object[]{"1509009951808","2","Zhejiang","Hangzhou",90});
//		jdbc.doBatch("insert into adclickedcount values(?,?,?,?,?)", insertp);
		Object[] up = new Object[]{80,"1509009951808","2","Zhejiang","Hangzhou"};
		jdbc.doUpdate("update adclickedcount set clickCount=? where times=? and adID=? and province=? and city=?", up);
		
	}
}
