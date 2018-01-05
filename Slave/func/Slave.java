package func;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import struct.ChunkInfo;

public class Slave {

	
	public  List<ChunkInfo> chunkInfoList=new ArrayList<ChunkInfo>();
	public static final String CHUNK_LOG = "E:\\testlog.txt"; // *1024;
	public static final int CHUNK_SIZE = 64*1024; // *1024;
	public static final int UPLOAD_BUFFER_SIZE = 8*1024;
	public static final int DOWNLOAD_BUFFER_SIZE = 8*1024;
	
	public Slave(){};
	public void IniSalve() throws IOException{
		
		
		File fileName = new File(CHUNK_LOG);
		if(fileName.exists()){  
		      FileReader fileReader=null;  
		      BufferedReader bufferedReader=null;  
		      try{  
		          fileReader=new FileReader(fileName);  
		          bufferedReader=new BufferedReader(fileReader);  
		          try{  
		              String read="";  
		              while((read=bufferedReader.readLine())!=null){  
		           //   System.out.println("line£º"+"\r\n"+read);  
		              String[] Info=read.split(" ");
		              ChunkInfo chunkInfo=new ChunkInfo(Integer.valueOf(Info[0]),Info[1],Integer.valueOf(Info[2]),Integer.valueOf(Info[3]),Integer.valueOf(Info[4]));
		             chunkInfoList.add(chunkInfo);
		            }
		              }catch(Exception e){  
		           e.printStackTrace();  
		          }
		          }catch(Exception e){  
		         e.printStackTrace();  
		         }finally{  
		           if(bufferedReader!=null){  
		              bufferedReader.close();  
		         }  
		          if(fileReader!=null){  
		             fileReader.close();  
		        }  
		     }  	  
		  }
	}

	
	public boolean CreateChunk(JSONObject chunk) throws Exception{
		
		ChunkInfo chunkInfo=new ChunkInfo(chunk.getInt("chunk_id"), chunk.getString("slave_ip"),
				chunk.getInt("port"), chunk.getInt("file_index"), chunk.getInt("chunk_left"));
		chunkInfoList.add(chunkInfo);
		this.writeChunkLog(chunkInfo);
		return true;
	}
	
	
	public JSONArray getChunkList(){
		JSONArray chunkArray = new JSONArray();
		for(int i=0;i<chunkInfoList.size();i++){
			
		    ChunkInfo chunkInfo=chunkInfoList.get(i);
			JSONObject obj = new JSONObject();
			obj.put("chunk_id", chunkInfo.chunkId);
			obj.put("slave_ip", chunkInfo.slaveIP);
			obj.put("port", chunkInfo.port);
			obj.put("file_index", chunkInfo.fileIndex);
			obj.put("chunk_left", chunkInfo.chunkLeft);
			chunkArray.put(obj);
		
		}
		return chunkArray;
	}
	
	
	public boolean deleteChunk(int chunkid) throws Exception{
		boolean flag=false;
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				chunkInfoList.remove(i);
				flag=true;
				break;
			}
		}
		if(flag){
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);			
			 File fileName = new File(CHUNK_LOG);
			   if(fileName.exists()){ 
				   fileName.delete();
			   }
			this.writeChunkLog(chunkInfo);
		}
		}
		return flag;
		
	}
	public byte[] readChunk(int chunkid,int offset,int readLen) throws IOException{
		byte[] buffer = new byte[readLen]; 
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				
				String contentPath="E:\\content"+Integer.toString(chunkid);
				File fileName = new File(contentPath);
				if(fileName.exists()){  
					FileInputStream in = new FileInputStream(contentPath);  
			         			        
					 in.read(buffer,offset,readLen);	         
			          
			        in.close();  
			        
				}
				break;
			}
		}
		return buffer;  
	}
	
	
	public  boolean writeChunk(int chunkid,int offset,int writeLen,String content){
		boolean flag=false;
		String contentPath="E:\\content"+Integer.toString(chunkid);
		try{  
			  File fileName = new File(contentPath);
		   if(!fileName.exists()){  
		    fileName.createNewFile();  
		    System.out.println("CreateFile"+contentPath);
		   }  
		   FileOutputStream out = new FileOutputStream(contentPath);	
		   		    		
	       out.write(content.getBytes(),offset,writeLen);
	       out.close(); 
		   
		   
		  }catch(Exception e){  
		   e.printStackTrace();  
		  }  
		for(int i=0;i<chunkInfoList.size();i++){
			ChunkInfo chunkInfo=chunkInfoList.get(i);
			if (chunkInfo.chunkId==chunkid){
				
				chunkInfo.chunkLeft-=writeLen;
				
				flag=true;
				break;
			}
			
		}
		return flag;
	}
	
	public  void writeChunkLog(ChunkInfo chunkInfo)throws Exception{  
		 
		  try{  
			  File fileName = new File(CHUNK_LOG);
		   if(!fileName.exists()){  
		    fileName.createNewFile();  
		    System.out.println("CreateFile"+CHUNK_LOG);
		   }  
		   
		   BufferedWriter output = new BufferedWriter(new FileWriter(fileName,true));  
           
		   String Info=chunkInfo.chunkId+" "+chunkInfo.slaveIP+" "+chunkInfo.port+" "+chunkInfo.fileIndex+" "+chunkInfo.chunkLeft+"\n";
		   
		   output.write(Info);  
           output.close(); 
		   
		   
		  }catch(Exception e){  
		   e.printStackTrace();  
		  }  
		  
		  
		 }   
	
	
	
	
	
}
