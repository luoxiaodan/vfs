package vfs.func;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.net.Socket;
import java.net.UnknownHostException;

import org.json.JSONArray;
import org.json.JSONObject;

import vfs.struct.VSFProtocols;

public class SocketUtil {
	public static void sendHeartMessage(DataOutputStream out) throws IOException{
		 responesClient(out,VSFProtocols.MESSAGE_OK); 
	}
	
   
   public static void getChunkInfo(DataOutputStream out,Slave slave) throws IOException{
    JSONArray chunkarray=slave.getChunkList();
   	String string=chunkarray.toString();
       
    byte[] bytes=string.getBytes();
      
    out.writeInt(bytes.length);
    out.write(bytes, 0, bytes.length);
      
   }
   
   public static void deleteChunk(DataOutputStream out,DataInputStream in,Slave slave) throws NumberFormatException, Exception{
   	//doing
  	boolean stateWruteChunk=slave.deleteChunk(Integer.valueOf(in.readInt()));
   	responseStatus(out,stateWruteChunk);
   }
   
   public static void createChunk(DataOutputStream out,DataInputStream input,Slave slave) throws Exception{
     
       int length = input.readInt();
   	   byte[] bytes = new byte[length];
   	   input.read(bytes, 0, length);
   	
       JSONObject chunk = new JSONObject(new String(bytes));
		boolean stateWruteChunk=slave.CreateChunk(chunk);
		responseStatus(out,stateWruteChunk);
       
   }
   
   public static void responesClient(DataOutputStream out,String content) throws IOException{
   	   out.writeUTF(content);
		   System.out.println("response to client: " + content);
	
   }
   
   public static void responseStatus(DataOutputStream out,boolean check) throws IOException{
   	if(check){
     	  responesClient(out,VSFProtocols.MESSAGE_OK); 
          
       }else{
     	  responesClient(out,VSFProtocols.MASTER_REJECT); 
       }
   }
   
   
   public static  int inputProtocols(Socket socket) throws IOException {		
   	DataInputStream input = new DataInputStream(socket.getInputStream());
		
   	byte[] protocolBuff = new byte[8];
		input.read(protocolBuff, 0, 8);
		int len = 0;
		for (int i = 0; i < protocolBuff.length; ++i) {
			if (protocolBuff[i] == '\0') {
				len = i;
				break;
			}
		}
		return Integer.valueOf(new String(protocolBuff, 0, len));
   	
   	
	}
 
   public static String inputString(Socket socket) throws IOException {
		DataInputStream input = new DataInputStream(socket.getInputStream());
		int length = input.readInt();

   	byte[] string = new byte[length];
		input.read(string, 0, length);
		int len = 0;
		for (int i = 0; i < string.length; ++i) {
			if (string[i] == '\0') {
				len = i;
				break;
			}
		}
	    return new String(string);
	    }
   
   public static void outputProtocol(DataOutputStream out, int protocol) throws IOException {
		byte[] protocolBuff = new byte[8];
		byte[] protocolBytes = (Integer.toString(protocol)).getBytes();
		for (int i = 0; i < protocolBytes.length; ++i) {
			protocolBuff[i] = protocolBytes[i];
		}
		out.write(protocolBuff, 0, protocolBuff.length);
	}
   
   public static boolean heartToMaster(int protocol) throws UnknownHostException, IOException{
	   boolean flag=false;
	   Socket heartsocket=new Socket(Slave.MASTER_IP,Slave.MASTER_PORT); 
       DataOutputStream out=new DataOutputStream(heartsocket.getOutputStream());
   	   DataInputStream input = new DataInputStream(heartsocket.getInputStream());
	
       outputProtocol(out,protocol);
       if(input.readUTF().equals(VSFProtocols.MESSAGE_OK)){
    	   flag=true;
       }
       input.close();
       out.close();
       return flag;
   }
   public static boolean sendToMaster(int protocol,int chunkid) throws UnknownHostException, IOException{
	   boolean flag=false;
	   Socket heartsocket=new Socket(Slave.MASTER_IP,Slave.MASTER_PORT); 
       DataOutputStream out=new DataOutputStream(heartsocket.getOutputStream());
   	   DataInputStream input = new DataInputStream(heartsocket.getInputStream());
	
       outputProtocol(out,protocol);
       out.writeInt(chunkid);
       if(input.readUTF().equals(VSFProtocols.MESSAGE_OK)){
    	   flag=true;
       }
       input.close();
       out.close();
       return flag;
   }
   public static boolean writeCopyChunk(String Slave_IP,int port,int chunkid,int offset,int len,String content) throws UnknownHostException, IOException{
	   boolean flag=false;
	   Socket socket=new Socket(Slave_IP,port);
	   DataOutputStream out=new DataOutputStream(socket.getOutputStream());
   	   DataInputStream input = new DataInputStream(socket.getInputStream());
       outputProtocol(out,VSFProtocols.WRITE_COPY);
       out.writeInt(chunkid);
       out.writeInt(offset);
       out.writeInt(len);
       byte[] buf=content.getBytes();
       int bufferSize = Slave.UPLOAD_BUFFER_SIZE;
		byte[] contentBuff = new byte[bufferSize];
		int contentCount = 0;
		while(contentCount < len){
			int writeNum = Math.min(bufferSize, len-contentCount);
			for(int i = 0; i < bufferSize; ++i){
				contentBuff[i] = buf[contentCount+i];
			}
			out.write(contentBuff, 0, writeNum);
			out.flush();
			
			contentCount += writeNum;
		}
		if(input.readUTF().equals(VSFProtocols.MESSAGE_OK)){
			socket.close();
		}else{
			System.out.println("write to copy error, copychunkid :"+chunkid);
		}
	   
	   return flag;
   }
   
   public static boolean deleteCopyChunk(String Slave_IP,int port,int chunkid) throws UnknownHostException, IOException{
	   boolean flag=false;
	   Socket socket=new Socket(Slave_IP,port);
	   DataOutputStream out=new DataOutputStream(socket.getOutputStream());
   	   DataInputStream input = new DataInputStream(socket.getInputStream());
       outputProtocol(out,VSFProtocols.RELEASE_CHUNK);
       out.writeInt(chunkid);
		if(input.readUTF().equals(VSFProtocols.MESSAGE_OK)){
			socket.close();
		}else{
			System.out.println("write to copy error, copychunkid :"+chunkid);
		}
	   out.close();
	   return flag;
   }

   public static boolean TocreateCopyChunk(String Slave_IP,int port,JSONObject chunk) throws UnknownHostException, IOException{
	   
	   Socket socket=new Socket(Slave_IP,port);
	   DataOutputStream out=new DataOutputStream(socket.getOutputStream());
   	   DataInputStream input = new DataInputStream(socket.getInputStream());
       outputProtocol(out,VSFProtocols.CREATE_COPY);
       
       byte[] bytes = chunk.toString().getBytes();	
       out.writeInt(bytes.length);
	   out.write(bytes, 0, bytes.length);
       out.close();
	   
	   return true;
	   
   }
	public static void createCopyChunk(DataOutputStream out,DataInputStream input,Slave slave) throws Exception{
		int length = input.readInt();
	   	   byte[] bytes = new byte[length];
	   	   input.read(bytes, 0, length);	   	
	       JSONObject chunk = new JSONObject(new String(bytes));
		   boolean stateWruteChunk=slave.createCopy(chunk);
		   responseStatus(out,stateWruteChunk);
		    
	   }
   public static void iniChunk(DataOutputStream out,DataInputStream input,Slave slave) throws IOException{
	   int length = input.readInt();
   	   byte[] bytes = new byte[length];
   	   input.read(bytes, 0, length);   	
       JSONObject chunk = new JSONObject(new String(bytes));
	   boolean stateWruteChunk=slave.iniChunk(chunk);
	   responseStatus(out,stateWruteChunk);
   }
}
