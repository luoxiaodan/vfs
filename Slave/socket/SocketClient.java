package socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class SocketClient {
	
	private static int port=8888;
	//192.168.0.105
	private static String Ip="127.0.0.1";
    
	public SocketClient(String _Ip,int _port){
		Ip=_Ip;
		port=_port;
	}
	public static void main(String[] args) throws UnknownHostException, IOException {    
        System.out.println("master start up successfully");    
          
       
        Socket socket = new Socket(Ip, port); 
                socket = new Socket(Ip, port);    
                    
                 
                OutputStream out = socket.getOutputStream();
                InputStream input = socket.getInputStream(); 
                
        		// protocol id
        		
        		byte[] protocolBytes = (Integer.toString((1000)).getBytes());
        		
        		out.write(protocolBytes, 0, protocolBytes.length);
        		System.out.println("protocol id: " + protocolBytes); 
        		
        		out.close();  
               //     break;
        		byte[] ret=new byte[8];
                input.read(ret,0,8);     
                System.out.println("from server: " + ret);    
                input.read(ret,0,8); 
           /*     if ("OK".equals(ret)) {    
                    System.out.println("close client");    
                    Thread.sleep(500);    
                    break;    
                }    */
                
                
         
           
        }    
      

}