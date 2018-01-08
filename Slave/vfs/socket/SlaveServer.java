package vfs.socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.json.JSONArray;
import org.json.JSONObject;

import vfs.func.Slave;
import vfs.func.SocketUtil;
import vfs.func.myInt;
import vfs.struct.VSFProtocols;

public class SlaveServer {
	private static ServerSocket server;
	private static long lastSendTime;
	public static String signRead = "";
	public static String signWrite = "";
	private static Slave slave = new Slave();

	public static void main(String[] args) throws IOException, InterruptedException {
		SlaveServer slaveServer = new SlaveServer();
		slave.IniSalve();
		slaveServer.Server();

	}

	public void Server() throws IOException {

		try {

			ServerSocket serverSocket = new ServerSocket(Slave.SLAVE_PORT);
			lastSendTime = System.currentTimeMillis();
			// new Thread(new KeepHeart()).start();
			// =======================test===============
			// new Thread(new KeepHeart()).start();//heartmessage
//			 slave.chunkOption("write", 1, 0, 2, "23",new DataOutputStream(new Socket("localhost",8888).getOutputStream()));
			/*
			 * slave.chunkOption("write", 1, 0, 2, "23"); slave.chunkOption("write", 1, 0,
			 * 2, "23"); slave.chunkOption("read", 1, 0, 2, "");
			 * while(!signRead.equals("OK")); if(signRead.equals("OK")){ byte[]
			 * content=slave.readChunk(1,0,2); System.out.println(new String(content)); }
			 */
			while (true) {

				Socket client = serverSocket.accept();

				new HandlerThread(client);
			}
		} catch (Exception e) {
			System.out.println("server error: " + e.getMessage());
		}
	}

	class KeepHeart implements Runnable {
		long checkDelay = 10000;
		long keepAliveDelay = 200000000;

		public KeepHeart() {
		}

		public void run() {
			while (true) {
				if (System.currentTimeMillis() - lastSendTime > keepAliveDelay) {
					try {

						if (!SocketUtil.heartToMaster(VSFProtocols.HEART_BEAT_DETECT_TO_MASTER)) {
							System.out.println("master error");
						}
					} catch (IOException e) {
						e.printStackTrace();

					}
					lastSendTime = System.currentTimeMillis();
				} else {
					try {
						Thread.sleep(checkDelay);
					} catch (InterruptedException e) {
						e.printStackTrace();

					}
				}
			}
		}

	}

	private class HandlerThread implements Runnable {
		private Socket socket;

		public HandlerThread(Socket client) {
			socket = client;
			new Thread(this).start();
		}

		@Override
		public void run() {
			try {
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream input = new DataInputStream(socket.getInputStream());

				int protocols = SocketUtil.inputProtocols(socket);

				switch (Integer.valueOf(protocols)) {
				case VSFProtocols.WRITE_CHUNK:
					System.out.println("recive write chunk");
					writeChunk(out, input, false);
					break;

				case VSFProtocols.READ_CHUNK:
					System.out.println("recive read chunk");
					readChunk(out);
					break;

				case VSFProtocols.INITIALIZE_CHUNK_INFO:// chunkinfo
					System.out.println("recive get chunkinfo");
					SocketUtil.getChunkInfo(out, slave);
					break;

				case VSFProtocols.NEW_CHUNK:// createchunk
					System.out.println("recive create chunk");
					SocketUtil.createChunk(out, input, slave);
					break;
				case VSFProtocols.RELEASE_CHUNK:
					System.out.println("recive delete chunk");
					SocketUtil.deleteChunk(out, input, slave);
					break;
				case VSFProtocols.HEART_BEAT_DETECT_TO_SLAVE:
					SocketUtil.sendHeartMessage(out);
					break;
				case VSFProtocols.ASSIGN_MAIN_CHUNK:
					SocketUtil.iniChunk(out, input, slave);
					break;
				case VSFProtocols.WRITE_COPY:
					System.out.println("recive write chunkcopy");
					writeChunk(out, input, true);
					break;
				case VSFProtocols.CREATE_COPY:
					System.out.println("recive create chunkcopy");
					SocketUtil.createCopyChunk(out, input, slave);
					break;

				}

				// server.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("error : " + e.getMessage());
			}
		}

		public void writeChunk(DataOutputStream out, DataInputStream input, boolean copy)
				throws IOException, InterruptedException {

			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int writelen = input.readInt();// writelen
			
            byte[] contentBuff=new byte[Slave.CHUNK_SIZE];
			int contentCount = 0;
			byte[] tempBuf = new byte[Slave.UPLOAD_BUFFER_SIZE];

			while (true) {
				if (contentCount >= writelen) {
					break;
				}
				int cRead = Math.min(writelen - contentCount, tempBuf.length);
				int aRead = input.read(tempBuf, 0, cRead);
				for(int i = 0 ; i < aRead ; i++) {
				contentBuff[i+contentCount] =tempBuf[i];
				}
				contentCount += aRead;
			}

			if (copy) {
				boolean check = slave.writeChunk(chunkid, offset, writelen, contentBuff);
				SocketUtil.responseStatus(out, check);
			} else {
				slave.chunkOption("write", chunkid, offset, writelen, contentBuff,out);
				Thread.sleep(500);
			}

		}

		public void readChunk(DataOutputStream out) throws NumberFormatException, IOException, InterruptedException {

			DataInputStream input = new DataInputStream(socket.getInputStream());
			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int readlen = input.readInt();// readlen

			byte[] content = new byte[Slave.CHUNK_SIZE];

			
			myInt mylen=new myInt(0);
			slave.chunkOption("read", chunkid, offset, readlen,null,out);
			while (!slave.getChunkStatus(chunkid).equals("read"))
				;
			if (slave.getChunkStatus(chunkid).equals("read")) {
				content = slave.readChunk(chunkid, offset, readlen,mylen);
			}
			slave.changeChunkStatus(chunkid);
			SocketUtil.responesClient(out, "OK");
			int len=mylen.getX();

//			if(len!=readlen);
			out.writeInt(len);

//			out.writeInt(readlen);

			int bufferSize = Slave.DOWNLOAD_BUFFER_SIZE;
			byte[] contentBuff = new byte[bufferSize];
			int contentCount = 0;
			while (contentCount < len) {
				int writeNum = Math.min(bufferSize, len - contentCount);
				for (int i = 0; i < bufferSize; ++i) {
					contentBuff[i] = content[contentCount + i];
				}
				out.write(contentBuff, 0, writeNum);
				out.flush();

				contentCount += writeNum;
			}

		}
	}
}
