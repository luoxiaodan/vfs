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
		long checkDelay = 10;
		long keepAliveDelay = 200;

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
					writeChunk(out, input, false);
					break;

				case VSFProtocols.READ_CHUNK:
					readChunk(out);
					break;

				case VSFProtocols.INITIALIZE_CHUNK_INFO:// chunkinfo
					SocketUtil.getChunkInfo(out, slave);
					break;

				case VSFProtocols.NEW_CHUNK:// createchunk
					SocketUtil.createChunk(out, input, slave);
					break;
				case VSFProtocols.RELEASE_CHUNK:
					SocketUtil.deleteChunk(out, input, slave);
					break;
				case VSFProtocols.HEART_BEAT_DETECT_TO_SLAVE:
					SocketUtil.sendHeartMessage(out);
					break;
				case VSFProtocols.ASSIGN_MAIN_CHUNK:
					SocketUtil.iniChunk(out, input, slave);
					break;
				case VSFProtocols.WRITE_COPY:
					writeChunk(out, input, true);
					break;
				case VSFProtocols.CREATE_COPY:
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
			String contentBuff = "";

			int contentCount = 0;
			byte[] tempBuf = new byte[Slave.UPLOAD_BUFFER_SIZE];

			while (true) {
				if (contentCount >= writelen) {
					break;
				}
				int cRead = Math.min(writelen - contentCount, tempBuf.length);
				int aRead = input.read(tempBuf, 0, cRead);
				contentBuff += new String(tempBuf);
				contentCount += aRead;
			}

			if (copy) {
				boolean check = slave.writeChunk(chunkid, offset, writelen, contentBuff);
				SocketUtil.responseStatus(out, check);
			} else {
				slave.chunkOption("write", chunkid, offset, writelen, contentBuff);

				while (!signWrite.equals("OK"))
					;
				if (signWrite.equals("OK")) {
					SocketUtil.responesClient(out, VSFProtocols.MESSAGE_OK);
				}
			}

		}

		public void readChunk(DataOutputStream out) throws NumberFormatException, IOException, InterruptedException {

			DataInputStream input = new DataInputStream(socket.getInputStream());
			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int readlen = input.readInt();// readlen

			byte[] content = null;

			signRead = "";

			slave.chunkOption("read", chunkid, offset, readlen, "");
			while (!signRead.equals("OK"))
				;
			if (signRead.equals("OK")) {
				content = slave.readChunk(chunkid, offset, readlen);
			}

			SocketUtil.responesClient(out, "OK");

			int len = 0;
			for (int i = 0; i < content.length; ++i) {
				if (content[i] == '\0') {
					len = i;
					break;
				}
			}
			out.writeInt(len);

			out.writeInt(readlen);

			int bufferSize = Slave.UPLOAD_BUFFER_SIZE;
			byte[] contentBuff = new byte[bufferSize];
			int contentCount = 0;
			while (contentCount < readlen) {
				int writeNum = Math.min(bufferSize, readlen - contentCount);
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
