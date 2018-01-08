package vfs.func;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;

public class Slave {

	public List<ChunkInfo> chunkInfoList = new ArrayList<ChunkInfo>();
	public List<Chunk> chunkRent = new ArrayList<Chunk>();
	public static final String CHUNK_LOG = "E:\\chunklog.txt";
	public static final String CHUNK_RENT = "E:\\chunkRentlog.txt";
	public static final int CHUNK_SIZE = 64 * 1024; // *1024;
	public static final int UPLOAD_BUFFER_SIZE = 8 * 1024;
	public static final int DOWNLOAD_BUFFER_SIZE = 8 * 1024;
	public static final int SLAVE_PORT = 8888;
	public static final int WRITE_CHUNK = 6001;
	public static final int READ_CHUNK = 6002;
	public static final String MASTER_IP = "localhost";
	public static final int MASTER_PORT = 8877;
	public static final String HEARTMESSAGE = "87654321";
	public String Slave_ip = "";

	public Slave() {
	};

	public String getSlaveIp() {
		return Slave_ip;
	}

	public void IniSalve() throws IOException {

		Slave_ip = InetAddress.getLocalHost().getHostAddress();
		;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;

		File fileName = new File(CHUNK_RENT);
		if (fileName.exists()) {
			fileReader = new FileReader(fileName);
			bufferedReader = new BufferedReader(fileReader);

			String read = "";
			while ((read = bufferedReader.readLine()) != null) {

				chunkRent.add(new Chunk(Integer.valueOf(read), this));
				// chunkRent.get(Integer.valueOf(read)).getInstance().start();
			}

		}

		fileName = new File(CHUNK_LOG);
		if (fileName.exists()) {
			try {
				fileReader = new FileReader(fileName);
				bufferedReader = new BufferedReader(fileReader);
				try {
					String read = "";
					while ((read = bufferedReader.readLine()) != null) {
						// System.out.println("line£º"+"\r\n"+read);
						String[] Info = read.split(" ");
						ChunkInfo chunkInfo = new ChunkInfo(Integer.valueOf(Info[0]), Info[1], Integer.valueOf(Info[2]),
								Integer.valueOf(Info[3]), Integer.valueOf(Info[4]));
						chunkInfoList.add(chunkInfo);

					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (bufferedReader != null) {
					bufferedReader.close();
				}
				if (fileReader != null) {
					fileReader.close();
				}
			}
		}

	}

	public boolean iniChunk(JSONObject chunklist) {
		int chunkid = chunklist.getInt("main_chunk_id");
		Chunk mainchunk = new Chunk(chunkid, this);
		mainchunk.isRent = true;
		chunkRent.add(mainchunk);
		JSONArray copys = chunklist.getJSONArray("copies");
		for (int i = 0; i < copys.length(); i++) {
			JSONObject chunk = copys.getJSONObject(i);
			int copyid = chunk.getInt("chunk_id");
			String copyip = chunk.getString("slave_ip");
			int copyport = chunk.getInt("port");
			ChunkInfo copy = new ChunkInfo(copyid, copyip, copyport, 0, CHUNK_SIZE);
			mainchunk.copyids.add(copy);
		}
		return true;
	}

	public void chunkOption(String option, int chunkid, int offset, int len, String content)
			throws InterruptedException {
		for (int i = 0; i < chunkRent.size(); i++) {
			if (chunkRent.get(i).chunkId == chunkid) {

				if (option.equals("write"))
					chunkRent.get(i).WRchunk(option, offset, len, content);
				else
					chunkRent.get(i).WRchunk(option, offset, len, "");
				break;
			}
		}

	}

	public boolean CreateChunk(JSONObject chunklist) throws Exception {

		int chunkid = chunklist.getInt("chunk_id");
		boolean isRent = chunklist.getBoolean("is_rent");
		Chunk mainchunk = null;
		if (isRent) {
			mainchunk = new Chunk(chunkid, this);
			mainchunk.isRent = true;
			chunkRent.add(mainchunk);
		}
		ChunkInfo chunkInfo = new ChunkInfo(chunkid, Slave_ip, SLAVE_PORT, 0, CHUNK_SIZE);
		chunkInfoList.add(chunkInfo);
		JSONArray copys = chunklist.getJSONArray("copies");
		for (int i = 0; i < copys.length(); i++) {
			JSONObject chunk = copys.getJSONObject(i);
			int copyid = chunk.getInt("chunk_id");
			String copyip = chunk.getString("slave_ip");
			int copyport = chunk.getInt("port");
			ChunkInfo copy = new ChunkInfo(copyid, copyip, copyport, 0, CHUNK_SIZE);
			SocketUtil.TocreateCopyChunk(copyip, copyport, chunk);
			mainchunk.copyids.add(copy);
		}

		this.writeChunkLog(chunkInfo);
		return true;
	}

	public boolean createCopy(JSONObject chunk) throws Exception {
		int copyid = chunk.getInt("chunk_id");
		String copyip = chunk.getString("slave_ip");
		int copyport = chunk.getInt("port");
		ChunkInfo copy = new ChunkInfo(copyid, copyip, copyport, 0, CHUNK_SIZE);
		chunkInfoList.add(copy);
		this.writeChunkLog(copy);
		return true;

	}

	public JSONArray getChunkList() {
		JSONArray chunkArray = new JSONArray();
		for (int i = 0; i < chunkInfoList.size(); i++) {

			ChunkInfo chunkInfo = chunkInfoList.get(i);
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

	public boolean deleteChunk(int chunkid) throws Exception {
		boolean flagchunk = false;

		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {
				chunkInfoList.remove(i);
				for (int j = 0; j < chunkRent.size(); j++) {
					if (chunkRent.get(j).chunkId == chunkid) {
						for (int k = 0; k <= chunkRent.get(j).copyids.size(); k++) {
							ChunkInfo copy = chunkRent.get(j).copyids.get(k);
							flagchunk = SocketUtil.deleteCopyChunk(copy.slaveIP, copy.port, copy.chunkId);
							if (!flagchunk) {
								System.out.println("delete copy chunk error,copyid :" + copy.chunkId);
							}
						}
						flagchunk = true;
						chunkRent.get(j).close = true;
						chunkRent.remove(chunkid);
						break;
					}
				}
			}

			break;
		}

		if (flagchunk) {
			File fileName = new File(CHUNK_LOG);
			if (fileName.exists()) {
				fileName.delete();
			}
			for (int i = 0; i < chunkInfoList.size(); i++) {
				ChunkInfo chunkInfo = chunkInfoList.get(i);
				this.writeChunkLog(chunkInfo);
			}
		}

		return flagchunk;

	}

	public byte[] readChunk(int chunkid, int offset, int readLen) throws IOException {
		byte[] buffer = new byte[readLen];
		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {

				String contentPath = "E:\\content" + Integer.toString(chunkid);
				File fileName = new File(contentPath);
				if (fileName.exists()) {
					FileInputStream in = new FileInputStream(contentPath);

					in.read(buffer, offset, readLen);

					in.close();
					for (int j = 0; j < chunkRent.size(); j++) {
						if (chunkRent.get(j).chunkId == chunkid) {

							chunkRent.get(j).option.remove(0);
							chunkRent.get(j).status = "idle";
							break;
						}

					}
				}
				break;
			}
		}
		return buffer;
	}

	public boolean writeChunk(int chunkid, int offset, int writeLen, String content) {
		boolean flag = false;
		String contentPath = "E:\\content" + Integer.toString(chunkid);
		try {
			File fileName = new File(contentPath);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + contentPath);
			}
			FileOutputStream out = new FileOutputStream(contentPath);

			out.write(content.getBytes(), offset, content.getBytes().length);
			out.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {

				chunkInfo.chunkLeft -= writeLen;

				flag = true;
				break;
			}

		}
		return flag;
	}

	public void writeChunkLog(ChunkInfo chunkInfo) throws Exception {

		try {
			File fileName = new File(CHUNK_LOG);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + CHUNK_LOG);
			}

			BufferedWriter output = new BufferedWriter(new FileWriter(fileName, true));

			String Info = chunkInfo.chunkId + " " + chunkInfo.slaveIP + " " + chunkInfo.port + " " + chunkInfo.fileIndex
					+ " " + chunkInfo.chunkLeft + "\n";

			output.write(Info);
			output.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void writeChunkRent(Chunk chunk) throws Exception {

		try {
			File fileName = new File(CHUNK_RENT);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + CHUNK_RENT);
			}

			BufferedWriter output = new BufferedWriter(new FileWriter(fileName, true));

			String Info = String.valueOf(chunk.chunkId);
			for (int i = 0; i < chunk.copyids.size(); i++) {
				Info += " " + String.valueOf(chunk.copyids.get(i));
			}
			output.write(Info);
			output.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
