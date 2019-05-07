package gate.languageanalysers;

import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class CacheRegister {

	private static CacheRegister instance;

	public static synchronized CacheRegister getInstance() {
		if (CacheRegister.instance == null) {
			CacheRegister.instance = new CacheRegister();
		}
		return CacheRegister.instance;
	}

	private DB mapdb;

	private CacheRegister() {
		mapdb = DBMaker.tempFileDB().fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable()
				.fileChannelEnable().make();
	}

	public synchronized Map<String, Integer> getCache(String name) {
		return mapdb.hashMap(name, Serializer.STRING, Serializer.INTEGER).createOrOpen();
	}

	public synchronized void clearCache(String name) {
		mapdb.hashMap(name, Serializer.STRING, Serializer.INTEGER).createOrOpen().clear();
	}

}
