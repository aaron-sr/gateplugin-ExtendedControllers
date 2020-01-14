package gate.languageanalysers;

import java.util.HashMap;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import gate.Corpus;
import gate.Gate;
import gate.Resource;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;

public class Cache {

	private static final Map<String, Cache> caches = new HashMap<>();

	public static synchronized Cache getInstance(String name) {
		return caches.computeIfAbsent(name, (n) -> new Cache(n));
	}

	private final Map<String, Corpus> corpora = new HashMap<>();

	private final DB mapdb;
	private final Map<String, String> corpusNameMap;
	private final Map<String, Integer> indexMap;

	private Cache(String name) {
		mapdb = DBMaker.tempFileDB().fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable()
				.fileChannelEnable().make();

		indexMap = mapdb.hashMap("corpusName", Serializer.STRING, Serializer.INTEGER).createOrOpen();
		corpusNameMap = mapdb.hashMap("index", Serializer.STRING, Serializer.STRING).createOrOpen();
	}

	public CacheResult get(String key) {
		String corpusName = corpusNameMap.get(key);
		if (corpusName != null) {
			Corpus corpus = corpora.get(corpusName);
			if (corpus != null) {
				Integer index = indexMap.get(key);
				return new CacheResult(corpus, index);
			}
			corpusNameMap.remove(key);
		}
		indexMap.remove(key);
		return null;
	}

	public void add(Corpus corpus, Integer index, String hash) {
		if (!corpora.containsValue(corpus)) {
			Gate.addCreoleListener(new CorpusUnloadListener(corpus, this));
		}
		Corpus old = corpora.put(corpus.getName(), corpus);
		if (old != null && old != corpus) {
			throw new IllegalStateException("corpus has same name");
		}
		synchronized (this) {
			corpusNameMap.put(hash, corpus.getName());
			indexMap.put(hash, index);
		}
	}

	public void clear() {
		indexMap.clear();
		corpusNameMap.clear();
	}

	public static class CacheResult {

		private final Corpus corpus;
		private final Integer index;

		public CacheResult(Corpus corpus, Integer index) {
			this.corpus = corpus;
			this.index = index;
		}

		public Corpus getCorpus() {
			return corpus;
		}

		public Integer getIndex() {
			return index;
		}

	}

	private static class CorpusUnloadListener implements CreoleListener {

		private Corpus corpus;
		private Cache cache;

		private CorpusUnloadListener(Corpus corpus, Cache cache) {
			this.corpus = corpus;
			this.cache = cache;
		}

		@Override
		public void resourceLoaded(CreoleEvent e) {

		}

		@Override
		public void resourceUnloaded(CreoleEvent e) {
			if (e.getResource().equals(corpus)) {
				cache.corpora.remove(corpus.getName());
				Gate.getCreoleRegister().removeCreoleListener(this);
			}
		}

		@Override
		public void datastoreOpened(CreoleEvent e) {

		}

		@Override
		public void datastoreCreated(CreoleEvent e) {

		}

		@Override
		public void datastoreClosed(CreoleEvent e) {

		}

		@Override
		public void resourceRenamed(Resource resource, String oldName, String newName) {
			if (resource.equals(corpus) && !cache.corpora.get(newName).equals(resource)) {
				throw new IllegalStateException("corpus renaming not supported by cache");
			}
		}

	}

}
