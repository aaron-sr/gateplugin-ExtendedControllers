package gate.languageanalysers;

import org.apache.log4j.Logger;

import gate.Resource;
import gate.creole.CustomDuplication;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "CacheWrite", comment = "Write part for cache, adds document index based on content (hash) to cache")
public class CacheWrite extends CacheAnalyser implements CustomDuplication {
	private static final long serialVersionUID = -3752065726491150003L;
	private static Logger logger = Logger.getLogger(CacheWrite.class);

	@Override
	public Resource init() throws ResourceInstantiationException {
		initCache();
		return this;
	}

	@Override
	public void execute() throws ExecutionException {
		String hash = buildHash(document);
		int index;
		synchronized (corpus) {
			index = corpus.indexOf(document);
		}

		cache.add(corpus, index, hash);
	}

}
