package gate.languageanalysers;

import java.util.Map.Entry;
import java.util.Set;

import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.creole.ExecutionException;
import gate.languageanalysers.Cache.CacheResult;

public abstract class CacheRead extends CacheAnalyser {
	private static final long serialVersionUID = 8779736540972451396L;

	protected Set<Object> featureKeys;

	@Override
	public void execute() throws ExecutionException {
		String hash = buildHash(document);
		CacheResult result = cache.get(hash);
		if (result != null) {
			Corpus cloneCorpus = result.getCorpus();
			Integer cloneIndex = result.getIndex();
			synchronized (cloneCorpus) {
				int documentIndex = cloneCorpus.indexOf(document);
				if (documentIndex == cloneIndex) {
					return;
				}
				boolean unloadClone = !cloneCorpus.isDocumentLoaded(cloneIndex);
				Document cloneDocument = cloneCorpus.get(cloneIndex);
				if (document.getContent().toString().contentEquals(cloneDocument.getContent().toString())) {
					applyCache(cloneDocument, document);
				}
				if (unloadClone) {
					Factory.deleteResource(cloneDocument);
				}
			}
		}
	}

	protected abstract void applyCache(Document fromDocument, Document toDocument);

	protected void copyFeatures(Document fromDocument, Document toDocument) {
		if (!fromDocument.getFeatures().isEmpty()) {

			if (featureKeys != null) {
				for (Entry<Object, Object> entry : fromDocument.getFeatures().entrySet()) {
					if (featureKeys.contains(entry.getKey())) {
						toDocument.getFeatures().put(entry.getKey(), entry.getValue());
					}
				}
			} else {
				toDocument.getFeatures().putAll(fromDocument.getFeatures());
			}

		}
	}

}
