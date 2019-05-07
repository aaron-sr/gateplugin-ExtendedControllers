package gate.languageanalysers;

import org.apache.log4j.Logger;

import gate.Factory;
import gate.Factory.DuplicationContext;
import gate.Gate;
import gate.Resource;
import gate.creole.AbstractResource;
import gate.creole.CustomDuplication;
import gate.creole.ExecutionException;
import gate.creole.ResourceData;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "CacheAdd", comment = "Add part for cache, adds document index based on content (hash) to cache")
public class CacheAdd extends CacheAnalyser implements CustomDuplication {
	private static final long serialVersionUID = -3752065726491150003L;
	private static Logger logger = Logger.getLogger(CacheAdd.class);

	@Override
	public Resource init() throws ResourceInstantiationException {
		initCache();
		return this;
	}

	@Override
	public Resource duplicate(DuplicationContext ctx) throws ResourceInstantiationException {
		ResourceData resourceData = Gate.getCreoleRegister().get(CacheAdd.class.getCanonicalName());
		CacheAdd duplicate = new CacheAdd();

		duplicate.setName(resourceData.getName() + "_" + Gate.genSym());
		AbstractResource.setParameterValues(duplicate, getInitParameterValues());
		AbstractResource.setParameterValues(duplicate, getRuntimeParameterValues());
		duplicate.setFeatures(Factory.newFeatureMap());
		duplicate.getFeatures().putAll(getFeatures());

		duplicate.messageDigest = initMessageDigest();
		duplicate.cache = cache;

		resourceData.addInstantiation(duplicate);
		return duplicate;
	}

	@Override
	public void execute() throws ExecutionException {
		String hash = buildHash(document);
		int index;
		synchronized (corpus) {
			index = corpus.indexOf(document);
		}

		cache.putIfAbsent(hash, index);
	}

}
