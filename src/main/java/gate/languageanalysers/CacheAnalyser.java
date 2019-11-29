package gate.languageanalysers;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.log4j.Logger;

import gate.Document;
import gate.Factory;
import gate.Factory.DuplicationContext;
import gate.Gate;
import gate.Resource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.AbstractResource;
import gate.creole.ResourceData;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;

public class CacheAnalyser extends AbstractLanguageAnalyser {
	private static final long serialVersionUID = 6870246820429562465L;
	private static Logger logger = Logger.getLogger(CacheAnalyser.class);

	protected String cacheName;
	protected String hashAlgorithm;

	protected MessageDigest messageDigest;
	protected Map<String, Integer> cache;

	protected void initCache() throws ResourceInstantiationException {
		cache = CacheRegister.getInstance().getCache(cacheName);
		messageDigest = initMessageDigest();
	}

	public Resource duplicate(DuplicationContext ctx) throws ResourceInstantiationException {
		ResourceData resourceData = Gate.getCreoleRegister().get(this.getClass().getCanonicalName());
		CacheAnalyser duplicate;
		try {
			duplicate = this.getClass().getConstructor().newInstance();
		} catch (Exception e) {
			throw new ResourceInstantiationException(e);
		}

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

	protected final MessageDigest initMessageDigest() throws ResourceInstantiationException {
		try {
			return MessageDigest.getInstance(hashAlgorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new ResourceInstantiationException(e);
		}
	}

	protected String buildHash(Document document) {
		String content = document.getContent().toString();
		messageDigest.update(content.getBytes());
		byte[] hashBytes = messageDigest.digest();
		messageDigest.reset();
		String hashString = new BigInteger(1, hashBytes).toString(16);
		return hashString;
	}

	public String getCacheName() {
		return cacheName;
	}

	@CreoleParameter(comment = "the name of the cache", defaultValue = "cache")
	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	public String getHashAlgorithm() {
		return hashAlgorithm;
	}

	@CreoleParameter(comment = "the name of the hash function (need to be an instance from java.security.MessageDigest)", defaultValue = "SHA-1")
	public void setHashAlgorithm(String hashAlgorithm) {
		this.hashAlgorithm = hashAlgorithm;
	}

}
