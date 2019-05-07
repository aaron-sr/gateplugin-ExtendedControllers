package gate.languageanalysers;

import org.apache.log4j.Logger;

import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "FeatureSetValue", comment = "Language analyser just to the value of a feature")
public class FeatureSetValue extends AbstractLanguageAnalyser {
	private static final long serialVersionUID = 2835981105764308245L;
	private static Logger logger = Logger.getLogger(FeatureSetValue.class);

	private Object featureKey;
	private Object featureValue;

	@Override
	public void execute() throws ExecutionException {
		document.getFeatures().put(featureKey, featureValue);
	}

	public Object getFeatureKey() {
		return featureKey;
	}

	@CreoleParameter(comment = "the key of the feature to set")
	public void setFeatureKey(Object featureKey) {
		this.featureKey = featureKey;
	}

	public Object getFeatureValue() {
		return featureValue;
	}

	@CreoleParameter(comment = "the value to be set by the feature key")
	public void setFeatureValue(Object featureValue) {
		this.featureValue = featureValue;
	}

}
