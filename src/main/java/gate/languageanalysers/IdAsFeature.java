package gate.languageanalysers;

import gate.Annotation;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.creole.metadata.RunTime;
import gate.relations.Relation;
import gate.util.FeatureBearer;
import gate.util.IdBearer;

@CreoleResource(name = "IdAsFeature", comment = "Language analyser just to copy the id as a feature (e.g. visibility in annotation stack inside GATE Developer)")
public class IdAsFeature extends AbstractLanguageAnalyser {
	private static final long serialVersionUID = 7579024710809470409L;

	private Object featureKey;

	@Override
	public void execute() throws ExecutionException {
		for (Annotation annotation : document.getAnnotations()) {
			setIdAsFeature(annotation);
		}
		for (Relation relation : document.getAnnotations().getRelations()) {
			setIdAsFeature(relation);
		}
		for (String annotationSetName : document.getAnnotationSetNames()) {
			for (Annotation annotation : document.getAnnotations(annotationSetName)) {
				setIdAsFeature(annotation);
			}
			for (Relation relation : document.getAnnotations(annotationSetName).getRelations()) {
				setIdAsFeature(relation);
			}
		}
	}

	private void setIdAsFeature(IdBearer idBearer) {
		if (idBearer instanceof FeatureBearer) {
			FeatureBearer featureBearer = (FeatureBearer) idBearer;
			featureBearer.getFeatures().put(featureKey, idBearer.getId());
		} else {
			throw new IllegalArgumentException("no feature bearer: " + idBearer);
		}
	}

	public Object getFeatureKey() {
		return featureKey;
	}

	@Optional
	@RunTime
	@CreoleParameter(comment = "the key of the feature to set", defaultValue = "_id")
	public void setFeatureKey(Object featureKey) {
		this.featureKey = featureKey;
	}

}
