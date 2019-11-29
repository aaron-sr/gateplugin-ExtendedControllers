package gate.languageanalysers;

import java.util.Set;

import org.apache.log4j.Logger;

import gate.Annotation;
import gate.Document;
import gate.Resource;
import gate.creole.CustomDuplication;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.relations.Relation;

@CreoleResource(name = "CacheReadAndAdd", comment = "Read part for cache, if document with same content was already processed and in case annotations, relations and features added")
public class CacheReadAndAdd extends CacheRead implements CustomDuplication {
	private static final long serialVersionUID = -864376621998897297L;
	private static Logger logger = Logger.getLogger(CacheReadAndAdd.class);

	@Override
	public Resource init() throws ResourceInstantiationException {
		initCache();
		return this;
	}

	@Override
	protected void applyCache(Document fromDocument, Document toDocument) {
		for (Annotation annotation : fromDocument.getAnnotations()) {
			if (toDocument.getAnnotations().get(annotation.getId()) != null) {
				alreadyPresentException("annotation", annotation.getId(), "default");
			}
		}
		for (Relation relation : fromDocument.getAnnotations().getRelations()) {
			if (toDocument.getAnnotations().getRelations().get(relation.getId()) != null) {
				alreadyPresentException("relation", relation.getId(), "default");
			}
		}
		for (String annotationSetName : fromDocument.getAnnotationSetNames()) {
			for (Annotation annotation : fromDocument.getAnnotations(annotationSetName)) {
				if (toDocument.getAnnotations(annotationSetName).get(annotation.getId()) != null) {
					alreadyPresentException("annotation", annotation.getId(), annotationSetName);
				}
			}
			for (Relation relation : fromDocument.getAnnotations(annotationSetName).getRelations()) {
				if (toDocument.getAnnotations(annotationSetName).getRelations().get(relation.getId()) != null) {
					alreadyPresentException("relation", relation.getId(), annotationSetName);
				}
			}
		}

		for (Annotation annotation : fromDocument.getAnnotations()) {
			toDocument.getAnnotations().add(annotation);
		}
		for (Relation relation : fromDocument.getAnnotations().getRelations()) {
			toDocument.getAnnotations().getRelations().add(relation);
		}
		for (String annotationSetName : fromDocument.getAnnotationSetNames()) {
			for (Annotation annotation : fromDocument.getAnnotations(annotationSetName)) {
				toDocument.getAnnotations(annotationSetName).add(annotation);
			}
			for (Relation relation : fromDocument.getAnnotations(annotationSetName).getRelations()) {
				toDocument.getAnnotations(annotationSetName).getRelations().add(relation);
			}
		}

		copyFeatures(fromDocument, toDocument);
	}

	private void alreadyPresentException(String type, Integer id, String setName) {
		throw new IllegalStateException(String.format("%s id %d already present in annotation set", type, id, setName));
	}

	public Set<Object> getFeatureKeys() {
		return featureKeys;
	}

	@CreoleParameter(comment = "the keys of features to copy from cached document (empty Set means all)")
	public void setFeatureKeys(Set<Object> featureKeys) {
		this.featureKeys = featureKeys;
	}

}
