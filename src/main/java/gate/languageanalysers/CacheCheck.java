package gate.languageanalysers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;
import gate.Factory;
import gate.Factory.DuplicationContext;
import gate.Gate;
import gate.Resource;
import gate.creole.AbstractResource;
import gate.creole.CustomDuplication;
import gate.creole.ExecutionException;
import gate.creole.ResourceData;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.relations.Relation;
import gate.relations.RelationSet;
import gate.util.GateRuntimeException;
import gate.util.InvalidOffsetException;

@CreoleResource(name = "CacheCheck", comment = "Check part for cache, if document with same content was already processed and in case annotations, relations and features are copied")
public class CacheCheck extends CacheAnalyser implements CustomDuplication {
	private static final long serialVersionUID = -864376621998897297L;
	private static Logger logger = Logger.getLogger(CacheCheck.class);

	private Set<String> annotationSetNames;
	private Set<String> relationSetNames;
	private Set<Object> featureKeys;

	@Override
	public Resource init() throws ResourceInstantiationException {
		initCache();
		return this;
	}

	@Override
	public Resource duplicate(DuplicationContext ctx) throws ResourceInstantiationException {
		ResourceData resourceData = Gate.getCreoleRegister().get(CacheCheck.class.getCanonicalName());
		CacheCheck duplicate = new CacheCheck();

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
		Integer cloneIndex = cache.get(hash);
		if (cloneIndex != null) {
			synchronized (corpus) {
				int documentIndex = corpus.indexOf(document);
				if (documentIndex == cloneIndex) {
					return;
				}
				boolean unloadClone = !corpus.isDocumentLoaded(cloneIndex);
				Document cloneDocument = corpus.get(cloneIndex);
				if (document.getContent().toString().contentEquals(cloneDocument.getContent().toString())) {
					copyDocumentValues(cloneDocument, document, annotationSetNames, relationSetNames, featureKeys);
				}
				if (unloadClone) {
					Factory.deleteResource(cloneDocument);
				}
			}
		}
	}

	public static final void copyDocumentValues(Document fromDocument, Document toDocument,
			Set<String> annotationSetNames, Set<String> relationSetNames, Set<Object> featureKeys) {
		Map<Integer, Integer> annotationMapping = new HashMap<>();
		if ((annotationSetNames == null || annotationSetNames.contains(""))
				&& !fromDocument.getAnnotations().isEmpty()) {
			for (Annotation annotation : fromDocument.getAnnotations()) {
				annotationMapping.put(annotation.getId(), copyAnnotation(toDocument.getAnnotations(), annotation));
			}
			if ((relationSetNames == null || relationSetNames.contains(""))
					&& !fromDocument.getAnnotations().getRelations().isEmpty()) {
				for (Relation relation : fromDocument.getAnnotations().getRelations()) {
					copyRelation(toDocument.getAnnotations().getRelations(), relation, annotationMapping);
				}
			}
		}
		for (String annotationSetName : fromDocument.getAnnotationSetNames()) {
			if (annotationSetName.length() == 0) {
				continue;
			}
			if (annotationSetNames != null && !annotationSetNames.contains(annotationSetName)) {
				continue;
			}
			annotationMapping.clear();
			if (!fromDocument.getAnnotations(annotationSetName).isEmpty()) {
				for (Annotation annotation : fromDocument.getAnnotations(annotationSetName)) {
					annotationMapping.put(annotation.getId(),
							copyAnnotation(toDocument.getAnnotations(annotationSetName), annotation));
				}
			}
			if (relationSetNames != null && !relationSetNames.contains(annotationSetName)) {
				continue;
			}
			if (!fromDocument.getAnnotations(annotationSetName).getRelations().isEmpty()) {
				for (Relation relation : fromDocument.getAnnotations(annotationSetName).getRelations()) {
					copyRelation(toDocument.getAnnotations().getRelations(), relation, annotationMapping);
				}
			}
		}
		if (!fromDocument.getFeatures().isEmpty()) {
			for (Entry<Object, Object> entry : fromDocument.getFeatures().entrySet()) {
				if (featureKeys == null || featureKeys.contains(entry.getKey())) {
					toDocument.getFeatures().put(entry.getKey(), entry.getValue());
				}
			}
		}
	}

	public static final Integer copyAnnotation(AnnotationSet annotationSet, Annotation annotation) {
		try {
			return annotationSet.add(annotation.getStartNode().getOffset(), annotation.getEndNode().getOffset(),
					annotation.getType(), annotation.getFeatures());
		} catch (InvalidOffsetException e) {
			throw new GateRuntimeException(e);
		}
	}

	public static final Integer copyRelation(RelationSet relations, Relation relation,
			Map<Integer, Integer> annotationMapping) {
		List<Integer> members = new ArrayList<>();
		for (Integer annotationId : relation.getMembers()) {
			members.add(annotationMapping.get(annotationId));
		}
		return relations.addRelation(relation.getType(), members.stream().mapToInt(i -> i).toArray()).getId();
	}

	public Set<String> getAnnotationSetNames() {
		return annotationSetNames;
	}

	@CreoleParameter(comment = "the names of annotation sets to copy from cached document (empty Set means all)")
	public void setAnnotationSetNames(Set<String> annotationSetNames) {
		this.annotationSetNames = annotationSetNames;
	}

	public Set<String> getRelationSetNames() {
		return relationSetNames;
	}

	@CreoleParameter(comment = "the names of annotation sets to copy relations from cached document (empty Set means all)")
	public void setRelationSetNames(Set<String> relationSetNames) {
		this.relationSetNames = relationSetNames;
	}

	public Set<Object> getFeatureKeys() {
		return featureKeys;
	}

	@CreoleParameter(comment = "the keys of features to copy from cached document (empty Set means all)")
	public void setFeatureKeys(Set<Object> featureKeys) {
		this.featureKeys = featureKeys;
	}

}
