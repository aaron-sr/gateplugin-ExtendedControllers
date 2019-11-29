package gate.languageanalysers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;
import gate.Resource;
import gate.creole.CustomDuplication;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.relations.Relation;
import gate.relations.RelationSet;
import gate.util.GateRuntimeException;
import gate.util.InvalidOffsetException;

@CreoleResource(name = "CacheReadAndCopy", comment = "Check part for cache, if document with same content was already processed and in case annotations, relations and features values are copied")
public class CacheReadAndCopy extends CacheRead implements CustomDuplication {
	private static final long serialVersionUID = -864376621998897297L;
	private static Logger logger = Logger.getLogger(CacheReadAndCopy.class);

	private Set<String> annotationSetNames;
	private Set<String> relationSetNames;

	@Override
	public Resource init() throws ResourceInstantiationException {
		initCache();
		return this;
	}

	@Override
	protected void applyCache(Document fromDocument, Document toDocument) {
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

		copyFeatures(fromDocument, toDocument);
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
