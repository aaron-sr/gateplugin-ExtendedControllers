package gate.controllers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import gate.ProcessingResource;
import gate.creole.ConditionalController;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.creole.RunningStrategy;
import gate.creole.metadata.CreoleResource;
import gate.strategies.RunningStrategyDuplicator;

@CreoleResource(name = "Conditional Parallel Pipeline", comment = "A parallel controller for PR pipelines, which duplicates processing resources and running strategies to run the pipeline in parallel on multiple documents")
public class ConditionalParallelDocumentAnalyserController extends ParallelDocumentAnalyserController
		implements ConditionalController {
	private static final long serialVersionUID = 6715814753335644867L;
	private static Logger logger = Logger.getLogger(ConditionalParallelDocumentAnalyserController.class);

	protected List<RunningStrategy> strategiesList = new ArrayList<>();
	private Map<ProcessingResource, List<RunningStrategy>> parallelRunningStrategies;

	@Override
	public void cleanup() {
		super.cleanup();
		strategiesList.clear();
	}

	@Override
	public List<RunningStrategy> getRunningStrategies() {
		return Collections.unmodifiableList(strategiesList);
	}

	@Override
	public void setRunningStrategies(Collection<RunningStrategy> strategies) {
		strategiesList.clear();
		strategiesList.addAll(strategies);
	}

	@Override
	protected Collection<List<ProcessingResource>> buildParallelProcessingResources()
			throws ResourceInstantiationException {
		Collection<List<ProcessingResource>> parallelProcessingResources = super.buildParallelProcessingResources();

		parallelRunningStrategies = buildParallelRunningStrategies(parallelProcessingResources);

		return parallelProcessingResources;
	}

	@Override
	protected void cleanupDuplicatedResources(Collection<List<ProcessingResource>> parallelProcessingResources) {
		parallelRunningStrategies.clear();
		super.cleanupDuplicatedResources(parallelProcessingResources);
	}

	protected Map<ProcessingResource, List<RunningStrategy>> buildParallelRunningStrategies(
			Collection<List<ProcessingResource>> parallelProcessingResources) {
		Map<ProcessingResource, List<RunningStrategy>> mappedStrategies = new IdentityHashMap<>();
		for (List<ProcessingResource> processingResources : parallelProcessingResources) {
			List<RunningStrategy> runningStrategies = new ArrayList<RunningStrategy>();
			for (int i = 0; i < processingResources.size(); i++) {
				ProcessingResource processingResource = processingResources.get(i);
				if (this.strategiesList.size() > i) {
					RunningStrategy runningStrategy = this.strategiesList.get(i);
					if (processingResources != this.processingResources) {
						runningStrategy = RunningStrategyDuplicator.duplicate(processingResource, runningStrategy);
					}
					runningStrategies.add(i, runningStrategy);
				}
			}
			for (ProcessingResource processingResource : processingResources) {
				mappedStrategies.put(processingResource, runningStrategies);
			}
		}
		return mappedStrategies;
	}

	@Override
	protected void runComponent(int documentIndex, int processingResourceIndex, ProcessingResource processingResource)
			throws ExecutionException {
		if (isParallelExecution()) {
			RunningStrategy runningStrategy = parallelRunningStrategies.get(processingResource)
					.get(processingResourceIndex);
			if (runningStrategy.shouldRun()) {
				super.runComponent(documentIndex, processingResourceIndex, processingResource);
			}
		} else if (isNoneParallelExecution()) {
			RunningStrategy runningStrategy = strategiesList.get(processingResourceIndex);
			if (runningStrategy.shouldRun()) {
				super.runComponent(documentIndex, processingResourceIndex, processingResource);
			}
		}
	}

}
