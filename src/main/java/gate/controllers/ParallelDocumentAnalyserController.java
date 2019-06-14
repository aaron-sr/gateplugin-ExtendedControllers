package gate.controllers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import gate.Controller;
import gate.Corpus;
import gate.CorpusController;
import gate.Document;
import gate.Factory;
import gate.Gate;
import gate.LanguageAnalyser;
import gate.ProcessingResource;
import gate.Resource;
import gate.creole.AbstractController;
import gate.creole.ExecutionException;
import gate.creole.ExecutionInterruptedException;
import gate.creole.Parameter;
import gate.creole.ResourceData;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.creole.metadata.RunTime;
import gate.event.ControllerEvent;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;
import gate.util.GateException;
import gate.util.GateRuntimeException;

@CreoleResource(name = "Parallel Document Pipeline", comment = "A parallel controller for PR pipelines, which duplicates processing resources to run the pipeline in parallel on multiple documents")
public class ParallelDocumentAnalyserController extends AbstractController
		implements CorpusController, LanguageAnalyser, CreoleListener {
	private static final long serialVersionUID = 5176305057028072578L;
	private static Logger logger = Logger.getLogger(ParallelDocumentAnalyserController.class);

	private Integer parallelTasks;
	private Boolean failOnException;
	private Boolean synchronizeCorpus;
	private Boolean orderedDocumentLoading;

	private ExecutorService executor;
	private boolean shutdownExecutor = false;

	protected List<ProcessingResource> processingResources;
	protected Corpus corpus;
	protected Document document;

	private ExecutorQueue queue;
	private Collection<ProcessingResource> parallelProcessingResources;

	public ParallelDocumentAnalyserController() {
		processingResources = Collections.synchronizedList(new ArrayList<ProcessingResource>());
		Gate.getCreoleRegister().addCreoleListener(this);
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		if (executor == null) {
			executor = Executors.newCachedThreadPool();
			shutdownExecutor = true;
		}
		return this;
	}

	@Override
	protected void executeImpl() throws ExecutionException {
		interrupted = false;
		if (corpus == null) {
			throw new ExecutionException("corpus is null");
		}
		if (document == null && parallelTasks > 1) {
			executeParallel();
		} else {
			executeNoneParallel();
		}
	}

	protected void executeNoneParallel() throws ExecutionException {
		if (document == null) {
			for (int documentIndex = 0; documentIndex < corpus.size(); documentIndex++) {
				if (isInterrupted()) {
					throw new ExecutionInterruptedException();
				}
				boolean unloadDocument;
				Document document;
				if (synchronizeCorpus) {
					synchronized (corpus) {
						unloadDocument = !corpus.isDocumentLoaded(documentIndex);
						document = corpus.get(documentIndex);
					}
				} else {
					unloadDocument = !corpus.isDocumentLoaded(documentIndex);
					document = corpus.get(documentIndex);
				}
				try {
					executeProcessingResources(processingResources, documentIndex, document);
				} catch (ExecutionException e) {
					if (failOnException) {
						throw e;
					} else {
						logger.error("exception occured while processing", e);
					}
				} finally {
					if (unloadDocument) {
						if (synchronizeCorpus) {
							synchronized (corpus) {
								Factory.deleteResource(document);
							}
						} else {
							Factory.deleteResource(document);
						}
					}
				}
			}
		} else {
			executeProcessingResources(processingResources, corpus.indexOf(document), document);
		}
	}

	protected void executeParallel() throws ExecutionException {
		Collection<List<ProcessingResource>> parallelProcessingResources;
		try {
			parallelProcessingResources = buildParallelProcessingResources();
			this.parallelProcessingResources = new ArrayList<>();
			for (List<ProcessingResource> processingResources : parallelProcessingResources) {
				this.parallelProcessingResources.addAll(processingResources);
			}
		} catch (ResourceInstantiationException e) {
			throw new ExecutionException(e);
		}

		queue = new ExecutorQueue(executor, parallelTasks);
		AtomicInteger documentIndexHolder = new AtomicInteger(0);

		queue.submit(new Iterator<Runnable>() {

			@Override
			public boolean hasNext() {
				return documentIndexHolder.get() < corpus.size();
			}

			@Override
			public Runnable next() {
				int documentIndex = documentIndexHolder.getAndAdd(1);

				Boolean unloadDocumentUpfront;
				Document documentUpfront;
				if (orderedDocumentLoading) {
					if (synchronizeCorpus) {
						synchronized (corpus) {
							unloadDocumentUpfront = !corpus.isDocumentLoaded(documentIndex);
							documentUpfront = corpus.get(documentIndex);
						}
					} else {
						unloadDocumentUpfront = !corpus.isDocumentLoaded(documentIndex);
						documentUpfront = corpus.get(documentIndex);
					}
				} else {
					unloadDocumentUpfront = null;
					documentUpfront = null;
				}

				return new Runnable() {

					@Override
					public void run() {
						boolean unloadDocument;
						Document document;
						if (orderedDocumentLoading) {
							unloadDocument = unloadDocumentUpfront;
							document = documentUpfront;
						} else {
							if (synchronizeCorpus) {
								synchronized (corpus) {
									unloadDocument = !corpus.isDocumentLoaded(documentIndex);
									document = corpus.get(documentIndex);
								}
							} else {
								unloadDocument = !corpus.isDocumentLoaded(documentIndex);
								document = corpus.get(documentIndex);
							}
						}
						try {
							executeDocumentParallel(parallelProcessingResources, documentIndex, document);
						} finally {
							if (unloadDocument) {
								if (synchronizeCorpus) {
									synchronized (corpus) {
										Factory.deleteResource(document);
									}
								} else {
									Factory.deleteResource(document);
								}
							}
						}
					}

					private void executeDocumentParallel(
							Collection<List<ProcessingResource>> parallelProcessingResources, Integer documentIndex,
							Document document) {
						List<ProcessingResource> processingResources;
						synchronized (parallelProcessingResources) {
							Iterator<List<ProcessingResource>> iterator = parallelProcessingResources.iterator();
							processingResources = iterator.next();
							iterator.remove();
						}
						try {
							executeProcessingResources(processingResources, documentIndex, document);
						} catch (ExecutionException e) {
							throw new RuntimeException(e);
						} finally {
							synchronized (parallelProcessingResources) {
								parallelProcessingResources.add(processingResources);
							}
						}
					}
				};
			}

		});

		try {
			while (!queue.hasCompleted()) {
				try {
					queue.awaitCompleted();
				} catch (Exception e) {
					if (failOnException) {
						queue.interrupt();
						while (true) {
							try {
								queue.awaitCompleted();
								break;
							} catch (Exception e1) {
								logger.error("another parallel execution exception", e1);
							}
						}
						throw new ExecutionException(e);
					} else {
						logger.error("exception occured while processing", e);
					}
				}
				if (isInterrupted()) {
					throw new ExecutionInterruptedException();
				}
			}
		} finally {
			synchronized (parallelProcessingResources) {
				cleanupDuplicatedResources(parallelProcessingResources);
			}
		}
	}

	protected Collection<List<ProcessingResource>> buildParallelProcessingResources()
			throws ResourceInstantiationException {
		Collection<List<ProcessingResource>> parallelProcessingResources = new ArrayList<>();
		parallelProcessingResources.add(processingResources);
		for (int i = 1; i < parallelTasks; i++) {
			List<ProcessingResource> duplicatedResources = new ArrayList<ProcessingResource>();
			for (ProcessingResource processingResource : processingResources) {
				duplicatedResources.add((ProcessingResource) Factory.duplicate(processingResource));
			}
			parallelProcessingResources.add(duplicatedResources);
		}
		return parallelProcessingResources;
	}

	protected void cleanupDuplicatedResources(Collection<List<ProcessingResource>> parallelProcessingResources) {
		for (List<ProcessingResource> processingResources : parallelProcessingResources) {
			if (processingResources != this.processingResources) {
				for (ProcessingResource processingResource : processingResources) {
					Factory.deleteResource(processingResource);
				}
			}
		}
	}

	protected void executeProcessingResources(List<ProcessingResource> processingResources, Integer documentIndex,
			Document document) throws ExecutionException {
		for (int processingResourceIndex = 0; processingResourceIndex < processingResources
				.size(); processingResourceIndex++) {
			if (isInterrupted()) {
				return;
			}
			ProcessingResource processingResource = processingResources.get(processingResourceIndex);
			if (processingResource instanceof LanguageAnalyser) {
				LanguageAnalyser languageAnalyser = (LanguageAnalyser) processingResource;
				languageAnalyser.setCorpus(corpus);
				languageAnalyser.setDocument(document);
			}
			runComponent(documentIndex, processingResourceIndex, processingResource);
			if (processingResource instanceof LanguageAnalyser) {
				LanguageAnalyser languageAnalyser = (LanguageAnalyser) processingResource;
				languageAnalyser.setCorpus(null);
				languageAnalyser.setDocument(null);
			}
		}
	}

	protected void runComponent(int documentIndex, int processingResourceIndex, ProcessingResource processingResource)
			throws ExecutionException {
		try {
			processingResource.execute();
		} catch (Exception e) {
			throw new ExecutionException(
					"failed to run processing resource " + processingResourceIndex + " on document " + documentIndex,
					e);
		}
	}

	@Override
	public synchronized void interrupt() {
		if (document == null && parallelTasks > 1) {
			interruptParallel();
		} else {
			interruptNoneParallel();
		}
	}

	protected void interruptNoneParallel() {
		super.interrupt();
	}

	protected void interruptParallel() {
		interrupted = true;
		queue.interrupt();
		for (ProcessingResource processingResource : parallelProcessingResources) {
			processingResource.interrupt();
		}
	}

	@RunTime
	@CreoleParameter(comment = "run n parallel pipelines", defaultValue = "1")
	public void setParallelTasks(Integer parallelTasks) {
		this.parallelTasks = parallelTasks;
	}

	public Integer getParallelTasks() {
		return parallelTasks;
	}

	@RunTime
	@Optional
	@CreoleParameter(comment = "fail on exceptions while loading, processing and unloading documents (otherwise log)", defaultValue = "true")
	public void setFailOnException(Boolean failOnException) {
		this.failOnException = failOnException;
	}

	public Boolean getFailOnException() {
		return failOnException;
	}

	@RunTime
	@Optional
	@CreoleParameter(comment = "synchronize the access of the corpus to avoid concurrent exceptions", defaultValue = "true")
	public void setSynchronizeCorpus(Boolean synchronizeCorpus) {
		this.synchronizeCorpus = synchronizeCorpus;
	}

	public Boolean getSynchronizeCorpus() {
		return synchronizeCorpus;
	}

	@RunTime
	@Optional
	@CreoleParameter(comment = "load documents in order (e.g. to avoid extensive forward/backward skipping of the underlying data), slower since single-threaded", defaultValue = "true")
	public void setOrderedDocumentLoading(Boolean orderedDocumentLoading) {
		this.orderedDocumentLoading = orderedDocumentLoading;
	}

	public Boolean getOrderedDocumentLoading() {
		return orderedDocumentLoading;
	}

	@RunTime
	@Optional
	@CreoleParameter(comment = "use existing executor service (otherwise, a new one will be generated and shutdown, if resource is deleted)")
	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	@Override
	public void cleanup() {
		Gate.getCreoleRegister().removeCreoleListener(this);
		if (processingResources != null && !processingResources.isEmpty()) {
			try {
				List<Resource> otherControllers = Gate.getCreoleRegister().getAllInstances("gate.Controller");
				otherControllers.remove(this);
				List<Resource> otherResources = new ArrayList<Resource>();
				for (Resource controller : otherControllers) {
					otherResources.addAll(((Controller) controller).getPRs());
				}
				for (Resource resource : getPRs()) {
					if (!otherResources.contains(resource)) {
						Factory.deleteResource(resource);
					}
				}
			} catch (GateException e) {
				throw new GateRuntimeException(e);
			}
		}
		if (shutdownExecutor) {
			executor.shutdown();
		}
	}

	@Override
	public Collection<ProcessingResource> getPRs() {
		return Collections.unmodifiableList(processingResources);
	}

	@Override
	public void setPRs(Collection<? extends ProcessingResource> processingResources) {
		this.processingResources.clear();
		this.processingResources.addAll(processingResources);
	}

	public void add(int index, ProcessingResource processingResource) {
		processingResources.add(index, processingResource);
		fireResourceAdded(new ControllerEvent(this, ControllerEvent.RESOURCE_ADDED, processingResource));
	}

	public void add(ProcessingResource processingResource) {
		processingResources.add(processingResource);
		fireResourceAdded(new ControllerEvent(this, ControllerEvent.RESOURCE_ADDED, processingResource));
	}

	public ProcessingResource remove(int index) {
		ProcessingResource old = processingResources.remove(index);
		fireResourceRemoved(new ControllerEvent(this, ControllerEvent.RESOURCE_REMOVED, old));
		return old;
	}

	public boolean remove(ProcessingResource processingResource) {
		boolean changed = processingResources.remove(processingResource);
		if (changed)
			fireResourceRemoved(new ControllerEvent(this, ControllerEvent.RESOURCE_REMOVED, processingResource));
		return changed;
	}

	public ProcessingResource set(int index, ProcessingResource pr) {
		return processingResources.set(index, pr);
	}

	@Override
	public void resourceLoaded(CreoleEvent e) {
	}

	@Override
	public void resourceUnloaded(CreoleEvent e) {
		if (e.getResource() instanceof ProcessingResource) {
			while (remove((ProcessingResource) e.getResource()))
				;
		}
		for (ProcessingResource processingResource : processingResources) {
			ResourceData resourceData = Gate.getCreoleRegister().get(processingResource.getClass().getName());
			if (resourceData != null) {
				for (List<Parameter> parameters : resourceData.getParameterList().getRuntimeParameters()) {
					for (Parameter parameter : parameters) {
						String parameterName = parameter.getName();
						try {
							if (processingResource.getParameterValue(parameterName) == e.getResource()) {
								processingResource.setParameterValue(parameterName, null);
							}
						} catch (ResourceInstantiationException ex) {
							throw new GateRuntimeException(ex);
						}
					}
				}
			}
		}
	}

	@Override
	public void datastoreOpened(CreoleEvent e) {
	}

	@Override
	public void datastoreCreated(CreoleEvent e) {
	}

	@Override
	public void datastoreClosed(CreoleEvent e) {
	}

	@Override
	public void resourceRenamed(Resource resource, String oldName, String newName) {
	}

	@Override
	public void setDocument(Document document) {
		this.document = document;
	}

	@Override
	public Document getDocument() {
		return document;
	}

	@Override
	public void setCorpus(Corpus corpus) {
		this.corpus = corpus;
	}

	@Override
	public Corpus getCorpus() {
		return corpus;
	}
}
