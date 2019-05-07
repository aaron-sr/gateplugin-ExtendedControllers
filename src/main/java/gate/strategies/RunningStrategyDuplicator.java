package gate.strategies;

import gate.LanguageAnalyser;
import gate.ProcessingResource;
import gate.creole.AnalyserRunningStrategy;
import gate.creole.RunningStrategy;
import gate.creole.RunningStrategy.UnconditionalRunningStrategy;

public class RunningStrategyDuplicator {

	public static final RunningStrategy duplicate(ProcessingResource processingResource,
			RunningStrategy runningStrategy) {
		if (runningStrategy instanceof UnconditionalRunningStrategy) {
			return new UnconditionalRunningStrategy(processingResource, runningStrategy.shouldRun());
		} else if (runningStrategy instanceof AnalyserRunningStrategy) {
			AnalyserRunningStrategy analyserRunningStrategy = (AnalyserRunningStrategy) runningStrategy;
			return new AnalyserRunningStrategy((LanguageAnalyser) processingResource, runningStrategy.getRunMode(),
					analyserRunningStrategy.getFeatureName(), analyserRunningStrategy.getFeatureValue());
		} else if (runningStrategy instanceof DuplicatableRunningStrategy) {
			return ((DuplicatableRunningStrategy) runningStrategy).duplicate(processingResource);
		} else {
			throw new UnsupportedOperationException("cannot clone RunningStrategy " + runningStrategy.getClass());
		}
	}

}
