package gate.strategies;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import gate.ProcessingResource;
import gate.creole.RunningStrategy;

public class DefaultRunningStrategies {

	public static final RunningStrategy and(RunningStrategy... runningStrategies) {
		return new JunctionRunningStrategy(JunctionType.AND, Arrays.asList(runningStrategies));
	}

	public static final RunningStrategy or(RunningStrategy... runningStrategies) {
		return new JunctionRunningStrategy(JunctionType.OR, Arrays.asList(runningStrategies));
	}

	public static final RunningStrategy not(RunningStrategy runningStrategy) {
		return new NegatedRunningStrategy(runningStrategy);
	}

	public static enum JunctionType {
		AND, OR;

	}

	public static final class JunctionRunningStrategy implements DuplicatableRunningStrategy {

		private JunctionType type;
		private List<RunningStrategy> runningStrategies;

		public JunctionRunningStrategy(JunctionType type, List<RunningStrategy> runningStrategies) {
			this.type = type;
			this.runningStrategies = runningStrategies;
		}

		@Override
		public boolean shouldRun() {
			switch (type) {
			case AND:
				for (RunningStrategy runningStrategy : runningStrategies) {
					if (!runningStrategy.shouldRun()) {
						return false;
					}
				}
				return true;
			case OR:
				for (RunningStrategy runningStrategy : runningStrategies) {
					if (runningStrategy.shouldRun()) {
						return true;
					}
				}
				return false;

			}
			throw new IllegalStateException();
		}

		@Override
		public int getRunMode() {
			return RunningStrategy.RUN_CONDITIONAL;
		}

		@Override
		public ProcessingResource getPR() {
			return runningStrategies.get(0).getPR();
		}

		@Override
		public RunningStrategy duplicate(ProcessingResource processingResource) {
			List<RunningStrategy> clones = runningStrategies.stream()
					.map(runningStrategy -> RunningStrategyDuplicator.duplicate(processingResource, runningStrategy))
					.collect(Collectors.toList());
			JunctionRunningStrategy clone = new JunctionRunningStrategy(type, clones);
			return clone;
		}
	}

	public static final class NegatedRunningStrategy implements DuplicatableRunningStrategy {

		private RunningStrategy runningStrategy;

		public NegatedRunningStrategy(RunningStrategy runningStrategy) {
			this.runningStrategy = runningStrategy;
		}

		@Override
		public boolean shouldRun() {
			return !runningStrategy.shouldRun();
		}

		@Override
		public int getRunMode() {
			if (runningStrategy.getRunMode() == RUN_ALWAYS) {
				return RUN_NEVER;
			}
			if (runningStrategy.getRunMode() == RUN_NEVER) {
				return RUN_ALWAYS;
			}
			return RUN_CONDITIONAL;
		}

		@Override
		public ProcessingResource getPR() {
			return runningStrategy.getPR();
		}

		@Override
		public RunningStrategy duplicate(ProcessingResource processingResource) {
			NegatedRunningStrategy clone = new NegatedRunningStrategy(
					RunningStrategyDuplicator.duplicate(processingResource, runningStrategy));
			return clone;
		}

	}

}
