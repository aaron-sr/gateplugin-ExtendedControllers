package gate.strategies;

import gate.ProcessingResource;
import gate.creole.RunningStrategy;

public interface DuplicatableRunningStrategy extends RunningStrategy {

	RunningStrategy duplicate(ProcessingResource processingResource);

}
