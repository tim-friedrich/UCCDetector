package de.metanome.algorithms.fddetector;

import java.util.ArrayList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.OrderDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.UniqueColumnCombinationsAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;

public class FDDetector extends FDDetectorAlgorithm 				// Separating the algorithm implementation and the Metanome interface implementation is good practice
						  implements FunctionalDependencyAlgorithm, 			// Defines the type of the algorithm, i.e., the result type, for instance, FunctionalDependencyAlgorithm or InclusionDependencyAlgorithm; implementing multiple types is possible
						  			 RelationalInputParameterAlgorithm	// Defines the input type of the algorithm; relational input is any relational input from files or databases; more specific input specifications are possible
						  			  {	

	public enum Identifier {
		INPUT_GENERATOR
	};

	public String getAuthors() {
		return "Tim Friedrich, Jonas Bounama"; // A string listing the author(s) of this algorithm
	}

	public String getDescription() {
		return "Tane like fd detection."; // A string briefly describing what this algorithm does
	}
	
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() { // Tells Metanome which and how many parameters the algorithm needs
		ArrayList<ConfigurationRequirement<?>> conf = new ArrayList<>();
		conf.add(new ConfigurationRequirementRelationalInput(FDDetector.Identifier.INPUT_GENERATOR.name()));
		//conf.add(new ConfigurationRequirementRelationalInput(MyIndDetector.Identifier.INPUT_GENERATOR.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES)); // An algorithm can ask for more than one input; this is typical for IND detection algorithms
		
		return conf;
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (!FDDetector.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.handleUnknownConfiguration(identifier, values);
		this.inputGenerator = values[0];
	}

	@Override
	public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void execute() throws AlgorithmExecutionException {
		super.execute();
	}

	private void handleUnknownConfiguration(String identifier, Object[] values) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> [" + concat(values, ",") + "]");
	}
	
	private static String concat(Object[] objects, String separator) {
		if (objects == null)
			return "";
		
		StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < objects.length; i++) {
			buffer.append(objects[i].toString());
			if ((i + 1) < objects.length)
				buffer.append(separator);
		}
		return buffer.toString();
	}
}
