package de.metanome.algorithms.uccdetector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;

public class UCCDetectorAlgorithm {
	
	protected RelationalInputGenerator inputGenerator = null;
	protected UniqueColumnCombinationResultReceiver resultReceiver = null;
	
	protected String relationName;
	protected List<String> columnNames;
	protected List<List<String>> records;
	protected String someStringParameter;
	protected Integer someIntegerParameter;
	private Integer depth = 0;
	protected Boolean someBooleanParameter;
	
	public void execute() throws AlgorithmExecutionException {
		
		////////////////////////////////////////////
		// THE DISCOVERY ALGORITHM LIVES HERE :-) //
		////////////////////////////////////////////
		// Example: Initialize
		this.initialize();
		// Example: Read input data
		this.records = this.readInput();
		// Example: Print what the algorithm read (to test that everything works)
		this.print(records);
		// Example: Generate some results (usually, the algorithm should really calculate them on the data)
		List<UniqueColumnCombination> results = this.generateResults();
		
		// Example: To test if the algorithm outputs results
		this.emit(results);
		/////////////////////////////////////////////
		
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput input = this.inputGenerator.generateNewCopy();
		this.relationName = input.relationName();
		this.columnNames = input.columnNames();
	}
	
	protected List<List<String>> readInput() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
		List<List<String>> records = new ArrayList<>();
		RelationalInput input = this.inputGenerator.generateNewCopy();
		while (input.hasNext())
			records.add(input.next());
		return records;
	}
	
	protected void print(List<List<String>> records) {
		// Print parameter
		System.out.println("Some String: " + this.someStringParameter);
		System.out.println("Some Integer: " + this.someIntegerParameter);
		System.out.println("Some Boolean: " + this.someBooleanParameter);
		System.out.println();
		
		// Print schema
		System.out.print(this.relationName + "( ");
		for (String columnName : this.columnNames)
			System.out.print(columnName + " ");
		System.out.println(")");
		
		// Print records
		for (List<String> record : records) {
			System.out.print("| ");
			for (String value : record)
				System.out.print(value + " | ");
			System.out.println();
		}
	}
	
	protected List<UniqueColumnCombination> generateResults() {
		List<UniqueColumnCombination> results = new ArrayList<>();
		List<ColumnCombination> columnCombinations = null;
		while(true){
			columnCombinations = getNextColumnCombination(columnCombinations);
			if(columnCombinations == null || columnCombinations.size() == 0){
				break;
			}
			for(ColumnCombination columnComb : columnCombinations){
				if(checkUniquinessFor(columnComb)){
					System.out.println("Column is unique");
					UniqueColumnCombination ucc = new UniqueColumnCombination(columnComb);
					results.add(ucc);
				} else{
					System.out.println("Column is not unique");
				}
			}
			
		}
		return results;
		
	}
	
	private boolean checkUniquinessFor(ColumnCombination colComb){
		Set<ColumnIdentifier> identifiers = colComb.getColumnIdentifiers();
		Map<String, Integer> resultMap = new HashMap<String, Integer>();
		for(List<String> record : this.records){
			String key = "";
			for(ColumnIdentifier identifier : identifiers){
				key += record.get(this.columnNames.indexOf(identifier.getColumnIdentifier()));
			}
			if(resultMap.containsKey(key)){
				return false;
			}
			resultMap.put(key, 1);
		}
		return true;
	}
	
	private List<ColumnCombination> getNextColumnCombination(List<ColumnCombination> previousCombinations){
		List<ColumnCombination> combinations = new ArrayList<ColumnCombination>();
		if(previousCombinations == null){
			for(String columnName : this.columnNames){
				combinations.add(new ColumnCombination(new ColumnIdentifier(this.relationName, columnName)));
			}
		} else{
			
		}
		
		if(depth==1){
			for(int i=0; i<this.columnNames.size();i++){
				for(int j=i+1; j<this.columnNames.size();j++){
					combinations.add(new ColumnCombination(new ColumnIdentifier(this.relationName, this.columnNames.get(i)), new ColumnIdentifier(this.relationName, this.columnNames.get(j))));
				}
			}
		}
		if(depth>2){
			for(int i=0; i<previousCombinations.size(); i++){
				for(int j=i; j<previousCombinations.size()-1; j++){
					if(samePrefix(previousCombinations.get(i), previousCombinations.get(j))){
						Set<ColumnIdentifier> identifiers = previousCombinations.get(i).getColumnIdentifiers();
						identifiers.addAll(previousCombinations.get(j).getColumnIdentifiers());
						ColumnCombination comb = new ColumnCombination();
						comb.setColumnIdentifiers(identifiers);
						combinations.add(comb);
					}
					previousCombinations.get(i);
				}
			}
		}
		
		depth++;
		return combinations;
	}
	
	private boolean samePrefix(ColumnCombination comb1, ColumnCombination comb2){
		return comb1.compareTo(comb2) == 1;
	}
	
	protected void emit(List<UniqueColumnCombination> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (UniqueColumnCombination od : results)
			this.resultReceiver.receiveResult(od);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
