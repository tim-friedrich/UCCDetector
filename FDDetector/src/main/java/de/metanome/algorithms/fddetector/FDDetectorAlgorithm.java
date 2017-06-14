package de.metanome.algorithms.fddetector;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;

public class FDDetectorAlgorithm {
	
	protected RelationalInputGenerator inputGenerator = null;
	protected FunctionalDependencyResultReceiver resultReceiver = null;
	ColumnCombinationBitset superBitset;
	protected String relationName;
	protected List<String> columnNames;
	protected List<List<String>> records;
	List<FunctionalDependency> results;
	Map<String, ArrayList> pruns = new HashMap<String, ArrayList>();
	protected String someStringParameter;
	protected Integer someIntegerParameter;
	private Integer depth = 0;
	protected Boolean someBooleanParameter;
	
	public void execute() throws AlgorithmExecutionException {
		
		this.initialize();

		this.records = this.readInput();

		this.print(records);

		this.generateResults();
		
		this.emit(results);
		
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
	
	// generates results
	protected List<FunctionalDependency> generateResults() {
		this.results = new ArrayList<>();
		this.pruns = new HashMap<>();
		
		List<FunctionalDependency> functionalDependencies = null;
		Boolean bottomUp = true;
		Integer bottomDepth = 0;
		Integer topDepth = this.columnNames.size();
		superBitset = new ColumnCombinationBitset();
		ArrayList<Integer> depths = new ArrayList<Integer>();
		for(int i=0; i<this.columnNames.size(); i++){
			superBitset.addColumn(i);
			depths.add(i);
		}
		
		System.out.println(superBitset.getNSubsetColumnCombinations(2));
		while(true){
			int idx = new Random().nextInt(depths.size());
			depth = depths.get(idx);
			depths.remove(idx);
			functionalDependencies = getPossibleFDsFor(depth);
			System.out.println("processing depth: "+depth+" With "+functionalDependencies.size()+" Elements");
			
			//getNextColumnCombination(depth);
			if(depths.size() <= 0){
				break;
			}
			for(FunctionalDependency functionalDependency : functionalDependencies){
				Boolean isDependant = checkDependencyFor(functionalDependency);
				
				//this will not work on all datasets
				if(isDependant){
					//System.out.println(results);
					this.results.add(functionalDependency);
				} else if(!isDependant){
					this.pruns.put(functionalDependency.getDependant().toString(), functionalDependency.getDeterminant())
				}
			}
			//System.out.println(bottomUp);
			//System.out.println(results);
			bottomUp = !bottomUp;
			
		}
		cleanUpResults();
		System.out.println("Number Results: "+ results.size());
		return results;
		
	}
	
	private void cleanUpResults(){
		List<FunctionalDependency> removables = new ArrayList<FunctionalDependency>();
		for(int i=0; i<this.results.size();i++){
			FunctionalDependency fd1 = this.results.get(i);
			for(int y=0; y<this.results.size();y++){
				FunctionalDependency fd2 = this.results.get(y);
				if(fd1.getDependant().equals(fd2.getDependant()) && 
						fd1.getDeterminant().getColumnIdentifiers().size() < fd2.getDeterminant().getColumnIdentifiers().size() &&
						fd2.getDeterminant().getColumnIdentifiers().containsAll(fd1.getDeterminant().getColumnIdentifiers())){
					removables.add(fd2);
				
				}
			}
		}
	
		this.results.removeAll(removables);
	}
	
	private boolean checkDependencyFor(FunctionalDependency functionalDep){
		
		Set<ColumnIdentifier> DetIdentifiers = functionalDep.getDeterminant().getColumnIdentifiers();
		Map<String, String> resultMap = new HashMap<String, String>();
		for(List<String> record : this.records){
			String key = "";
			for(ColumnIdentifier identifier : DetIdentifiers){
				String value = record.get(this.columnNames.indexOf(identifier.getColumnIdentifier()));
				if(value == null){
					value = "null";
				}
				key += value;
			}
			
			String dependantVal = record.get(this.columnNames.indexOf(functionalDep.getDependant().getColumnIdentifier()));
			if(dependantVal == null){
				dependantVal = "null";
			}
			if(resultMap.containsKey(key) && !resultMap.get(key).equals(dependantVal)){
				return false;
			}
			resultMap.put(key, dependantVal);
		}
		return true;
	}

	private List<FunctionalDependency> getPossibleFDsFor(Integer dep){
		List<FunctionalDependency> combinations = new ArrayList<FunctionalDependency>();
		List<ColumnCombinationBitset> bitCombs = superBitset.getNSubsetColumnCombinations(depth);
		System.out.println(bitCombs.size());
		for(ColumnCombinationBitset bitComb : bitCombs){
			ColumnCombination comb = bitComb.createColumnCombination(this.relationName, this.columnNames);
			for(int y=0; y<this.columnNames.size(); y++){
				ColumnIdentifier identifier = new ColumnIdentifier(this.relationName, this.columnNames.get(y));
				if(!comb.getColumnIdentifiers().contains(identifier)){
					FunctionalDependency fd = new FunctionalDependency(
							comb, 
							identifier);
				
					if(notProne(fd)){
						combinations.add(fd);
					}
				}
			}
		}
		return combinations;
	}
	
	
	// prones single Column Combinations. proning is done just in place to increase the performance
	// returns false if a found ucc is a subset of the given column combination
	private boolean notProne(FunctionalDependency fd){
		if(fd.getDeterminant().getColumnIdentifiers().contains(fd.getDependant()))
			return false;
		
		for(FunctionalDependency resultFd : this.results){
			// to be improved
			if(fd.getDeterminant().getColumnIdentifiers().containsAll(resultFd.getDeterminant().getColumnIdentifiers()) &&
					fd.getDependant().equals(resultFd.getDependant()))
				return false;
		}
		for(FunctionalDependency pronedFd : this.pruns){
			// to be improved
			if(fd.getDependant().equals(pronedFd.getDependant()) && 
					pronedFd.getDeterminant().getColumnIdentifiers().containsAll(fd.getDeterminant().getColumnIdentifiers()))
				return false;
		}
		return true;
	}
	
	// checks if the columns have the same prefix
	private boolean samePrefix(ColumnCombination comb1, ColumnCombination comb2, Integer dep){
		Iterator<ColumnIdentifier> comb1Iterator = comb1.getColumnIdentifiers().iterator();
		Iterator<ColumnIdentifier> comb2Iterator = comb2.getColumnIdentifiers().iterator();
		int i = 0;
		
		boolean result = true;
		// check for same prefix until depth size
		while(comb1Iterator.hasNext() && comb2Iterator.hasNext() && i < dep-1){
			ColumnIdentifier identifier1 = comb1Iterator.next();
			ColumnIdentifier identifier2 = comb2Iterator.next();

			if(!identifier1.equals(identifier2)){
				result = false;
			}
			i++;
		}
		return result;
	}
	
	protected void emit(List<FunctionalDependency> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (FunctionalDependency od : results)
			this.resultReceiver.receiveResult(od);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
