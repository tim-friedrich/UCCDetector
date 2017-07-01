package de.metanome.algorithms.inddetector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.OpenBitSet;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

public class INDDetectorAlgorithm {
	
	protected RelationalInputGenerator[] inputGenerator = null;
	protected InclusionDependencyResultReceiver resultReceiver = null;
	
	protected List<String> relationNames;
	protected List<String> columnNames;
	protected List<List<String>> relationColumnNames;
	protected List<String> columnRelationMap;
	protected List<List<List<String>>> records;
	protected List<OpenBitSet> recordBitMaps;
	List<InclusionDependency> results;
	protected String someStringParameter;
	protected Integer someIntegerParameter;
	protected Boolean someBooleanParameter;
	private Integer hashCounter = 0;
	private Map<String, Integer> perfektHashMap = new HashMap<String, Integer>();
	
	public void execute() throws AlgorithmExecutionException {
		
		this.initialize();
		
		this.records = this.readInput();
		this.recordBitMaps = new ArrayList<OpenBitSet>();
		generateBitmaps();
		this.print(records);

		this.generateResults();
		
		this.emit(results);
		
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		this.relationNames = new ArrayList<String>();
		this.columnRelationMap = new ArrayList<String>(); 
		this.columnNames = new ArrayList<String>();
		this.relationColumnNames = new ArrayList<List<String>>();
		for(int i=0; i<this.inputGenerator.length; i++){
			RelationalInput input = this.inputGenerator[i].generateNewCopy();
			this.relationColumnNames.add(new ArrayList<String>());
			this.relationNames.add(input.relationName());
			for(String cName : input.columnNames()){
				this.relationColumnNames.get(i).add(cName);
				this.columnNames.add(cName);
				this.columnRelationMap.add(input.relationName());
			}
			
			
		}
		
	}
	
	protected List<List<List<String>>> readInput() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
		List<List<List<String>>> records = new ArrayList<>();
		for(int i=0; i<this.inputGenerator.length; i++){
			RelationalInput input = this.inputGenerator[i].generateNewCopy();
			records.add(new ArrayList<List<String>>());
			while (input.hasNext())
				records.get(i).add(input.next());
		}
		return records;
	}
	
	protected void print(List<List<List<String>>> records) {
		
		// Print schema
		//System.out.print(this.relationName + "( ");
		for (String columnName : this.columnNames)
			System.out.print(columnName + " ");
		System.out.println(")");
		System.out.println(relationNames);
		
		// Print records
//		for (List<String> record : records) {
//			System.out.print("| ");
//			for (String value : record)
//				System.out.print(value + " | ");
//			System.out.println();
//		}
	}
	
	// generates results
	protected List<InclusionDependency> generateResults() {
		this.results = new ArrayList<>();
		
		for(int i=0; i<this.columnNames.size(); i++){
			for(int y=i+1; y<this.columnNames.size(); y++){
				checkInclusionFor(i,y);
			}
		}
		
		return results;	
	}
	
	private void addToResult(int rowIndex1, int rowIndex2){
		ColumnIdentifier dependantIdentifier = new ColumnIdentifier(this.columnRelationMap.get(rowIndex1), this.columnNames.get(rowIndex1));
		ColumnIdentifier referencedIdentifier = new ColumnIdentifier(this.columnRelationMap.get(rowIndex2), this.columnNames.get(rowIndex2));
		ColumnPermutation dependant = new ColumnPermutation(dependantIdentifier);
		ColumnPermutation referenced = new ColumnPermutation(referencedIdentifier);
		InclusionDependency ind = new InclusionDependency(dependant, referenced);
		System.out.println(ind);
		this.results.add(ind);
	}
	
	private void checkInclusionFor(int rowIndex1, int rowIndex2){
		if(checkSubsetFor(rowIndex1, rowIndex2)){
			addToResult(rowIndex1, rowIndex2);
		}
		if(checkSubsetFor(rowIndex2, rowIndex1)){
			addToResult(rowIndex2, rowIndex1);
		}
	}
	
	private boolean checkSubsetFor(int rowIndex1, int rowIndex2){
		OpenBitSet bitSet1 = this.recordBitMaps.get(rowIndex1).clone();
		OpenBitSet bitSet2 = this.recordBitMaps.get(rowIndex2);
		bitSet1.and(bitSet2);
		bitSet1.xor(bitSet2);
		return bitSet1.isEmpty();
	}
	
	private void generateBitmaps(){
		List<List<OpenBitSet>> tmpMap = new ArrayList<List<OpenBitSet>>();
		for(int relationIndex=0; relationIndex<this.relationNames.size(); relationIndex++){
			tmpMap.add(new ArrayList<OpenBitSet>());
			for(int columnIndex=0; columnIndex<this.relationColumnNames.get(relationIndex).size(); columnIndex++){
				tmpMap.get(relationIndex).add(new OpenBitSet());
			}
			List<List<String>> relation = records.get(relationIndex);
			for(int rowIndex=0; rowIndex<relation.size(); rowIndex++){
				List<String> row = relation.get(rowIndex);
				for(int columnIndex=0; columnIndex<row.size(); columnIndex++){
					OpenBitSet bitmap = tmpMap.get(relationIndex).get(columnIndex);
					String value = row.get(columnIndex);
					if(value != null && !value.isEmpty())
						bitmap.set(perfektHash(value));
				}	
			}
		}
		for(List<OpenBitSet> relation :tmpMap){
			for(OpenBitSet column : relation){
				this.recordBitMaps.add(column);
			}
		}
		
	}
	
	private Integer perfektHash(String value){
		
		if(perfektHashMap.containsKey(value)){
			return this.perfektHashMap.get(value);
		}
		int result = this.hashCounter;
		this.perfektHashMap.put(value, this.hashCounter);
		this.hashCounter++;
		return result;
	}
	
	
	protected void emit(List<InclusionDependency> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (InclusionDependency ind : results)
			this.resultReceiver.receiveResult(ind);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
