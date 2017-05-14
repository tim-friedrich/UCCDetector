package de.uni_potsdam.hpi.metanome_test_runner;

import de.uni_potsdam.hpi.metanome_test_runner.config.Config;
import de.uni_potsdam.hpi.metanome_test_runner.mocks.MetanomeMock;

public class Main {

	public static void main(String[] args) {
		Config conf = Config.create(args);
		MetanomeMock.execute(conf);
	}

}
