package de.uni_potsdam.hpi.metanome_test_runner;

import de.uni_potsdam.hpi.metanome_test_runner.config.Config;
import de.uni_potsdam.hpi.metanome_test_runner.mocks.MetanomeMock;

public class Main {

	public static void main(String[] args) {
		Config conf = Config.create(args);
		String[] arg = new String[2];
		arg[0] = "INDDETECTOR";
		arg[1] = "ASTRONOMICAL";
		Config conf2 = Config.create(arg);
		MetanomeMock.execute(conf, conf2);
	}

}
