Introduction:
This is a getting-started package for Metanome algorithm developer. It contains two skeleton projects "MyOdDetector" and 
"MetanomeTestRunner". MyOdDetector is the actual profiling algorithm. The jar-file that you can build from this algorithm 
can be imported into a running instance of the Metanome tool. Because the graphical import is impractical during development, 
we use the second project, which is the MetanomeTestRunner, to run the algorithm with a set of predefined parameters during 
development. The MetanomeTestRunner mocks the Metanome backend and algorithmically performs the configuration, which is 
usually performed by the user. The essential difference between the algorithm and test runner project is that the algorithm 
only depends on the Metanome interface whereas the test runner depends on the entire Metanome backend.

Requirements:
- Java JDK 1.7 (or higher)
- Apache Maven

How two use:
1. Import both projects as maven projects into your IDE.
2. Run the MetanomeTestRunner to see if everything works.
3. Rename "MyOdDetector" into your own algorithms name. Your algorithm should have a unique name!
   3.1 Rename the project.
   3.2 Rename the project's classes.
   3.3 Change the names in the pom.xml files of both projects accordingly.
       Do not forget to rename the Algorithm-Bootstrap-Class!!!
   3.4 Change the names in the MetanomeTestRunner classes.
4. Run the TestRunner again to see if the renaming was correct.
5. Start implementing and testing your algorithm.
   5.1 Change the interface implementation according to what you want the algorithm to do.
       The skeleton is set up to discover Order Dependencies; you might want to discover something else.
   5.2 Implement the algorithm logic, i.e., the execute()-method.
   5.3 Adjust the configuration of your algorithm in the TestRunner's Config class (parameters, input dataset, ...).
   5.4 Whenever you change your algorithm's interface (e.g. parameters), change the TestRunner's MetanomeMock accordingly.
6. Do not forget to test your algorithm in Metanome.
   6.1 See the Metanome user guide in the GitHub Wiki on how to deploy your algorithm.
   6.2 You only need the algorithm project for the deployment; the TestRunner is only used to ease the development process!
