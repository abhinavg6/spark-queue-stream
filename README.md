spark-queue-stream
==================

A simple spark streaming program to be run on local IDE (Eclipse/IntelliJ). It reads from a input file containing meetup events, maps those to a set of technology categories, and prints the counts per event category on console (every 1 sec as per streaming batch window). To know how the input data was generated, please see the [Meetup client](https://github.com/abhinavg6/Simple-Meetup-Client).

Please download latest version of maven to run mvn commands from command-line, or import it as a maven project in your IDE (provided maven plug-in is present). Please run "mvn clean install" and "mvn eclipse:eclipse" if you're running from a command line, and then import the project in your IDE.

Once the project is setup in IDE, you may run the class MeetupEventStream as a Java application, and sparm should do its magic locally.
