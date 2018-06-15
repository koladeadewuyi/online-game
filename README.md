# Game results analytics

A simple Spark project that computes the team(s) and player(s) with the highest score from a week's worth of game results

## Assumptions made
- team and scores files will only contain data for one week 

## Getting Started

To start up a local instance of the service use:
```bash
mvn exec:java -Dexec.mainClass="com.db.exercise.App" \
-Dexec.args='<eteamFilePath> <scoreFilePath> <rddOutputPath> <dfOutputPath>'
```

Upon running the app with suitable args, the results should then be found in <rddOutputPath> and <dfOutputPath>

### Prerequisites 

* [Java 8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Scala 2.11.8](https://www.scala-lang.org/download/)
* [Maven 3.5.3](https://maven.apache.org/install.html)


## Running the tests

```bash
mvn clean test
```

### Coding style

Coding style adopted from: 
* [Scala docs style guide](https://docs.scala-lang.org/style/)
* [Originate scala guide](https://www.originate.com/library/scala-guide-best-practices)


## Versioning

For simplicity reasons Semantic versioning was not used for this submission 
