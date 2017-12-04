## Welcome to Nannoq Repository

nannoq-repository is a collection of repository implementations for Vert.x. All repositories operate with a unified querying interface that abstracts away the underlying data store. Individual implementations will be extended for any data store specific functionality that is not reasonable to abstract away.

### Prerequisites

Vert.x >= 3.5.0

Java >= 1.8

Maven

### Installing

mvn clean package -Dgpg.skip=true

## Running the tests

mvn clean test -Dgpg.skip=true

## Running the integration tests

mvn clean verify -Dgpg.skip=true
