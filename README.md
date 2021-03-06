# DISCONTINUED JAVA LEGACY - Use Kotlin version @ [Nannoq-Tools](https://github.com/NoriginMedia/nannoq-tools)

# Nannoq Repository

[![Build Status](https://www.tomrom.net/buildStatus/icon?job=nannoq-tools/master)](https://www.tomrom.net/job/nannoq-tools/job/master/)

nannoq-repository is a collection of repository implementations for Vert.x. All repositories operate with a unified querying interface that abstracts away the underlying data store. Individual implementations will be extended for any data store specific functionality that is not reasonable to abstract away.

It supports:
 - Caching, extends to clustered caching with JCache if vertx is clustered.
 - Will refresh ETags if supplied an EtagManager implementation.
 
Operations:
 - Batch create/read/update/delete
 - Queries
   * Filtering
   * Ordering
   * Projections
   * Aggregations
 
Current implementations:
 
 - AWS DynamoDB

## Prerequisites

Vert.x >= 3.5.0

Java >= 1.8

Maven

## Installing

mvn clean package -Dgpg.skip=true

### Running the tests

mvn clean test -Dgpg.skip=true

### Running the integration tests

mvn clean verify -Dgpg.skip=true

## Usage

First install with either Maven:

```xml
<dependency>
    <groupId>com.nannoq</groupId>
    <artifactId>repository</artifactId>
    <version>1.0.5</version>
</dependency>
```

or Gradle:

```groovy
dependencies {
    compile group: 'nannoq.com:repository:1.0.5’
}
```

### Implementation and Querying

Please consult the [Wiki](https://github.com/NoriginMedia/nannoq-repository/wiki) for guides on implementations and queries on the repositories.

## Contributing

Please read [CONTRIBUTING.md](https://github.com/NoriginMedia/nannoq-repository/blob/master/CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/NoriginMedia/nannoq-repository/tags)

## Authors

* **Anders Mikkelsen** - *Initial work* - [Norigin Media](http://noriginmedia.com/)

See also the list of [contributors](https://github.com/NoriginMedia/nannoq-repository/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](https://github.com/NoriginMedia/nannoq-repository/blob/master/LICENSE) file for details
