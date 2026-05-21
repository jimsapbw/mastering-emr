# Step 03: Maven Setup

## Purpose

Install Maven so the EMR demo can build a Scala Spark application as a JAR.

The main BRBF workload is planned as one Scala project with multiple entry points:

- `FeatureLogConverter`
- `EligibleUserDataLogConverter`
- `BrbfJob`

## Environment

```text
OS: Amazon Linux 2023
Architecture: aarch64
```

## Install Command

```bash
sudo dnf install -y maven
```

## Verification Command

```bash
mvn -version
```

Observed:

```text
Apache Maven 3.8.4
Maven home: /usr/share/maven
Java version: 1.8.0_492
OS arch: aarch64
```

## Notes

- Maven was selected instead of sbt because it is simple to install on Amazon Linux and sufficient for building a Spark Scala JAR.
- The project should target Scala `2.12`, which matches Spark 3.5 on EMR 7.x.
- The Maven build should mark Spark dependencies as `provided` so the JAR uses the Spark libraries already available on EMR.
