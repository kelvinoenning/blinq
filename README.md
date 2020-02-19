## Blinq

Blink is collection of Apache Beam examples with Kotlin.

### Apache Beam
Apache Beam is an advanced unified programming model that implement batch and streaming data processing jobs that run on any execution engine.

### How To run?

#### Dependencies
[Gradle](https://gradle.org/) helps teams build, automate and deliver better software, faster.
Also, you need a Java sdk 8 version installed in your system and then export java sdk 8 to you system:
  - `export JAVA_HOME=/usr/libexec/java_home -v 1.8`

#### WordCount example

1. run `gradle wordCount --args="--input=<INPUT_LOCATION> --output=<OUTPUT_LOCATION>`

#### PubSubToRow example

1. export you Google Cloud Credential
  - `export GOOGLE_APPLICATION_CREDENTIALS=<GCP_CREDENTIALS>`

2. run `gradle pubSubToRow --args="--project=<PROJECT_ID> --topic=<TOPIC> --runner=DataflowRunner --gcpTempLocation=<TMP_LOCATION>"`

## License
Copyright © 2020