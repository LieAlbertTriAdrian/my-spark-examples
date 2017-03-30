1. Start Kafka
2. sbt assembly instead of sbt package (because includes external dependency -> kafka utils)
3. ./run.sh with jar specified in the step 1