all: classpath
classpath: $(HOME)/.m2/repository/ai/shane/test/bigtable-shim/1.0-SNAPSHOT/bigtable-shim-1.0-SNAPSHOT.jar
	echo "$(HOME)/.m2/repository/ai/shane/test/bigtable-shim/1.0-SNAPSHOT/bigtable-shim-1.0-SNAPSHOT.jar:$(shell mvn dependency:build-classpath -Dmdep.outputFile=/dev/stderr  2>&1 >/dev/null)" > classpath
$(HOME)/.m2/repository/ai/shane/test/bigtable-shim/1.0-SNAPSHOT/bigtable-shim-1.0-SNAPSHOT.jar: src/main/java/ai/shane/bigtableshim/*.java
	mvn install
test: classpath
	CLASSPATH=$(shell cat classpath) python -m unittest python/bigtableexport_test.py
