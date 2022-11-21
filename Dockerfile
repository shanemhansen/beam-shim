FROM maven:latest as mvn
COPY . /app/
RUN test -d /classpath || mkdir /classpath/
RUN cd /app/ && mvn install && mvn dependency:build-classpath -Dmdep.outputFile=/dev/stderr  2>&1 >/dev/null \
    | tr ':' '\n' | xargs cp --target-directory=/classpath
