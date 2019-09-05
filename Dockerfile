FROM openjdk:8u151-jdk-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD /target/apache-nutch-ldcbench-adapter-shaded.jar /usr/src/app

CMD ["java", "-cp", "apache-nutch-ldcbench-adapter-shaded.jar", "org.hobbit.core.run.ComponentStarter", "org.dice_research.anutch.adapter.system.SystemAdapter"]
