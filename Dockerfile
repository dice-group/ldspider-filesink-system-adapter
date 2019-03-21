FROM openjdk:8u151-jdk-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD /target/ldspider-ldcbench-adapter-shaded.jar /usr/src/app

CMD ["java", "-cp", "ldspider-ldcbench-adapter-shaded.jar", "org.hobbit.core.run.ComponentStarter", "org.dice_research.ldspider.adapter.system.SystemAdapter"]
