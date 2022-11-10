FROM openjdk:8-jdk-alpine

ARG JAR_FILE="MMAv3.jar"
ARG CONFIG_FILE="config.ini"

RUN mkdir /mma

COPY ${JAR_FILE} /mma/mma.jar
COPY ${CONFIG_FILE} /mma/config.ini

ENV TZ Asia/Shanghai
WORKDIR /mma
CMD ["java","-jar","mma.jar", "-c", "config.ini"]