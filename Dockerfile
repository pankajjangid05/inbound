# Build stage
FROM maven:3.6.0-jdk-11-slim AS build
ENV HOME=/home/app
RUN mkdir -p $HOME
WORKDIR $HOME
ADD pom.xml $HOME

# Arguments 
ARG username
ARG token

# Print arguments value
RUN echo $username
RUN echo $token

# copy settings file to home settings file
COPY /settings.xml $HOME/settings.xml 

# replace username & token in settings file
RUN sed -i "s/USERNAME/$username/g" $HOME/settings.xml
RUN sed -i "s/TOKEN/$token/g" $HOME/settings.xml
RUN cat $HOME/settings.xml

# Maven package build
RUN mvn -s $HOME/settings.xml dependency:go-offline

ADD /src $HOME/src
RUN mvn package -s $HOME/settings.xml -DskipTests=true

# Package stage
FROM openjdk:12-alpine
ENV HOME=/home/app
ENV export $(cat .env | xargs)
WORKDIR $HOME
COPY --from=build $HOME/target/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]



