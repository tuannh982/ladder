Ladder
======

[![GitHub](https://img.shields.io/github/license/tuannh982/ladder.svg)](https://github.com/tuannh982/ladder/blob/master/LICENSE)
[![Total Alerts](https://img.shields.io/lgtm/alerts/g/tuannh982/ladder.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/tuannh982/ladder/alerts)
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/tuannh982/ladder.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/tuannh982/ladder/context:java)

## Introduction
Ladder is a lightning fast persistent queue written in Java.

## Usage
### Installation
// TODO publish to Maven Central
### Create persistent queue instance
```java
String path = "/path/to/your/queue/dir";
        Queue queue = new LadderQueue(
        new File(path),
        LadderQueueOptions.builder()
        .dataFlushThreshold(512 * 1024)
        .maxFileSize(100 * 1024)
        .build()
);
```
### Basic operations
#### put
```java
byte[] data = new byte[] {...};
queue.put(data);
```
#### take
```java
byte[] read = queue.take();
```
#### poll
```java
long timeoutInMs = 500;
byte[] read = queue.poll(timeoutInMs);
```