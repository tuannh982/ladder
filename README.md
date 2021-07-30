# Ladder

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