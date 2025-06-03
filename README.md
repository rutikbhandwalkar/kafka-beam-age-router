
# Apache Beam Kafka Age Filter

This project is a streaming data pipeline built using **Apache Beam**, **Kafka**, and **Apache Flink**. It reads JSON messages from a Kafka topic, calculates the age of individuals based on their date of birth, and routes the data to different topics based on whether the age is even or odd.

---

## 🛠️ Tech Stack

- Java 17
- Apache Beam
- Apache Flink (Runner)
- Apache Kafka
- Docker
- Maven

---

## 📦 Project Structure

```
├── src
│   └── main
│       └── java
│           └── com.example.agefilter
│               ├── model
│               │   └── Person.java
│               └── service
│                   └── BeamPipeline.java
│                   └── KafkaService.java
│               ├── util
│                   └── PersonsUtils.java
│               ├── AgeRouterApplicaton.java
├── test
│       └── java
│           └── com.example.agefilter
│               ├── BeamPipelineTest.java
├── pom.xml
└── README.md
```

---

## 🚀 Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/rutikbhandwalkar/kafka-beam-age-router.git
cd kafka-beam-age-router
```

### 2️⃣ Start Kafka and Zookeeper (via Docker)

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
   zookeeper:
      image: confluentinc/cp-zookeeper:7.4.0
      environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000
      ports:
         - "2181:2181"

   kafka:
      image: confluentinc/cp-kafka:7.4.0
      depends_on:
         - zookeeper
      ports:
         - "9092:9092"
      environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
         KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

```

Start the services:

```bash
docker-compose up -d
```

---

### 3️⃣ Start Apache Flink (via Docker)

```bash
docker run -d -p 8081:8081 apache/flink:1.20.1
```


```bash
docker-compose -f docker-compose-flink.yml up -d
```

Check Flink UI at: http://localhost:8081

---

## 📄 Define Kafka Topics

Open Kafka container bash:

```bash
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8.1
```

Create topics:

```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic SOURCE_TOPIC
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EVEN_TOPIC
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ODD_TOPIC
```

List topics:

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

---

## ⚙️ Build and Run the Pipeline

### 1. Build the JAR

```bash
mvn clean package
```

### 2. Run the pipeline

```bash
java -jar target/kafka-beam-age-router-1.0.jar
```

---

## 🧪 Test with Producer & Consumer

### Open Kafka container shell

```bash
docker exec -it docker-kafka-1 bash
```

### Produce test data

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic SOURCE_TOPIC
>{"name":"Ajay","dob":"2000-06-01"}
>{"name":"Rutik","dob":"1999-01-02"}
```

### Consume from EVEN_TOPIC

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic EVEN_TOPIC --from-beginning
```

### Consume from ODD_TOPIC

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic ODD_TOPIC --from-beginning
```

---

## 🧠 How It Works

1. JSON messages like `{"name":"Ajay", "dob":"2000-06-01"}` are produced to `SOURCE_TOPIC`.
2. The Beam pipeline:
    - Deserializes JSON into `Person` objects.
    - Calculates age from DOB.
    - Filters even and odd aged persons.
    - Writes them to `EVEN_TOPIC` or `ODD_TOPIC`.

---

## ✅ Sample Output

If you produce:

```json
{"name":"Ajay","dob":"2000-06-01"}
{"name":"Rutik","dob":"1999-01-02"}
```

Then:
- Charlie (25) goes to `ODD_TOPIC`
- Daisy (26) goes to `EVEN_TOPIC`

---

## 💡 Notes

- Ensure Kafka container is healthy (`docker ps` to check).
- Make sure `dob` is in ISO format (`yyyy-MM-dd`).

---

## 🧹 Clean Up

```bash
docker-compose down
```