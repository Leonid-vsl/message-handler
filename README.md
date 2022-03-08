# message-handler

Demo spring based application working with kafka and postgre sql

Main goal is to implement follow logic:

  1) Do filtering upstream messages
  2) Do fixed size batch message aggregation
  3) Do store batches in db when they fill up, or within time out


Tech: Spring, Spring Kafka, Spring JPA, Kafka, PostgreSQL, test containers, docker
