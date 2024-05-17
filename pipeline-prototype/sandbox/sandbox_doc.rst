Erste Versuche mit Kafka
========================

Das Ziel des ersten Versuchs war die Erstellung zweier möglichst simpler
Dateien: ``consumer.py`` und ``producer.py``. Diese sollten die Grundlagen
von Apache Kafka einführen.

Vorgehen
--------
--------

1. **Hinzufügen der Dependencies**

``requirements.txt`` um ``confluent-kafka`` erweitert

2. **Verwendung eines Docker-Containers**

Die Datei ``docker-compose.yml`` konfiguriert die Verwendung des
Kafka-Containers und des Zookeeper-Containers. Alternativ könnten Kafka
und Zookeeper auch direkt auf dem System installiert und gestartet werden.

Start der Container::

    docker compose up -d

Überprüfung durch::

    docker ps

Hinzufügen des Topics, das in ``consumer.py`` und ``producer.py``
verwendet wird (``my_topic``) im Kafka-Container::

    docker exec c9e0d1a873d6 ../../bin/kafka-topics --create --topic my_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

Überprüfung durch::

    docker exec c9e0d1a873d6 ../../bin/kafka-topics --list --bootstrap-server localhost:9092

3. **Ausführen des Consumer-Codes**

Der Consumer "abonniert" das erstellte Topic und empfängt die Nachrichten::

    python .\consumer.py

4. **Ausführen des Producer-Codes**

Der Producer erstellt und sendet 10 Nachrichten in ``my_topic``::

    python .\producer.py

