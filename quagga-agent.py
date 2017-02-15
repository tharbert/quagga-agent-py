
import json
import sys
import time
from subprocess import check_output
from kafka import SimpleProducer, KafkaClient

KAFKA_SERVER = 'x.x.x.x:y'

def publish_to_kafka(topic, message):
    kafka = KafkaClient(KAFKA_SERVER)
    producer = SimpleProducer(kafka)
    producer.send_messages(topic, message)

def main():
    # create empty set for recording down nodes
    down_peer_set = set()
    while True:
        json_data = check_output(['cl-bgp', 'summary', 'show', 'json'])
        loaded = json.loads(json_data)
        for peer in loaded['peers']:
            remote_as = loaded['peers'][peer]['remoteAs']
            state = loaded['peers'][peer]['state']
            # check for peer using BGP unnumbered and collect hostname
            try:
                hostname = loaded['peers'][peer]['hostname']
                peer_summary = '%s(%s):AS%s' % (peer, hostname, remote_as)
            except KeyError:
                peer_summary = '%s:AS%s' % (peer, remote_as)
            # check if peer state is down and not recorded
            if (state != 'Established') and (peer_summary not in down_peer_set):
                down_peer_set.add(peer_summary)
                message = 'BGP neighbor down: ' +peer_summary
                publish_to_kafka('testing', bytes(message))
            # check if peer state is up and recorded as down
            elif (state == 'Established') and (peer_summary in down_peer_set):
                down_peer_set.remove(peer_summary)
                message = 'BGP neighbor recovered: ' +peer_summary
                publish_to_kafka('testing', bytes(message))
            else:
                continue

        time.sleep(60)


if __name__ == '__main__':
    main()
