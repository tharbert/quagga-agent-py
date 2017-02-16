#!/usr/bin/python

import json
import socket
import sys
import time
from subprocess import check_output
from kafka import SimpleProducer, KafkaClient


KAFKA_SERVER = 'x.x.x.x:y'
KAFKA_TOPIC = 'quagga-bgp'
INTERVAL = 60
LOCAL_HOSTNAME = socket.gethostname()
VERSION = 20170216


def publish_to_kafka(message):
    kafka = KafkaClient(KAFKA_SERVER)
    producer = SimpleProducer(kafka)
    producer.send_messages(KAFKA_TOPIC, message)


def check_bgp(down_peer_set, alert):
    json_data = check_output(['cl-bgp', 'summary', 'show', 'json'])
    loaded = json.loads(json_data)
    for peer in loaded['peers']:
        remote_as = loaded['peers'][peer]['remoteAs']
        state = loaded['peers'][peer]['state']
        # check for peer using BGP unnumbered and collect hostname
        try:
            hostname = loaded['peers'][peer]['hostname']
            peer_summary = '%s(%s) AS%s' % (peer, hostname, remote_as)
        except KeyError:
            peer_summary = '%s AS%s' % (peer, remote_as)
        # check if peer state is down and not recorded
        if (state != 'Established') and (peer_summary not in down_peer_set):
            down_peer_set.add(peer_summary)
            message = 'BGP neighbor down on %s: %s' % (LOCAL_HOSTNAME, peer_summary)
            if alert == True: publish_to_kafka(bytes(message))
        # check if peer state is up and recorded as down
        elif (state == 'Established') and (peer_summary in down_peer_set):
            down_peer_set.remove(peer_summary)
            message = 'BGP neighbor recovered on %s: %s' % (LOCAL_HOSTNAME, peer_summary)
            if alert == True: publish_to_kafka(bytes(message))
        else:
            continue
    return down_peer_set


def main():
    # create empty set for recording down nodes
    down_peer_set = set()
    # populate set without alerting
    down_peer_set = check_bgp(down_peer_set, False)
    # begin regular process with regular checking of state and alerting
    while True:
        down_peer_set = check_bgp(down_peer_set, True)
        time.sleep(INTERVAL)


if __name__ == '__main__':
    main()
