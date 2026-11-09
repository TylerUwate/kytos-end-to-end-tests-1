import os
import time
import json
import asyncio

import pytest
import requests
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from tests.helpers import NetworkTest

CONTROLLER = '127.0.0.1'
KYTOS_API = 'http://%s:8181/api/kytos' % CONTROLLER
KAFKA_TOPIC = "event_logs"
TIMEOUT = 1000
KAFKA_ADDRESSES = os.environ.get("KAFKA_HOST_ADDR").split(',')

class TestE2EKafkaEvents:
    net = None
    admin = None

    def setup_method(self, method):
        """
        It is called at the beginning of every class method execution
        """
        # Start the controller setting an environment in
        # which all elements are disabled in a clean setting
        self.net.config_all_links_up()
        self.net.start_controller(clean_config=True, enable_all=False)
        self.net.wait_switches_connect()
        time.sleep(5)

    @classmethod
    def setup_class(cls):
        cls.net = NetworkTest(CONTROLLER)
        cls.net.start(start_controller=False)

    @classmethod
    def teardown_class(cls):
        cls.net.stop()

    async def check_for_assignments(self, consumer, max_tries=4, sleep_interval=0.5):
        """Check the consumer for assignments."""
        tries = 1
        while not consumer.assignment():
            await asyncio.sleep(sleep_interval)
            if tries > max_tries:
                assert False, "Consumer does not get assignments."
            tries += 1

    async def test_01_napp_sends_data_correctly(self):
        """
        Test that kafka_events correctly runs the 'setup' method. This would require
        that the AIOKafkaProducer has been properly initialized
        """
        # Create a consumer before the message is propagated to reduce the amount of messages

        consumer = AIOKafkaConsumer(
            (KAFKA_TOPIC),
            bootstrap_servers=KAFKA_ADDRESSES,
        )

        await consumer.start()

        # Make sure consumer has assignments and waits for them
        await self.check_for_assignments(consumer)
        await consumer.seek_to_end()

        # Create an EVC-creation event to send to Kafka

        evc_name = "Vlan_%s" % 902

        payload = {
            "name": evc_name,
            "enabled": True,
            "dynamic_backup_path": True,
            "uni_a": {
                "interface_id": "00:00:00:00:00:00:00:01:1",
                "tag": {"tag_type": "vlan", "value": 902}
            },
            "uni_z": {
                "interface_id": "00:00:00:00:00:00:00:03:1",
                "tag": {"tag_type": "vlan", "value": 902}
            }
        }
        api_url = KYTOS_API + '/mef_eline/v2/evc/'
        response = requests.post(
            api_url,
            data=json.dumps(payload),
            headers={'Content-type': 'application/json'}
        )

        assert response.status_code == 201, response.text

        # Wait for the message to propagate

        await asyncio.sleep(4)

        # Try pulling from Kafka multiple times

        found = False

        for _ in range(5):

            if found:
                break

            try:
                # Wait up to 2 seconds for messages
                results = await consumer.getmany(timeout_ms=2000)

                # Ensure values exist
                assert results.values()

                for messages in results.values():
                    for msg in messages:
                        event = json.loads(msg.value.decode())
                        if event["event"] == 'kytos/mef_eline.created':
                            found = True
                            break

            except Exception as exc:
                print(f"An exception occurred: {exc}")

        assert found

        await consumer.stop()
    def test_02_napp_get_topics(self):
        """
        Test that the napp returns the correct topics and patterns
        """
        api_url = KYTOS_API + '/kafka_events/v1/topics'
        response = requests.get(
            api_url,
            headers={'Content-type': 'application/json'},
        )

        assert response.status_code == 200, response.text

        data = response.json()
        print("Data: ", data)

        assert "allowed" in data
        assert "blocked" in data

        # Check that the blocked patterns are correct
        blocked_patterns = data["blocked"]
        print("Blocked patterns: ", blocked_patterns)
        expected_blocked_patterns = [
            "kytos/of_core.v0x04.messages.*",
            "kytos/flow_manager.messages.out.*",
            "kytos/of_lldp.messages.out.*",
            "kytos/core.openflow.raw.*",
            "kytos/core.interface_tags",
            "kytos/of_lldp.interface.is.nni"
        ]

        for pattern in expected_blocked_patterns:
            assert pattern in blocked_patterns
            print(f"{pattern} is in blocked patterns.")
    
    
    
    
   
    def test_04_napp_delete_topic_pattern(self):
        """
        Tests that a pattern can be deleted. 
        Recommend using get_topics to see if a topic was there or wasn't. 
        """
        TOPICS_ENDPOINT = KYTOS_API + '/kafka_events/v1/topics'
        HEADERS = {'Content-type', 'application/json'}

        #pattern to delete, must be changed.
        pattern = 2

        #1. Remove the pattern (kafka_events is a pattern created)
        response = requests.delete(TOPICS_ENDPOINT, json={"pattern": pattern, })


    def test_03_napp_add_topic_pattern(self):
        """Test that a topic pattern can be created through the topics endpoint."""
        TOPICS_ENDPOINT = KYTOS_API + '/kafka_events/v1/topics'
        HEADERS = {'Content-type': 'application/json'}

        response = requests.post(
            TOPICS_ENDPOINT,
            json={
                "pattern": [0],
                "description": ["e2e test pattern"],
            },
            headers=HEADERS,
        )
        assert response.status_code == 201, response.text