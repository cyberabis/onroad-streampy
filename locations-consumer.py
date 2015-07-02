import os
from azure.servicebus import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME
from firebase import firebase
import json

ns_key = os.environ.get('NS_KEY')
firebase = firebase.FirebaseApplication('https://logbasedev.firebaseio.com/', None)

bus_service = ServiceBusService(
    service_namespace='onroad-ns',
    shared_access_key_name='RootManageSharedAccessKey',
    shared_access_key_value=ns_key)

while True:
    msg = bus_service.receive_subscription_message('onroad-topic', 'locations', peek_lock=False)
    if msg.body:
	    for event in json.loads(msg.body):
	        new_location = {'latitude':event['lat'], 'longitude':event['long'], 'locationtime':event['time']};
	        #TODO: Remove device hardcoding
	        firebase.patch('/account/simplelogin:2/livecars/0', new_location)