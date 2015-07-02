import os
from azure.servicebus import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME
from firebase import firebase
import json
import uuid

ns_key = os.environ.get('NS_KEY')
firebase = firebase.FirebaseApplication('https://logbasedev.firebaseio.com/', None)

bus_service = ServiceBusService(
    service_namespace='onroad-ns',
    shared_access_key_name='RootManageSharedAccessKey',
    shared_access_key_value=ns_key)

while True:
    msg = bus_service.receive_subscription_message('onroad-alerts', 'alerts-consumer', peek_lock=False)
    if msg.body:
        alert = json.loads(msg.body)
        print('running... ')
        #TODO: Improve, take the last item only  
        for event in alert['location']:
            alert_location = {'latitude':event['lat'], 'longitude':event['long'], 'locationtime':event['time']};
        #TODO: Remove device number hard coding
        new_alert = {'alertid': str(uuid.uuid1()), 'alerttype': alert['alert'], 'devicenumber': '8650670123456', 'latitude': alert_location['latitude'], 'longitude': alert_location['longitude'], 'status':'Open', 'time': alert_location['locationtime']}
        print new_alert
        firebase.post('/account/simplelogin:2/alerts', new_alert)