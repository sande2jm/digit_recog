from subprocess import call
from subprocess import check_output
import time
import boto3
import json
from worker import Worker
import sys

sqs = boto3.resource('sqs',region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='swarm.fifo')
my_id = check_output(['curl', 'http://169.254.169.254/latest/meta-data/instance-id'])
my_id = "".join(map(chr, my_id))

d = {
'message': 'launched',
'id': my_id,
'progress': 'None'}

response = queue.send_message(MessageBody=json.dumps(d), MessageGroupId='bots')

w = Worker()
w.extract()
w.run()
w.dump()

