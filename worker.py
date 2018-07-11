#the point now is to create a worker that can use instructions.
import boto3
import json
from subprocess import call
from subprocess import check_output
from helper import *
from threading import Thread
import time
import decimal
import pandas as pd
import numpy as np

class Worker():

	def __init__(self):
		
		"""
		Connect to AWS s3 download full swarms parameters. Set the file this 
		will be reading from and the file this will be writing too. 
		"""
		self.direc = get_parent()
		self.params = {}
		self.s3 = boto3.resource('s3')
		self.my_id = check_output(['curl', 'http://169.254.169.254/latest/meta-data/instance-id'])
		self.my_id = "".join(map(chr, self.my_id))
		self.sqs = boto3.resource('sqs', region_name='us-east-1')
		self.state = ['waiting']
		self.queue = self.sqs.get_queue_by_name(QueueName='swarm.fifo')
		self.controller_listener = Thread(target=self.check_in, daemon=True)
		self.controller_listener.start()

		self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
		self.table = self.dynamodb.Table('test')


	def check_in(self):
		while True:
			with open(self.direc + '/state.txt', 'r') as f:
				self.state[0] = f.read()
				time.sleep(3)

	def extract(self):
		"""
		Use the file_in from init to extract this workers specific parameters
		from json dictionary based on ec2 instance ids
		"""	
		self.s3.Bucket('swarm-instructions').download_file('parameters.txt', 'parameters.txt')	
		with open('parameters.txt', 'r') as f:
			swarm_params = json.load(f)
		self.params = swarm_params[self.my_id]
		self.s3.Bucket('swarm-instructions').download_file(self.params['train'],self.params['train'])

		df = pd.read_csv(self.params['train'])
		df.head()

		self.y = df['label'].tolist()

		print(self.y[:10])

		imgs = df.loc[:,'pixel0':'pixel783']

		imgs.head()
		self.x = []
		for row in imgs.values.tolist():
		    self.x.append(np.reshape(np.asarray(row), (28,28,1)))

		self.x = np.asarray(self.x)
		self.y = np.asarray(self.y)
		#I am now trying to achieve this through keras with their system
		self.y = utils.to_categorical(self.y, num_classes=10)
		print(self.x.shape, type(self.x))
		print(self.y.shape, type(self.y))
		


	def run(self):
		"""
		Take the params from extract and run whatever operations you want
		on them. Set self.results in this method based on self.params
		"""
		batch_size = self.params['batch_size']
		learning_rate = self.params['learning_rate']
		epochs = self.params['epochs']
		epochs = self.params['epochs']
		dropout_rate = self.params['dropout_rate']
		x = self.params['train']
		y = self.params['test']

		size = 10000
		i = 0
		while self.state[0] == 'waiting':
			print("Waiting for GO") 
			time.sleep(.3)

		i = 0
		start = time.clock()
		while i < size and self.state[0] != 'exit':
			if i%100 == 0:
				self.report(i, size=size)
			while self.state[0] == 'pause':
				time.sleep(.3)
				self.report(i,size=size)
			i += 1
		end = time.clock()
		self.put_in_Dynamo()

	def put_in_Dynamo(self, *args):
		batch_size = self.params['batch_size']
		learning_rate = self.params['learning_rate']
		epochs = self.params['epochs']
		dropout_rate = self.params['dropout_rate']
		train = self.params['train']
		test = self.params['test']
		response = self.table.update_item(
		    Key={
		    'id': self.my_id
		    },
		    UpdateExpression='SET #a = :val1, #b = :val2, #c = :val3, #d = :val4, #e = :val5, #f = :val6',
		    ExpressionAttributeNames={
		        '#a': 'batch_size',
		        '#b': 'learning_rate',
		        '#c': 'epochs',
		        '#d': 'dropout_rate',
		        '#e': 'train',
		        '#f': 'test'

		    },
		    ExpressionAttributeValues={
		        ':val1': batch_size,
		        ':val2': decimal.Decimal(str(learning_rate)),
		        ':val3': epochs,
		        ':val4': decimal.Decimal(str(dropout_rate)),
		        ':val5': train,
		        ':val6': test
		    }
		)


	def report(self,i, size = 100):
		"""
		Post to swarm queue my progress and state
		"""
		d = {
		'message': 'working',
		'state': self.state[0],
		'id': self.my_id,
		'progress': round(i/size,4)}
		response = self.queue.send_message(MessageBody=json.dumps(d), MessageGroupId='bots')

	def dump(self):
		"""
		Use the file_out to write the results of this worker to s3.
		"""
		d = {
		'message': 'complete',
		'state': self.state[0],
		'id': self.my_id,
		'progress': 'None'}
		response = self.queue.send_message(MessageBody=json.dumps(d), MessageGroupId='bots')


		



