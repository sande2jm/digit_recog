import os
def get_parent():
	current_file = os.path.abspath(os.path.dirname(__file__))
	parent = os.path.join(current_file)
	parent = parent.split("/")
	return parent[-1]