# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
#
# Reads in metadata from json format and converts to a .txt file
# with product_id, title pairs.

import json
import re

with open('meta.json') as f:
	count = 0
	for line in f:
		line_dict = eval(line)
		pid = line_dict['asin']

		try:
			title = line_dict['title']
		except KeyError:
			count += 1
		else:
			print(pid + '|' + re.sub(r'\W+', '', title))
	