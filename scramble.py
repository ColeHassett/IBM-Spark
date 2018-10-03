from pyspark import SparkContext
from pyspark.sql import SparkSession
import re
import numpy as np
import sys

sc = SparkContext("local", "app")
spark = SparkSession \
	.builder \
	.appName("app") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

dict_file = "file:///Users/Cole/Documents/Fun/IBM/Spark/freq_dict.json"
data_file = "file:///Users/Cole/Documents/Fun/IBM/Spark/data.json"
test_file = "file:///Users/Cole/Documents/Fun/IBM/Spark/test.json"

dictionary = sc.textFile(dict_file).cache()

def main():
	data_df = spark.read.json(data_file)
	scrambles_list = np.array(data_df.select("scrambles").collect())
	results_list = np.array(data_df.select("results").collect())
	final_result = []

	for index,puzzle in enumerate(scrambles_list):
		words_arr = getWords(puzzle)
		end_phrase = getPhrase(words_arr, results_list[index])
		final_result.append(end_phrase)

	file = open('output.txt', 'r+')
	file.truncate(0)
	for i in final_result:
		file.write(str(i)+"\n")
		print i

def getWords(scrambles):
	result_arr = []
	scrambles_arr = str(scrambles)[3:-2].split("|")

	for scramble in scrambles_arr:
		temp_arr = scramble.split(":")
		temp_word = re.sub("[^a-zA-Z]+","", temp_arr[0])
		temp_circles = temp_arr[1]
		possible_words = unscramble(temp_word)
		result_arr.append(possible_words + "|" + temp_circles)

	return result_arr

def unscramble(letters):
	dict_f = dictionary.filter(lambda s: (len(letters) == len(re.sub("[^a-zA-Z]+","", s))) and sorted(letters) == sorted(re.sub("[^a-zA-Z]+","", s)))
	zero_test = dict_f.filter(lambda s: re.sub("[^0-9]+","", s) != "0").collect()
	if zero_test != []:
		dict_f = zero_test
	else:
		dict_f = dict_f.collect()

	frequency = "9888"
	result = ""
	for i in dict_f:
		temp_word = re.sub("[^a-zA-Z]+","", i)
		temp_freq = re.sub("[^0-9]+","", i)
		if temp_freq < frequency:
			frequency = temp_freq
			result = temp_word

	return result

def getPhrase(words, word_lengths):
	words_str = ""
	for word in words:
		temp_arr = word.split("|")
		temp_words = list(temp_arr[0])
		temp_circles = list(re.sub("[^0-9]+","", temp_arr[1]))
		for index in temp_circles:
			words_str += temp_words[int(index)-1]

	length_str = ""
	dict_arr = []
	for val in word_lengths:
		length_str += str(val)+","
		temp_arr = val.split(",")
		for num in temp_arr:
			dict_arr.append(dictionary.filter(lambda s: len(re.sub("[^a-zA-Z]+","", s)) == int(num) and all(e in words_str.split() for e in re.sub("[^a-zA-Z]+","", s).split())).collect())
		# for i in dict_arr:
		# 	print i
		# 	sys.exit()

	length_str = length_str[:-1]

	# add frequencies together lowest total = most likely

	return str(words_str) + "|" + str(length_str)

main()
