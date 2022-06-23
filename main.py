# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from ratings import mainScript
from friends_by_age import mainScript
from min_temperatures import mainScript
# from min_temperatures_solution import mainScript
from most_popular_movie_with_countByKey import mainScript
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    mainScript(sc)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
