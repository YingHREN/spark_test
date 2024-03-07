import re
import datetime

from pyspark.sql import Row

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

import sys
import os
# from test_helper import Test
from pyspark.context import SparkContext

logFile = '/Users/renyinghao/projects/spark_test/assignment-4-MincYu/apache.log'
sc = SparkContext('local', 'test')
def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print ('Number of invalid logline: %d' % failed_logs.count())
        for line in failed_logs.take(20):
            print ('Invalid logline: %s' % line)

    print ('Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*.*(\S*)" (\d{3}) (\S+)'

parsed_logs, access_logs, failed_logs = parseLogs()

# COMMAND ----------

# TEST Data cleaning (1c)
# Test.assertEquals(failed_logs.count(), 0, 'incorrect failed_logs.count()')
# Test.assertEquals(parsed_logs.count(), 43177 , 'incorrect parsed_logs.count()')
# Test.assertEquals(access_logs.count(), parsed_logs.count(), 'incorrect access_logs.count()')

# Calculate statistics based on the content size.
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print ('Content Size Avg: %i, Min: %i, Max: %s' % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()))


# Response Code to Count
responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .cache())
responseCodeToCountList = responseCodeToCount.take(100)
print ('Found %d response codes' % len(responseCodeToCountList))
print ('Response Code Counts: %s' % responseCodeToCountList)
# Test.assertEquals(len(responseCodeToCountList), 4, 'incorrect number of response code')
# Test.assertEquals(sorted(responseCodeToCountList), [ (200, 38606), (302, 1734), (304, 2631), (404, 206) ], 'incorrect count list')


labels = responseCodeToCount.map(lambda tpl: tpl[0]).collect()
print (labels)
count = access_logs.count()
fracs = responseCodeToCount.map(lambda tpl: (float(tpl[1]) / count)).collect()
print (fracs)

# COMMAND ----------

import matplotlib.pyplot as plt


def pie_pct_format(value):
    """ Determine the appropriate format string for the pie chart percentage label
    Args:
        value: value of the pie slice
    Returns:
        str: formated string label; if the slice is too small to fit, returns an empty string for label
    """
    return '' if value < 7 else '%.0f%%' % value

fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple']
explode = (0.05, 0.05, 0.1, 0)
patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_pct_format,
                                    shadow=False,  startangle=125)
for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')  # If the slice is small to fit, don't show a text label
plt.legend(labels, loc=(0.80, -0.1), shadow=True)


# COMMAND ----------

hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

hostsPick20 = (hostMoreThan10
               .map(lambda s: s[0])
               .take(20))

print ('Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20)

endpoints = (access_logs
             .map(lambda log: (log.endpoint, 1))
             .reduceByKey(lambda a, b : a + b)
             .cache())
ends = endpoints.map(lambda tpl: tpl[0]).collect()
counts = endpoints.map(lambda tpl: tpl[1]).collect()

endpointCounts = (access_logs
                  .map(lambda log: (log.endpoint, 1))
                  .reduceByKey(lambda a, b : a + b))

topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])

print ('Top Ten Endpoints: %s' % topEndpoints)
# Test.assertEquals(topEndpoints, 
#                   [(u'/images/NASA-logosmall.gif', 2752), (u'/images/KSC-logosmall.gif', 2392), 
#                    (u'/shuttle/countdown/count.gif', 1809), (u'/shuttle/countdown/', 1798), 
#                    (u'/shuttle/missions/sts-71/sts-71-patch-small.gif', 1092), (u'/images/ksclogo-medium.gif', 1049), 
#                    (u'/images/MOSAIC-logosmall.gif', 1049), (u'/images/USA-logosmall.gif', 1048), 
#                    (u'/images/WORLD-logosmall.gif', 1024), (u'/images/launch-logo.gif', 973)], 
#                   'incorrect Top Ten Endpoints')

not200 = access_logs.filter(lambda log: log.response_code != 200)

endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1))

endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b : a + b)

topFiveErrURLs = endpointSum.takeOrdered(5, lambda s: -1 * s[1])
print ('Top Five failed URLs: %s' % topFiveErrURLs)

# COMMAND ----------

# TEST Top five error endpoints (3a)
# Test.assertEquals(endpointSum.count(), 1369, 'incorrect count for endpointSum')
# Test.assertEquals(topFiveErrURLs, 
#                   [(u'/images/NASA-logosmall.gif', 468), (u'/images/KSC-logosmall.gif', 294), 
#                    (u'/shuttle/countdown/liftoff.html', 199), (u'/shuttle/countdown/', 141),
#                    (u'/shuttle/countdown/count.gif', 140)], 
#                   'incorrect Top Ten failed URLs (topFiveErrURLs)')


hosts = access_logs.map(lambda log: (log.host, 1))

uniqueHosts = hosts.reduceByKey(lambda a,b : a+b)

uniqueHostCount = uniqueHosts.count()
print ('Unique hosts: %d' % uniqueHostCount)

# COMMAND ----------

# TEST Number of unique hosts (3b)
# Test.assertEquals(uniqueHostCount, 3597, 'incorrect uniqueHostCount')


hourToHostPairTuple = access_logs.map(lambda log: (log.date_time.hour, log.host))

hourGroupedHosts = (hourToHostPairTuple.distinct()
                    .groupByKey()
                    .sortByKey())

hourHostCount = hourGroupedHosts.count()

hourlyHosts = hourGroupedHosts.map(lambda tpl: (tpl[0] ,len(tpl[1]))).cache()
              
hourlyHostsList = hourlyHosts.collect()
print ('Unique hosts per hour: %s' % hourlyHostsList)

# COMMAND ----------

# TEST Number of unique hourly hosts (3c)
# Test.assertEquals(hourlyHosts.count(), 18, 'incorrect hourlyHosts.count()')
# Test.assertEquals(hourlyHostsList, [(0, 378), (1, 329), (2, 263), (3, 194), (4, 179), (5, 156), (6, 165), (7, 170), (8, 211), (9, 245), (10, 328), (11, 323), (12, 280), (13, 306), (14, 317), (15, 351), (16, 362), (17, 112)], 'incorrect hourlyHostsList')
# Test.assertTrue(hourlyHosts.is_cached, 'incorrect hourlyHosts.is_cached')


# TODO: Replace <FILL IN> with appropriate code

hoursWithHosts = hourlyHosts.map(lambda s: s[0]).collect()
hosts = hourlyHosts.map(lambda s: s[1]).collect()

# COMMAND ----------

# TEST Visualizing unique hourly hosts (3d)
test_hours = range(0, 18)
# Test.assertEquals(hoursWithHosts, test_hours, 'incorrect hours')
# Test.assertEquals(hosts, [378, 329, 263, 194, 179, 156, 165, 170, 211, 245, 328, 323, 280, 306, 317, 351, 362, 112], 'incorrect hosts')

hourAndHostTuple = access_logs.map(lambda log: (log.date_time.hour, 1))

groupedByHour = hourAndHostTuple.reduceByKey(lambda a,b : a+b)

sortedByHour = groupedByHour.sortByKey()

avgHourlyReqPerHost = (sortedByHour.join(hourlyHosts)).mapValues(lambda tup: tup[0] / tup[1]).sortByKey().cache()
avgHourlyReqPerHostList = avgHourlyReqPerHost.take(18)
print ('Average number of hourly requests per Hosts is %s' % avgHourlyReqPerHostList)

# COMMAND ----------

# TEST Average number of hourly requests per hosts (3e)
# Test.assertEquals(avgHourlyReqPerHostList, [(0, 9), (1, 9), (2, 8), (3, 8), (4, 8), (5, 8), (6, 9), (7, 9), (8, 9), (9, 8), (10, 9), (11, 10), (12, 9), (13, 10), (14, 9), (15, 9), (16, 8), (17, 6)], 'incorrect avgHourlyReqPerHostList')
# Test.assertTrue(avgHourlyReqPerHost.is_cached, 'incorrect avgHourlyReqPerHost.is_cache')
# TODO: Replace <FILL IN> with appropriate code

hoursWithAvg = avgHourlyReqPerHost.map(lambda s: s[0]).collect()
avgs = avgHourlyReqPerHost.map(lambda s: s[1]).collect()

# COMMAND ----------

# TEST Average Hourly Requests per Unique Host (3f)
# Test.assertEquals(hoursWithAvg, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], 'incorrect hours')
# Test.assertEquals(avgs, [9, 9, 8, 8, 8, 8, 9, 9, 9, 8, 9, 10, 9, 10, 9, 9, 8, 6], 'incorrect avgs')

badRecords = access_logs.filter(lambda log: log.response_code == 404).cache()
print ('Found %d 404 URLs' % badRecords.count())

# COMMAND ----------

# TEST Counting 404 (4a)
# Test.assertEquals(badRecords.count(), 6185, 'incorrect badRecords.count()')
# Test.assertTrue(badRecords.is_cached, 'incorrect badRecords.is_cached')

# TODO: Replace <FILL IN> with appropriate code

badEndpoints = badRecords.map(lambda log: log.endpoint)

badUniqueEndpoints = badEndpoints.distinct()

badUniqueEndpointsPick30 = badUniqueEndpoints.take(30)
print ('404 URLS: %s' % badUniqueEndpointsPick30)

# COMMAND ----------



# COMMAND ----------

# TEST Listing 404 records (4b)

badUniqueEndpointsSet30 = set(badUniqueEndpointsPick30)
# Test.assertEquals(len(badUniqueEndpointsSet30), 30, 'badUniqueEndpointsPick30 not distinct')

# COMMAND ----------

# MAGIC %md
# MAGIC **(4c) Exercise: Listing the Top Ten 404 Response Code Endpoints**
# MAGIC 
# MAGIC Using the RDD containing only log records with a 404 response code that you cached in part (4a), print out a list of the top ten endpoints that generate the most 404 errors.
# MAGIC 
# MAGIC *Remember, top endpoints should be in sorted order*

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code

badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1))

badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(lambda a, b: a + b)

badEndpointsTop10 = badEndpointsSum.top(10, lambda tup: tup[1])
print ('Top Twenty 404 URLs: %s' % badEndpointsTop10)

# COMMAND ----------

# TEST Top twenty 404 URLs (4c)
# Test.assertEquals(badEndpointsTop10, 
#                   [(u'/pub/winvn/readme.txt', 20), (u'/pub/winvn/release.txt', 19), 
#                    (u'/shuttle/missions/sts-71/images/KSC-95EC-0916.txt', 14), (u'/shuttle/resources/orbiters/atlantis.gif', 13), 
#                    (u'/history/apollo/publications/sp-350/sp-350.txt~', 12), (u'/://spacelink.msfc.nasa.gov', 5), 
#                    (u'/misc/showcase/personal_edition/images/milan_banner.gif', 5), (u'/people/nasa-cm/jmd.html', 4), 
#                    (u'/shuttle/missions/sts-XX/mission-sts-XX.html', 4), (u'/shuttle/missions/sts-68/ksc-upclose.gif', 4)], 
#                   'incorrect badEndpointsTop20')

# COMMAND ----------

# MAGIC %md
# MAGIC **(4d) Exercise: Hourly 404 Response Codes**
# MAGIC 
# MAGIC Using the RDD `badRecords` you cached in the part (4a) and by hour of the day and in increasing order, create an RDD containing how many requests had a 404 return code for each hour of the day (midnight starts at 0). Cache the resulting RDD hourRecordsSorted and print that as a list.

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code

hourCountPairTuple = badRecords.map(lambda log: (log.date_time.hour, 1))

hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a,b:a+b)

hourRecordsSorted = hourRecordsSum.sortByKey().cache()

errHourList = hourRecordsSorted.collect()
print ('Top hours for 404 requests: %s' % errHourList)

# COMMAND ----------

# TEST Hourly 404 response codes (4h)
# Test.assertEquals(errHourList, [(0, 24), (1, 10), (2, 12), (3, 16), (4, 10), (5, 9), (6, 4), (7, 2), (8, 6), (9, 3), (10, 13), (11, 23), (12, 10), (13, 13), (14, 19), (15, 17), (16, 14), (17, 1)], 'incorrect errHourList')
# Test.assertTrue(hourRecordsSorted.is_cached, 'incorrect hourRecordsSorted.is_cached')

# COMMAND ----------

# MAGIC %md
# MAGIC **(4e) Exercise: Visualizing the 404 Response Codes by Hour**
# MAGIC 
# MAGIC Using the results from the previous exercise, use `matplotlib` to plot a "Line" or "Bar" graph of the 404 response codes by hour.

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code

hoursWithErrors404 = hourRecordsSorted.map(lambda s: s[0]).collect()
errors404ByHours = hourRecordsSorted.map(lambda s: s[1]).collect()
print (hoursWithErrors404)
print (errors404ByHours)

# COMMAND ----------



# COMMAND ----------

# TEST Visualizing the 404 Response Codes by Hour (4i)
# Test.assertEquals(hoursWithErrors404, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], 'incorrect hoursWithErrors404')
# Test.assertEquals(errors404ByHours, [24, 10, 12, 16, 10, 9, 4, 2, 6, 3, 13, 23, 10, 13, 19, 17, 14, 1], 'incorrect errors404ByHours')

# COMMAND ----------

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(hoursWithErrors404), 0, max(errors404ByHours)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Hour')
plt.ylabel('404 Errors')
plt.plot(hoursWithErrors404, errors404ByHours)
# display(fig)

# COMMAND ----------


