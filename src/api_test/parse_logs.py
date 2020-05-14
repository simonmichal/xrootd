#!/bin/python3

import math  
import numpy as np
from matplotlib import pyplot as plt

#------------------------------------------------------------------------------
# Calculate avarage
#------------------------------------------------------------------------------
dtr_sum = 0.0
duration_sum = 0.0
dtr_cnt = 0
duration_cnt = 0
exceeded_cnt = 0
dtrs = []
durations = []

for i in range( 0, 200 ):
  fn = "data/stream." + str( i * 200 ) + ".txt"
  with open( fn ) as stream_log:
    for cnt, line in enumerate( stream_log ):
      if line.startswith( 'Data transfer rate' ):
        dtr = float( line.split()[4] )
        dtr_sum += dtr
        dtr_cnt += 1
        dtrs.append( dtr )
      if line.startswith( 'Duration' ):
        duration = float( line.split()[2] )
        duration_sum += duration
        duration_cnt += 1
        if duration > 40000:
          exceeded_cnt += 1
        durations.append( duration )

avg_dtr = dtr_sum / dtr_cnt
print( "Average data transfer rate:", avg_dtr )
avg_duration = duration_sum / duration_cnt
print( "Average transfer duration:", avg_duration )

print( "Transfer duration exceeded 40s", exceeded_cnt, "times." )
fraction_exceeded = float( exceeded_cnt ) / dtr_cnt * 100
print( fraction_exceeded, "% of all transfers exceeded 40s." )

dtrs.sort()
print( "Median data transfer rate:", dtrs[int( len( dtrs ) / 2 )] )

durations.sort()
print( "Median transfer duration:", durations[int( len( durations ) /2)] )


#------------------------------------------------------------------------------
# Plot histogram of duration
#------------------------------------------------------------------------------
bins = np.arange(0, 100000, 10000) # fixed bin size
plt.xlim( [min( durations ) - 5, max( durations ) + 5 ] )
plt.hist( durations, bins=bins, alpha=0.5 )
plt.title( 'Transfer duration hist.' )
plt.xlabel('duration in ms (bin size = 10s)')
plt.ylabel('count')
plt.gcf().savefig( 'hist.png' )
plt.clf()

#------------------------------------------------------------------------------
# Calculate stddev
#------------------------------------------------------------------------------
dtr_stddev_sum = 0.0
dtr_stddev_cnt = 0
duration_stddev_sum = 0.0
duration_stddev_cnt = 0
for i in range( 0, 200 ):
  fn = "data/stream." + str( i * 200 ) + ".txt"
  with open( fn ) as stream_log:
    for cnt, line in enumerate( stream_log ):
      if line.startswith( 'Data transfer rate' ) :
        dtr = float( line.split( ' ' )[4] )
        dtr_stddev_sum += ( dtr - avg_dtr ) ** 2
        dtr_stddev_cnt += 1
      if line.startswith( 'Duration' ):
        duration = float( line.split()[2] )
        duration_stddev_sum += ( duration - avg_duration ) ** 2
        duration_stddev_cnt += 1

dtr_stddev = math.sqrt( dtr_stddev_sum / dtr_stddev_cnt )
print( "Data transfer rate standard deviation:", dtr_stddev )
duration_stddev = math.sqrt( duration_stddev_sum / duration_stddev_cnt )
print( "Transfer duration standard deviation:", duration_stddev )