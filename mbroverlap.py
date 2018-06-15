#run on python 2.7.5, xrange isn't available in python 3, but centos doesn't seem to have the __future__ module,
#so this is not python 3.x compliant
import sys
ptlist = []
mbrlist = []

#create a list of mbrs
with open('a','r') as mbrs:
   for line in mbrs:
      p = line.split(' ')
      xl = float(p[0])
      yl = float(p[1])
      xh = float(p[2])
      yh = float(p[3])
      a = [xl, yl, xh, yh]
      mbrlist.append(a)
      
#for each pair of mbrs, check overlap by determining if each boundary edge of one mbr
#falls between the boundary edgedges of the other
for i in xrange(len(mbrlist)):
   for j in xrange(i+1,len(mbrlist)):
      if((mbrlist[i][0] >= mbrlist[j][0]) and (mbrlist[i][0] <= mbrlist[j][2])):
         if((mbrlist[i][1] >= mbrlist[j][1]) and (mbrlist[i][1] <= mbrlist[j][3])):
            print([mbrlist[i],mbrlist[j]])
         elif((mbrlist[i][3] >= mbrlist[j][1]) and (mbrlist[i][3] <= mbrlist[j][3])):
            print([mbrlist[i],mbrlist[j]])
      elif((mbrlist[i][2] >= mbrlist[j][0]) and (mbrlist[i][2] <= mbrlist[j][2])):
         if((mbrlist[i][1] >= mbrlist[j][1]) and (mbrlist[i][1] <= mbrlist[j][3])):
            print([mbrlist[i],mbrlist[j]])
         elif((mbrlist[i][3] >= mbrlist[j][1]) and (mbrlist[i][3] <= mbrlist[j][3])):
            print([mbrlist[i],mbrlist[j]])

