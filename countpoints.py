#run on python 2.7.5
import sys
ptlist = []
mbrlist = []

#create a list of mbrs 
with open('mbrs','r') as mbrs:
   for line in mbrs:
      p = line.split(',')
      xl = float(p[0])
      yl = float(p[1])
      xh = float(p[2])
      yh = float(p[3])
      a = [xl, yl, xh, yh]
      mbrlist.append(a)
print('mbrs finished \n\n')
#create a list of points. For each point, check whether it falls within any mbr
with open('pts','r') as f:
   for line in f:
      pt = False
      p = line.split('|')
      x = float(p[3])
      y = float(p[4])
      print([x,y])
      for m in mbrlist:
        if((m[0] <= x) and (m[2] >= x) and (m[1] <= y) and (m[3] >= y)):
           pt = True
      if(pt is False):
        ptlist.append([x,y])     

#print all points that fall within 0 mbrs
print('and now the list')
for p in ptlist:
   print(p)


