
# run on anaconda python 3.6 in Windows using the Spyder ide
# format range points so that matlab can load them
with open ('rpUpdated','w') as rpu:
    with open ('rangedPoints','r') as rp:
        for line in rp:
            L = line.split('|')
            rpu.write(L[3].strip()+' '+L[4].strip() + '\n')

# format mbrs for matlab
with open('mbrsupd','w') as upd:
    with open('mbrs','r') as mbrs:
        for line in mbrs:
            line = line.replace('mbr(','')
            line = line.replace(')','')
            k = line.split(',')
            upd.write(k[0] + ' ' + k[1] + ' ' + k[2] + ' ' + k[3])
            