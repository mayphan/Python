import os, sys
import re


def file_filter(fp, fp_write):
    val = fp.read()
    mylist = val.split('\n')
    for i in mylist:
        match = re.search('Payload\s+\(size=1000\)', i)
        if match:
            fp_write.write(i)
            fp_write.write('\n')


def node_ip_map(ip):
    #nodelist+1=ip
    return ip-1

def delay(fp):
    val = fp.read()
    mylist = val.split('\n')
    print(len(mylist))
    f = 0
    sum = 0
    delay_time=[]
    for line in mylist:
        print(line)
        if not f:
            print(f)
            match_t_t = re.search(r't\s+(\d+.*)\s(/NodeList/(\d+)/)', line)
            ##match_t_Tx = re.search(r'/Tx(0)', line)
            match_t_id=re.search(r'(id\s*(\d*)\s*protocol)',line)
            match_t_ip=re.search(r'length:\s*\d+\s+(\d+)\.(\d+)\.(\d+)\.(\d+)\s+\>\s+(\d+)\.(\d+)\.(\d+)\.(\d+)',line)
            if match_t_t:
                ##if match_t_Tx:
                    if match_t_id:
                        if match_t_ip:
                            Source_ip_t = str((match_t_ip.group(1) + '.' + match_t_ip.group(2) + '.' + match_t_ip.group(3) + '.' + match_t_ip.group(4)))
                            Destination_ip_t = str((match_t_ip.group(5) + '.' + match_t_ip.group(6) + '.' + match_t_ip.group(7) + '.' + match_t_ip.group(8)))
                            transmission_time=float((match_t_t.group(1)))
                            delay_time.append(transmission_time)
                            sequence_no = match_t_id.group(2)
                            destination_node = node_ip_map(int(match_t_ip.group(8)))
                            f=1
        if f:
            print(f)
            match_new = re.search(r'r\s+(\d+.*)\s(/NodeList/' + str(destination_node) + '/)', line)
            ##match_t_Rx = re.search(r'/Rx(1)', line)
            match_new_1=re.search(r'id\s*'+sequence_no+'\s*protocol',line)
            match_new_s_ip=re.search(r'length:\s*\d+\s+'+Source_ip_t+'\s+\>\s+'+Destination_ip_t+'',line)
            if match_new:
                ##if match_t_Rx:
                    if match_new_1:
                        if match_new_s_ip:
                            receive_time=float(match_new.group(1))
                            delay_time.append(receive_time-transmission_time)
                        f=0
    delay_time.sort()
    print("last value is ",delay_time[len(delay_time)-1])
    print('Min Value : %f' % delay_time[0])
    print('Max Value : %f' % delay_time[len(delay_time) - 1])
    for i in delay_time:
        sum += i

    avg = sum / len(delay_time)
    print('Average is : %f' % avg)
    fp.close()


###############################
### MAIN ###
###############################


file1 = str(input('Enter your file name :'))
fp_orig = open(file1, 'r')
fp_new = open('new_file', 'w+')
file_t=open('file_t','w')
file_r=open('file_r','w+')

file_filter(fp_orig, fp_new)

fp_new.seek(0, 0)
#print(len(fp_new))
delay(fp_new)


fp_orig.close()
fp_new.close()
