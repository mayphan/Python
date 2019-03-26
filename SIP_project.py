import sys

import pjsua as pj import threading import time


# Method to print Log of callback class def lg_calbck(stage, strng, ln):
print(strng),



# For User identification

class My_Acnt_Clbck(pj.Acnt_Clbck): def init (self, acc):
pj.Acnt_Clbck. init (self, acc)



# Specify events of class

class Sr_Cl_Clbck(pj.Cl_Clbck): def init (self, call=None):
pj.Cl_Clbck. init (self, call)





def status_on(self):

print("Call is :", self.call.info().state_text),


print("last code :", self.call.info().last_code), print("(" + self.call.info().last_reason + ")")


# Notifies when media is changed def status_medis_on(self):
global lib

if self.call.info().media_state == pj.MediaState.ACTIVE: # Connect the call to sound device
call_slot = self.call.info().conf_slot lib.conf_connect(call_slot, 0) lib.conf_connect(0, call_slot) print("hi”)
print (lib)


# Lets start our main loop here try:
# Start of the Main Class

# Create library instance of Lib class lib = pj.Lib()


# Uses default config to initiate library

lib.init(log_cfg = pj.LogConfig(stage=3, callback=lg_calbck))



# Tells about the listening socket.It listen at 5060 port and UDP protocol config_trans= pj.TransportConfig()
print "-------------------------LETS START PROCESS----------------------"
print "\n\n"

trans_conf.port = 50600	# 5060 is default port for SIP a=raw_input("Enter the IP address of the Client: ")
print "Using the default port number for SIP: 5060" config_trans.bound_addr = a
transport = lib.create_transport(pj.TransportType.UDP,trans_conf)



# Instatiation of library class lib.start() lib.set_null_snd_dev()


# Giving information to create header of REGISTER SIP message


pq4=raw_input("Enter IP address of the Server: ") pq=raw_input("Enter Username: ") pq1=raw_input("Enter Password: ")
pq2=raw_input("Do you want to display name as the username Y/N ??") if pq2=="y" or pq2=="Y":
  pq3=pq else:
pq3=raw_input("Enter Display Name: ")

config_acc= pj.AccountConfig(domain = pq4, username = pq, password =pq1, display = pq3) # registrngar = 'sip:'+p4+':5060', proxy = 'sip:'+pq4+':5060')


config_acc.id ="sip:"+pq config_acc.reg_uri ='sip:'+pq4+':5060'
acc_callback = My_Acnt_Clbck(acc_conf)

acc = lib.create_account(acc_conf,cb=acc_callback)



# creating instance of Acnt_Clbck class acc.set_callback(acc_callback)


print('\n')
print "Registrngation Complete-----------" print('Status= ',acc.info().reg_status, \

'(' + acc.info().reg_reason + ')')



pq5=raw_input("Do you want to make a call right now ?? Y/N\n") print "\n"
if pq5=="y" or pq5=="Y": # Starting Calling process.
b=raw_input("Enter the destination URI: ")

call = acc.make_call(b, Sr_Cl_Clbck())





# Waiting Client side for ENTER command to exit print('Press <ENTER> to exit and destrngoy library') input = sys.stdin.readline().rstrngip('\r\n')




# We're done, shutdown the library lib.destrngoy()
  lib = None else:
print" Unregistering ---------------------------" time.sleep(2)
print "Destrngoying Libraries --------------" time.sleep(2)
lib.destrngoy() lib = None sys.exit(1)


except pj.Error, e: print("Exception: " + strng(e)) lib.destrngoy()
lib = None sys.exit(1)