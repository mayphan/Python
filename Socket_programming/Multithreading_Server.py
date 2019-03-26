import socket
import threading
'''******************** Library of Books *************'''
# DATASET for Library Management System
#Author Names
book1 = {'Oppenhiem':1,'Heykin':0}
book2 = {'Kurose':3,'Tanenbaum':7}
book3 = {'Richard':5,'Rhodes':12}
book4 = {'Peebles':4,'Papoulis':9}
book5 = {'Smsze':2,'Jaspritsingh':11}
#Bookname
books={'Signals and Systems':book1,'Computer Networking':book2, 'Network
Programming':book3, 'Probability and Random Variables':book4, 'Semiconductor
Devices':book5}
'''****************************************************'''
host = '127.0.0.1' # Loop back (IP)
address of host
port = 2736 # Port number of server
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
''' Creation of a TCP socket. Default parameters socket.AF_INET represents IPv4
addressing scheme
and socket.SOCK_STREAM represents TCP connection '''
s.bind((host,port)) # Bind the socket to
the host and number
s.listen(120) # Number of clients the
server can listen
def getBookName(choice): # Method to get the
list of Book name based on choice entered by user. If book is not found, return
Null
bookName = {
'1':'Signals and Systems',
14
'2':'Computer Networking',
'3':'Network Programming',
'4':'Probability and Random Variables',
'5':'Semiconductor Devices'}
return bookName.get(choice,'Null')
def threaded_client(c): # Create a method
to do server-side validation
while 1:
try:
#=== Book name verification starts here ===
data_bookname = c.recv(1024) # Get bookname to
be searched from client
bookNameChoice=data_bookname.decode();
bookname = getBookName(bookNameChoice)
bookKey = books.get(bookname) # Get the bookname
from getBookName method and store in bookKey variable
if bookname is not 'Null':
print(bookname + ' is available')
author_name = bookKey.keys()
author =''
for authorL in author_name: # Return all the
authors available for a particular book name
quantity = bookKey.get(authorL) # Get the quantity
of the authors present for a particular author
#print(authorL)
print(quantity)
if(quantity >0):
author = authorL+';'+author # If the quantity
is greater than 0,return the author name
else:
if(author is not ''):
author = 'Null' # No authors
available for selected book, return Null
else:
author = 'Null' + ';' + author
print(author)
c.send((str(author)).encode())
if(author is not 'Null'):
data_authorName = c.recv(1024)
data_authorName = data_authorName.decode()
data_authorName = str(data_authorName).capitalize()
if data_authorName in bookKey.keys(): # Validate if the
author name is valid
#print('in if of book author')
status_author = 'available'
c.send(status_author.encode())
15
data_book = c.recv(1024)
checkout=data_book.decode()
if str(checkout).capitalize()=='Y':
quantity = bookKey.get(data_authorName)
if(quantity>0):
i=bookKey.get(data_authorName) # Get the
current count of the books
i=i-1 # Decrement the
count by 1
print(str(i)+' number of books available now')
#print(data_authorName)
bookKey[data_authorName]=i # Update the new
quantity of available author after checkout
#print(bookKey.get(data_authorName))
message = 'success'
c.send(message.encode())
else:
message ='fail'
c.send(message.encode())
continue
else:
message ='fail'
c.send(message.encode())
continue
else:
#print('in else of book author')
status_author = 'unavailable' # Invalid author name
c.send(status_author.encode())
continue
else:
continue
else:
status_bookname=bookname # Invalid book name
c.send(status_bookname.encode())
#print(bookname + ' is unavailable')
continue
except:
#print('Thanks for connecting.!')
break
while 1:
c,addr = s.accept() # Accept the
connection from a new client
print ('Connected with ' + addr[0] + ':' + str(addr[1]))
try:
thread = threading.Thread(target = threaded_client,args =(c,)) #
Create a thread for each connected client
thread.daemon = True # Program exits when
16
only daemon threads are left
thread.start() # Start the newly
created thread object
except:
print ("Error: unable to start thread")