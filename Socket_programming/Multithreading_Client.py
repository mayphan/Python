import socket
host = '127.0.0.1' # Loop back (IP)
address of server the client needs connect to
port = 2736 # Port number of server
client needs to connect to
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
''' Creation of a TCP socket. Default parameters socket.AF_INET represents IPv4
addressing scheme
and socket.SOCK_STREAM represents TCP connection '''
s.connect((host,port)) # Connect to server
using the socket defined.
print('\n------------------------------------------------------------------------
-------\n')
print('\t\t\tWelcome to Library Management system.')
print('\n------------------------------------------------------------------------
--------')
message =''
while message.lower() != 'q':
print('Please select from the list of books available to checkout\n')
print('1.Signals and Systems \n2.Computer Networking \n3.Network Programming
\n4.Probability and Random Variables \n5.Semiconductor Devices \nQ.Quit')
message = input('\nEnter the choice for book between 1-5 or q to Quit: ')
message = str(message)
if message.lower() != 'q':
#print('Message entered from client>> :' +message)
s.send(message.encode()) # Send book name entered
by user
authorName = s.recv(1024)
author_name = authorName.decode()
#print(author_name)
#author_name = author_name.strip('([])')
#print ('Received data from server : '+author_name)
if(author_name == 'Null'):
17
print('\nBook Unavailable!! Please try again\n')
continue
else:
#print(author_name)
author_name = author_name.split(sep=';') # Receive all the authors
available for a book
print(author_name)
author_name.pop()
if('Null' in author_name ):
author_name.pop(1)
print('List of available authors are: '+ str(author_name))
message = input('\nPlease enter the author name to checkout:')
#print('author Name: '+message)
s.send(message.encode()) # Send the author name
entered to server for validation
status_authorName = str(s.recv(1024).decode())
if status_authorName != 'unavailable':
message = input('Do you want to checkout the book ? Please type
Y/N: ')
s.send(message.encode()) # Send the checkout
confirmation to server for validation
status = str(s.recv(1024).decode())
if(status =='success'):
print('The book is checked out to you successfully.\n')
message = input('Press Y to continue any other key to exit :
' )
if(message.capitalize()=='Y'):
continue
else:
print ('Thank you for connecting to Library management
system. Have a great day!\n\n\n')
break
else:
print('Sorry,No authors of the requested book are available.
Please try some other book.')
continue
else:
print('Sorry, No authors of the requested book are available.
Please try some other book.')
continue
else:
print ('Thank you for connecting to Library management system. Have a
great day!\n\n\n')
s.close()