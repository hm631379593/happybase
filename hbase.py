import happybase
connection = happybase.Connection('192.168.1.66')
print(connection.tables())