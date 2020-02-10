import re

#definition of the function to establish connection to server and retrieve its result from it for client
code_gen = "\n\ndef {}:\n\tcon = Consumer({}'{}': sys.argv[1], 'group.id': '1', 'auto.offset.reset': 'earliest'{})\
\n\tcon.subscribe([sys.argv[2]])\
\n\tprod = Producer({}'{}': sys.argv[1]{})\
\n\tstring= \"{}\"{}\
\n\tstring += '$'+ topic\
\n\tprod.produce('server', string.encode('utf-8'))\
\n\twhile True:\
\n\t\tmsg = con.poll(1.0)\
\n\t\tif msg is None:\
\n\t\t\tcontinue\
\n\t\tif msg.error():\
\n\t\t\tprint('ERROR')\
\n\t\t\tcontinue\
\n\t\tprint(msg.value().decode('utf-8'))\
\n\t\treturn\n\n"
    
def process_string (line):
    list = line.split('\n')
#    print (list)
    list1 = str(list[0]).split('(')
#    print (list1)
    list2= str(list1[1]).split(')')
#    print (list2)
    list3 =str(list2[0]).split(',')
#    print (list3)
#    print (len(list3))
    s= '('
    for i in range (0, len(list3)):
        s += str(chr(i+97))+','
            
    s = s[:-1]
    s += ')'   
#    print (s)
    s= str(list1[0]) +s
#    print (s)
    return s, str(list1[0]), list3



#main (read the incomplete client and creating extentible executable file)       
f = open('/home/smriti/Desktop/CourseWork/SEMESTER-2/IAS/A Simple Middleware System using Instant Messaging/Client1/extendible_file.py', "w")
lib ="import sys\nfrom confluent_kafka import Producer, Consumer, KafkaError"
f.write(lib)
f.write('\ntopic= sys.argv[2]')
pattern= re.compile('^rpc_')
f_input= open("/home/smriti/Desktop/CourseWork/SEMESTER-2/IAS/A Simple Middleware System using Instant Messaging/Client1/client.py", "r")

contents= f_input.readlines()
#check for each line in client file if it has made a RPC call
for line in contents:
    if pattern.match(line):
        s, fun_name, list = process_string(line)
#        print (s)
#        print (fun_name)
#        print (list)
        
        args = ""
        
        index= 0
        for i in list:
            g= chr(index+97)
            args += "+\":\"+" + "str(type("+g+"))" + "+\"-\"+" +  "\"" + str(i)+ "\"" 
            index +=1
        print (args)
        
        f.write(code_gen.format(s,'{', 'bootstrap.servers','}', '{','bootstrap.servers','}', fun_name,args))
       
f_input.close() 

f_input= open("/home/smriti/Desktop/CourseWork/SEMESTER-2/IAS/A Simple Middleware System using Instant Messaging/Client1/client.py", "r")
data= f_input.read()
f.write(data)  
f_input.close()       
f.close()
    



