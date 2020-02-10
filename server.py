from confluent_kafka import Producer, Consumer, KafkaError
import threading
import sys 

def rpc_sub(*args):
    diff= args[0]
    
    for i in range (1, len(args)):
        diff -= args[i]
        
    return str(diff)

def rpc_multiply(*args):
    sum= args[0]
    
    for i in range (1, len(args)):
        sum *= args[i]
        
    return str(sum)

def rpc_modulo(*args):
    return str(args[0]%args[1])

#process the required function names and their types to call the function defined in server
def process_string(line):
    list= line.split(':')
#    print (list)
    func= str(list[0]) +"("
#    print (func_name)
    
    for i in range (1, len(list)):
        l= str(list[i]).split('\'')
#        print (l)
        d_type= str(l[1])
#        print (d_type)
        func += d_type +"("
        l= str(l[2]).split('-')
#        print (l)
        value= str(l[1])
        func += value + "),"
#        print (value)
#        print()
    
    func = func[:-1]
    func += ")"        
    print ("function= ", func)  
    result= eval(func)
    print (result)
    print()
    return result


#main (creating connection to clients and sending recieving required results)
if __name__ == "__main__":
    c = Consumer({'bootstrap.servers': sys.argv[1], 'group.id': '1', 'auto.offset.reset': 'earliest'})
    c.subscribe(['server'])
    p= Producer({'bootstrap.servers': sys.argv[1]})

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        # decode the message consumed
        notification= msg.value().decode('utf-8')
        params= notification.split('$')
        data= params[0]
        topic= params[1]

        result= process_string(data)
        p.produce(topic, result.encode('utf-8'))

