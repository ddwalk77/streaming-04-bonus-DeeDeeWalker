"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Name: DeeDee Walker
    Date: 2/3/23

"""

import pika
import sys
import time
import csv

# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    #define the queue so we know how to proceed
    queue = qn
    #declare variable to place the body data
    bodydata = body.decode()
    #declare list variable for data to be transformed from bodydata
    transformeddata = ['TBD']
    """ Define behavior on getting a message."""
    if queue == 'flightdelay':
        # read delay status and change to a string for clarity
        if bodydata == '0':
            transformeddata[0] = 'On Time'
        else:
            transformeddata[0] = 'Delayed'
    elif queue == 'dayofwk':
        # read Day of Week and change to string for clarity
        if bodydata == '1':
            transformeddata[0] = 'Monday'
        elif bodydata == '2':
            transformeddata[0] = 'Tuesday'
        elif bodydata == '3':
            transformeddata[0] = 'Wednesday'
        elif bodydata == '4':
            transformeddata[0] = 'Thursday'
        elif bodydata == '5':
            transformeddata[0] = 'Friday'
        elif bodydata == '6':
            transformeddata[0] = 'Saturday'
        else:
            transformeddata[0] = 'Sunday'
    else:
        # queue is not the two above then print message
        print(f' This is an unexpected queue')
        # decode the binary message body to a string
        print(f' [x] Received {body.decode()}')
        # when done with task, tell the user
        print(' [x] Done.')
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # put the values in a list (see the square brackets)
    # and write the list of values to the output file
    row = [{body.decode()}],[transformeddata[0]]
    writer.writerow(row)
    # decode the binary message body to a string
    print(f' [x] Received {body.decode()}')
    # print transformed value
    print(f' [x] Indicates: {transformeddata[0]}')
    # when done with task, tell the user
    print(' [x] Done.')
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str, qn: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print('ERROR: connection to RabbitMQ server failed.')
        print(f'Verify the server is running on host={hn}.')
        print(f'The error says: {e}')
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        print(' [*] Ready for work. To exit press CTRL+C')

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print('ERROR: something went wrong.')
        print(f'The error says: {e}')
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(' User interrupted continuous listening process.')
        sys.exit(0)
    finally:
        print('\nClosing connection. Goodbye.\n')
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Declare variables to hold the output file names
    #It would be better to go back and place error hanlding to force
    #the file to be csv.
    output_file_name = input('Enter file name ending in .csv: ')
    # Create a file object for output (w = write access)
    # On Windows, without newline='', 
    # we'll get an extra line after each record
    #add with opn to close this in the function above
    output_file = open(output_file_name, "w", newline='')
    # Create a csv writer for a comma delimited file
    writer = csv.writer(output_file, delimiter=",")
    # Enter queue name
    #Need coding for entering an unexpected queue
    #right now it adds the queue and is ready to work then doesn't proceed
    qn = input('Enter queue name as either flightdelay or dayofwk: ')
    # call the main function with the information needed
    main('localhost',qn)