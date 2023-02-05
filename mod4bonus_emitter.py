"""
    This program reads a csv and sends a message from a specific column to a specific queue
    on the RabbitMQ server.
    

    Name: DeeDee Walker
    Date: 2/3/23
    Original data: https://www.kaggle.com/datasets/jimschacko/airlines-dataset-to-predict-a-delay

"""
import pika
import sys
import webbrowser
import csv
import time

def offer_rabbitmq_admin_site(show_offer):
    """Pass true or false to offer the RabbitMQ Admin website to open automatically or prompt"""
    if show_offer == True :
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
    else:
        webbrowser.open_new("http://localhost:15672/#/queues")
        
def send_message(host: str, queue_name: str, file_name, column):
    """
    Reads csv file as a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue

    """
    # read from a file to get the messages (aka data) to be sent - declaring variable file_name
    with open(file_name, 'r') as file:
        # Create a csv reader for a comma delimited file
        reader = csv.reader(file, delimiter= ',')

        # Our file has a header row, move to next to get to data
        header = next(reader)
    
        for row in reader:
            # use an fstring to create a message from our data after transformed 
            string = f"{row[column]}"
            # prepare a binary (1s and 0s) message to stream
            message = string.encode()

            try:
                # create a blocking connection to the RabbitMQ server
                conn = pika.BlockingConnection(pika.ConnectionParameters(host))
                # use the connection to create a communication channel
                ch = conn.channel()
                # use the channel to declare a durable queue
                # a durable queue will survive a RabbitMQ server restart
                # and help ensure messages are processed in order
                # messages will not be deleted until the consumer acknowledges
                ch.queue_declare(queue=queue_name, durable=True)
                # use the channel to publish a message to the queue
                # every message passes through an exchange
                ch.basic_publish(exchange="", routing_key=queue_name, body=message)
                # print a message to the console for the user
                print(f" [x] Sent {message}")
            except pika.exceptions.AMQPConnectionError as e:
                print(f"Error: Connection to RabbitMQ server failed: {e}")
                sys.exit(1)
            finally:
                # close the connection to the server
                conn.close()
            
            #sleep for a second
            time.sleep(1)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site or just open it 
    # by passing True or False
    offer_rabbitmq_admin_site(False)

    # send the message to the queue (day of the week for the flight)
    send_message('localhost','dayofwk','data1.csv', 5)

    # send the message to the queue (flight delayed or not)
    send_message('localhost','flightdelay','data1.csv', 8)

    #It would be better if the function send_message read each column simultaneously 
    #this way if there is lots of data it will read each row for each column at the 
    # same time, not waiting on the other function to complete. This is more complicated 
    # with two queues so for time it was not completed at this time