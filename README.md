# streaming-04-bonus-DeeDeeWalker
Read multi-column csv with data from each column distributed to different queues after a transformation of the data is completed

## Name: DeeDee Walker
## Date: 2/5/23

Original data: https://www.kaggle.com/datasets/jimschacko/airlines-dataset-to-predict-a-delay
Data was reduced to 8 lines plus the original header for simplicity

The mod4bonus_emitter.py reads the csv file data1.csv for the specified column then reads it again for the next specified column. Each column is sent to it's own queue, not the same queue. Noting here that I would like to go back and chnage the code to read both columns in at the same time with two queues in the same function. For time, this was not done right now.

The mod4bonus_listener.py processes the messages on the user input queues and sends them to a declared csv file that is also input by the user. One column/queue has flight delay information and the other has the day of the week. Both read a number indicator then change the information to a string. The information is displayed on the screen and placed in the csv. This can be improved by error handling on the user input to ensure the file is a csv. Improvmenets also need to be made to the queue user input to ensure the correct queue information is entered. User input was used so the function was reusable for both queues, datasets, and output files. Nee to add code for with open on the csv files to close.

Run mod4bonus_emitter.py in a terminal and  mod4bonus_listener.py twice in two different terminals. Follow the prompts on the screen for the mod4bonus_listener.py.

Terminal screens running
![Terminal screens](https://github.com/ddwalk77/streaming-04-bonus-DeeDeeWalker/blob/main/bonusterminals.png "Terminal screens")