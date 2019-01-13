# Project           : Enron Analysis
# Script name       : summarize-enron.py
# Author            : conradtan
# Date created      : 20190107
# Purpose           : To generate the following within 1 python code file:
#                     - (1) A .csv file containing the number of emails that person sent or received in the data set, sorted by the number of emails sent.
#                     - (2) A PNG image chart visualizing the number of emails sent over time by some of the most prolific senders in (1)
#                     - (3) A PNG image chart visualizing for the same people, the number of unique people/email addresses who contacted them over the same time period.
#
# Revision History  :
# Date        Author      Ref    Revision (Date in YYYYMMDD format) 
# 20190107    conradtan     1    Initial creation
# 20190113    conradtan     2    Final version and commit to repo


import sys
import pandas as pd
import matplotlib.pyplot as plt

if len(sys.argv) <= 1:
    print('\nsummarize-enron.py - please provide valid argument')
    print('\nCommand: python summarize-enron.py [name of csv data file]')
    print('Example: python summarize-enron.py enron-event-history-all.csv')
    exit(1)

# Get input from param
input_filename = sys.argv[1]
# input_filename = "enron-event-history-all.csv"  # Used for Jupyter Notebook

# Set number of prolific senders to assess in 2nd and 3rd outputs
num_prolific_senders = 10



# Function to plot chart by time(by month)
# Input parameters
# 1. target_df: dataframe to plot chart on
# 2. legend_lst: list to display on chart legend
# 3. y_label: y axis chart label
# 4. title: Title of chart
# 5. output_fname: Output filename of generated chart
# Output parameters
# None
def plot_output_by_time(target_df, legend_lst, y_label, title, output_fname):
    plt.figure(figsize=(20, 10), dpi=100)
    plt.xlabel('Time by Month')
    plt.ylabel(y_label)
    plt.xticks(rotation=45)
    plt.title(title)
    plt.plot(target_df)
    plt.legend(legend_lst)
    plt.savefig(output_fname, format='png')


# Data loading - START #

try:  # Read the csv data file on the required columns, create the columns headers and handle any exceptions
    event_hist_df = pd.read_csv(input_filename, header=None, usecols=[0, 1, 2, 3], names=['DateTime', 'Msg_ID', 'Sent', 'Received'])
except NameError:
    print("Filename Not Found.")
except Exception as err:
    print("Error encountered when reading file.")
    print("Error class is:  ", type(err))
    print("Error message is:", err)

# Data loading - END #

# Data Preparation/Cleaning - START #

working_df = event_hist_df.copy()  # Get a working copy
working_df['Sent'] = working_df['Sent'].str.lower()  # Convert Sent column to lowercase
working_df['Received'] = working_df['Received'].str.lower()  # Convert Received columns to lowercase
working_df = working_df.drop_duplicates(subset=['Msg_ID'], keep='first')  # 2086 records with duplicate Msg_IDs to remove (assumption to be duplicate messages)
working_df.drop(['Msg_ID'], axis=1, inplace=True) 
na_columns = working_df.columns[working_df.isna().any()]  # Get columns with na
working_df = working_df.dropna(subset=na_columns)  # Remove rows with missing Sent or Received columns values (assumption to be invalid messages)
working_df['DateTime'] = pd.to_datetime(pd.Series(working_df['DateTime']), unit='ms')  # Convert unix time to datetime
cleaned_df = working_df[~working_df.Sent.isin(['notes', 'announcements'])]  # Remove "notes" and "announcements" from sender as assumption they are not person name

# Data Preparation/Cleaning - END #


# Create split_received_df once for use in Outputs 1, 2 and 3 
split_received_df = cleaned_df['Received'].str.split("|", expand=True)  # Split recipients by '|' and expand to additional columns
split_received_df = pd.concat([cleaned_df[['Sent']], split_received_df], axis=1)


# Output 1 - START #

# Create sent_df for use in Output 1
sent_df = pd.DataFrame()
sent_df['sent'] = cleaned_df['Sent'].groupby(cleaned_df['Sent']).count()
sent_df.reset_index(inplace=True)
sent_df.columns = ['person', 'sent']
sent_df.sort_values(by='sent', ascending=False, inplace=True)  # Sort by sent count descending

# Create received_df for use in Output 1
received_df = pd.melt(split_received_df, value_name='Received')  # Unpivot for counting in received column
received_df = received_df['Received'].value_counts()
received_df = pd.DataFrame(received_df)
received_df.reset_index(level=0, inplace=True)
received_df.columns = ['person', 'received']

output_1_df = pd.merge(sent_df, received_df, on='person', how='outer').fillna(0)  # Merge received df to sent df (so its still sorted desc by sent) and fill missing(na) with 0
output_1_df.to_csv('Output_1.csv', index=False)  # Generate Output 1 to csv file

# Output 1 - END #


# Create prolific_senders_df once based on Output 1 for use in Outputs 2 and 3
prolific_senders_df = pd.DataFrame()
prolific_senders_df['Sent'] = output_1_df['person'].head(num_prolific_senders)  # Get most prolific senders

# Create sender_recipient_date_df once with list of senders, recipients and the email exchange datetime for use in Outputs 2 and 3
sender_recipient_date_df = split_received_df.copy()  # Get a working copy
sender_recipient_date_df['DateTime'] = cleaned_df['DateTime']  # Add DateTime to Dataframe
sender_recipient_date_df = pd.melt(working_df, id_vars=['DateTime', 'Sent'], value_name='Received')  # Unpivot recepients into 1 Received column
sender_recipient_date_df.reset_index(inplace=True) 
sender_recipient_date_df.index = working_df.set_index(['DateTime']).index.to_period('M').to_timestamp('M')  # Convert index to month-end dates
sender_recipient_date_df.drop(['DateTime', 'variable'], axis=1, inplace=True) 



# Output 2 - START #

working_2_df = sender_recipient_date_df.copy()  # Get working copy
working_2_df = working_2_df.loc[working_2_df['Sent'].isin(prolific_senders_df['Sent'])]  # Check the Sender column and filter off non-prolific senders
working_2_df = working_2_df.groupby(working_2_df['Sent'], as_index=True).resample('M').count()  # Resample to plot by month
working_2_df.drop(['Sent', 'index'], axis=1, inplace=True) 
working_2_df.reset_index(inplace=True) 
working_2_df.columns = ['Sender', 'Date', 'Count']

output_2_df = working_2_df.pivot(index="Date", columns="Sender", values="Count").fillna(0).astype(int)  # Pivot for plotting by senders and time

plot_output_by_time(output_2_df, prolific_senders_df['Sent'], 'No. of emails sent', 'No. of emails sent by most prolific senders', 'Output_2.png')  # Generate png file

# Output 2 - END #


# Output 3 - START #

working_3_df = sender_recipient_date_df.copy()  # Get working copy
working_3_df = working_3_df.loc[working_3_df['Received'].isin(prolific_senders_df['Sent'])]  # Check the receipent column and filter off non-prolific senders 
working_3_df = working_3_df.drop_duplicates()  # Drop duplicates to get unique contact(s) per month
working_3_df = working_3_df['Sent'].groupby([working_3_df.index, working_3_df['Received']]).count()  # Group by Date and Received
working_3_df = pd.DataFrame(working_3_df)
working_3_df.reset_index(inplace=True)  # Make indices into columns
working_3_df.columns = ['Date', 'Recipient', 'Count']

output_3_df = working_3_df.pivot(index="Date", columns="Recipient", values="Count").fillna(0).astype(int)  # Pivot for plotting by senders and time

plot_output_by_time(output_3_df, prolific_senders_df['Sent'], 'No. of unique contact(s)', 'No. of unique person(s) who contacted the prolific senders', 'Output_3.png')  # Generate png file

# Output 3 - END #
