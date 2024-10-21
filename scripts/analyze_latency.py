# analyze_latency.py
#
# Author: Team 14 (Maddox, Emma, Abhay)
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
#
# Purpose: Utility file that analyzes the latency data from each trial run of 1, 2, 3, and 4 producers. 
# The CDF for each producer is plotted and the average CDF is also plotted. The 90th and 95th percentiles
# are also computed

import matplotlib.pyplot as plt
import numpy as np

# Define the workloads and corresponding producer file names for each run
workloads = {
    1: ['producer_latencies/producer_1_1'],
    2: ['producer_latencies/producer_1_2', 'producer_latencies/producer_2_2'],
    3: ['producer_latencies/producer_1_3', 'producer_latencies/producer_2_3', 'producer_latencies/producer_3_3'],
    4: ['producer_latencies/producer_1_4', 'producer_latencies/producer_2_4', 'producer_latencies/producer_3_4', 'producer_latencies/producer_4_4']
}

colors = ['blue', 'green', 'red', 'orange']

# Plot CDF for each workload
def plot_cdf_for_workload(num_producers, producer_ids):
    all_latency_results = []  
    plt.figure()

    for i, producer_id in enumerate(producer_ids):
        latency_results = []

        # Read latency data from file
        filename = f'{producer_id}.txt'
        try:
            with open(filename, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        _, latency_str = line.split(',')
                        latency = float(latency_str)
                        latency_results.append(latency)
                        all_latency_results.append(latency)
        except FileNotFoundError:
            print(f"File {filename} not found. Make sure the file exists.")
            continue

        if not latency_results:
            continue

        # Sort the latency values and compute cumulative probabilities for this producer
        latency_array = np.array(latency_results)
        sorted_latency = np.sort(latency_array)
        cum_probs = np.arange(1, len(sorted_latency) + 1) / len(sorted_latency)

        # Plot the CDF for the current producer
        color = colors[i]
        plt.plot(sorted_latency, cum_probs, label=f'{producer_id}', color=color)
    
    # Compute and plot average CDF
    if all_latency_results:
        all_latency_array = np.array(all_latency_results)
        sorted_all_latency = np.sort(all_latency_array)
        cum_probs_all = np.arange(1, len(sorted_all_latency) + 1) / len(sorted_all_latency)
        plt.plot(sorted_all_latency, cum_probs_all, label=f'Average ({num_producers} Producers)', color='black', linestyle='--')

        # Calculate and print 90th and 95th percentiles for the average latencies
        latency_90th = np.percentile(all_latency_array, 90)
        latency_95th = np.percentile(all_latency_array, 95)
        print(f"{num_producers} Producers - 90th percentile average latency: {latency_90th} seconds")
        print(f"{num_producers} Producers - 95th percentile average latency: {latency_95th} seconds")

    plt.xlim(0.01, 0.06)  
    plt.ylim(0, 1)  
    plt.xlabel('Latency (seconds)')
    plt.ylabel('Cumulative Probability')
    plt.title(f'CDF of Latency for {num_producers} Producer(s)')
    plt.legend()
    plt.grid(True)
    plt.show()

# Loop through each workload and plot the CDF
for num_producers, producer_ids in workloads.items():
    plot_cdf_for_workload(num_producers, producer_ids)
