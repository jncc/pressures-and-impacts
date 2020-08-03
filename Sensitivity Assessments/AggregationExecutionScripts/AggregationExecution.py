########################################################################################################################

# Title: Aggregation Execution

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                              A. Aggregation Preparation:                                             #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required or data manipulation
import os
import time


# Test the run time of the function
start = time.process_time()

# Import all scripts as libraries which are required for the aggregation process
# Once imported, the library will automatically run as a separate script. This can only be executed via 'Run' of the
# main AggregationExecution script

# Set a working directory for executing all Sensitivity scripts which build individual aggregation outputs
os.chdir(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_Aggregation_PythonScripts\Python_Scripts\OffshoreSensitivityAggregationScripts")

# Execute Sensitivity Aggregations (offshore only)
import SensitivityAggregationOffshore
# Execute Resistance Aggregations (offshore only)
import ResistanceAggregationOffshore
# Execute Resilience Aggregations (offshore only)
import ResilienceAggregationOffshore

# Execute the Aggregation Audit Log
import AggregationAuditLog

# Stop the timer post computation and print the elapsed time
elapsed = (time.process_time() - start)

# Create print statement to indicate how long the process took and round value to 1 decimal place.
print("The 'AggregationExecution' script took " + str(
    round(elapsed / 60, 1)) + ' minutes to run and complete.')

